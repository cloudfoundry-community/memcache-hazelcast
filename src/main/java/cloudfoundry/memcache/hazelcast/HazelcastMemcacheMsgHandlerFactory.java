package cloudfoundry.memcache.hazelcast;

import cloudfoundry.memcache.AuthMsgHandler;
import cloudfoundry.memcache.MemcacheMsgHandler;
import cloudfoundry.memcache.MemcacheMsgHandlerFactory;
import cloudfoundry.memcache.ShutdownUtils;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.MaxSizeConfig.MaxSizePolicy;
import com.hazelcast.config.MemberGroupConfig;
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.PartitionGroupConfig;
import com.hazelcast.config.PartitionGroupConfig.MemberGroupType;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleEvent.LifecycleState;
import com.hazelcast.core.LifecycleListener;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheRequest;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HazelcastMemcacheMsgHandlerFactory implements MemcacheMsgHandlerFactory, AutoCloseable {
	private static final Logger LOGGER = LoggerFactory.getLogger(HazelcastMemcacheMsgHandlerFactory.class);
	public static final String EXECUTOR_INSTANCE_NAME = "memcache";
	public static final String DELETED_CACHES_KEY = "deletedCachesKey";

	private final HazelcastInstance instance;
	private final ScheduledExecutorService memoryTrimmerExecutor;
	private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(4);
	private volatile boolean shuttingDown = false;
	private final MemcacheHazelcastConfig appConfig;

	public HazelcastMemcacheMsgHandlerFactory(MemcacheHazelcastConfig appConfig) {
		this.appConfig = appConfig;
		Config config = new Config();
		for (Map.Entry<String, MemcacheHazelcastConfig.Plan> plan : appConfig.getPlans().entrySet()) {
			LOGGER.info("Configuring plan: {}", plan.getKey());
			MapConfig mapConfig = new MapConfig(plan.getKey() + "*");
			mapConfig.setStatisticsEnabled(true);
			mapConfig.setBackupCount(plan.getValue().getBackup());
			mapConfig.setAsyncBackupCount(plan.getValue().getAsyncBackup());
			if (plan.getValue().getEvictionPolicy() == EvictionPolicy.LRU) {
				mapConfig.setMapEvictionPolicy(LRUCreatedOrUpdateEvictionPolicy.INSTANCE);
			} else {
				mapConfig.setEvictionPolicy(plan.getValue().getEvictionPolicy());
			}
			mapConfig.setMaxIdleSeconds(plan.getValue().getMaxIdleSeconds());
			mapConfig.setMaxSizeConfig(
					new MaxSizeConfig(plan.getValue().getMaxSizeUsedHeap(), MaxSizePolicy.USED_HEAP_SIZE));
			if (plan.getValue().getNearCache() != null) {
				NearCacheConfig nearCacheConfig = new NearCacheConfig();
				nearCacheConfig.setInvalidateOnChange(true);
				nearCacheConfig.setCacheLocalEntries(false);
				nearCacheConfig.setLocalUpdatePolicy(LocalUpdatePolicy.INVALIDATE);
				nearCacheConfig.setMaxSize(plan.getValue().getNearCache().getMaxSize());
				nearCacheConfig.setTimeToLiveSeconds(plan.getValue().getNearCache().getTtlSeconds());
				nearCacheConfig.setMaxIdleSeconds(plan.getValue().getNearCache().getMaxIdleSeconds());
				nearCacheConfig.setEvictionPolicy(plan.getValue().getNearCache().getEvictionPolicy());
				mapConfig.setNearCacheConfig(nearCacheConfig);
			}
			config.addMapConfig(mapConfig);
		}
		NetworkConfig networkConfig = new NetworkConfig().setReuseAddress(true);
		config.setNetworkConfig(networkConfig);
		if (appConfig.getHazelcast().getPort() != null) {
			networkConfig.setPort(appConfig.getHazelcast().getPort());
		}
		networkConfig.setPortAutoIncrement(false);
		JoinConfig joinConfig = new JoinConfig();
		networkConfig.setJoin(joinConfig);
		joinConfig.setMulticastConfig(new MulticastConfig().setEnabled(false));
		TcpIpConfig tcpIpConfig = new TcpIpConfig().setEnabled(true);
		joinConfig.setTcpIpConfig(tcpIpConfig);
		PartitionGroupConfig partitionGroupConfig = new PartitionGroupConfig();
		config.setPartitionGroupConfig(partitionGroupConfig);
		partitionGroupConfig.setEnabled(true);
		partitionGroupConfig.setGroupType(MemberGroupType.CUSTOM);
		for (Map.Entry<String, List<String>> zone : appConfig.getHazelcast().getMachines().entrySet()) {
			MemberGroupConfig memberGroupConfig = new MemberGroupConfig();
			for (String machine : zone.getValue()) {
				tcpIpConfig.addMember(machine);
				memberGroupConfig.addInterface(machine);
			}
			partitionGroupConfig.addMemberGroupConfig(memberGroupConfig);
		}
		config.setProperty("hazelcast.memcache.enabled", "false");
		config.setProperty("hazelcast.rest.enabled", "false");
		config.setProperty("hazelcast.shutdownhook.enabled", "false");
		config.setProperty("hazelcast.logging.type", "slf4j");
		config.setProperty("hazelcast.phone.home.enabled", "false");
		config.setProperty("hazelcast.backpressure.enabled", "true");
		config.setProperty("hazelcast.jmx", "true");
		setPropertyIfNotNull(config, "hazelcast.io.thread.count", appConfig.getHazelcast().getIoThreadCount());
		setPropertyIfNotNull(config, "hazelcast.operation.thread.count",
				appConfig.getHazelcast().getOperationThreadCount());
		setPropertyIfNotNull(config, "hazelcast.operation.generic.thread.count",
				appConfig.getHazelcast().getOperationGenericThreadCount());
		setPropertyIfNotNull(config, "hazelcast.event.thread.count", appConfig.getHazelcast().getEventThreadCount());
		setPropertyIfNotNull(config, "hazelcast.client.event.thread.count",
				appConfig.getHazelcast().getClientEventThreadCount());
		setPropertyIfNotNull(config, "hazelcast.partition.count", appConfig.getHazelcast().getPartitionCount());
		setPropertyIfNotNull(config, "hazelcast.max.no.heartbeat.seconds",
				appConfig.getHazelcast().getMaxNoHeartbeatSeconds());
		setPropertyIfNotNull(config, "hazelcast.operation.call.timeout.millis",
				appConfig.getHazelcast().getOperationCallTimeout());
		setPropertyIfNotNull(config, "hazelcast.socket.receive.buffer.size",
				appConfig.getHazelcast().getReceiveBufferSize());
		setPropertyIfNotNull(config, "hazelcast.socket.send.buffer.size", appConfig.getHazelcast().getSendBufferSize());
		config.setProperty("hazelcast.socket.connect.timeout.seconds", "30");
		config.setProperty("hazelcast.slow.operation.detector.enabled", "false");
		config.setProperty("hazelcast.diagnostics.enabled", "false");
		setPropertyIfNotNull(config, "hazelcast.partition.migration.timeout",
				appConfig.getHazelcast().getMaxNoHeartbeatSeconds());
		setPropertyIfNotNull(config, "hazelcast.graceful.shutdown.max.wait",
				appConfig.getHazelcast().getLocalMemberSafeTimeout());
		config.setProperty("hazelcast.max.join.seconds", "30");
		config.setProperty("hazelcast.max.no.master.confirmation.seconds", "60");
		config.setProperty("hazelcast.member.list.publish.interval.seconds", "90");
		config.setProperty("hazelcast.map.invalidation.batch.enabled", "false");
		config.addListenerConfig(new ListenerConfig(new ShutdownListener()));

		SerializerConfig serializerConfig = new SerializerConfig()
				.setImplementation(new HazelcastMemcacheCacheValueSerializer())
				.setTypeClass(HazelcastMemcacheCacheValue.class);
		config.getSerializationConfig().addSerializerConfig(serializerConfig);
		setupSerializables(config);
		config.addReplicatedMapConfig(new ReplicatedMapConfig().setName(Stat.STAT_MAP));

		ExecutorConfig executorConfig = new ExecutorConfig().setStatisticsEnabled(false);
		if (appConfig.getHazelcast().getExecutorPoolSize() == null
				|| appConfig.getHazelcast().getExecutorPoolSize() == 0) {
			executorConfig.setPoolSize(Runtime.getRuntime().availableProcessors() * 2);
		} else {
			executorConfig.setPoolSize(appConfig.getHazelcast().getExecutorPoolSize());
		}

		config.setExecutorConfigs(Collections.singletonMap(EXECUTOR_INSTANCE_NAME, executorConfig));

		instance = Hazelcast.newHazelcastInstance(config);
		LOGGER.info("Hazelcast Started.");
		instance.getReplicatedMap(Stat.STAT_MAP).putIfAbsent(Stat.UPTIME_KEY, System.currentTimeMillis());
		if (appConfig.getHazelcast().getEnableMemoryTrimmer().booleanValue()) {
			try {
				memoryTrimmerExecutor = Executors.newScheduledThreadPool(1);
				memoryTrimmerExecutor.scheduleWithFixedDelay(new MaxHeapTrimmer(HazelcastMemcacheMsgHandlerFactory.this,
						appConfig.getHazelcast().getMaxCacheSize(), appConfig.getHazelcast().getPercentToTrim()),
						appConfig.getHazelcast().getTrimDelay(), appConfig.getHazelcast().getTrimDelay(),
						TimeUnit.SECONDS);
			} catch (Exception e) {
				instance.shutdown();
				throw e;
			}
		} else {
			memoryTrimmerExecutor = null;
		}
	}

	private class ShutdownListener implements LifecycleListener {
		@Override
		public void stateChanged(LifecycleEvent event) {
			if (LifecycleState.SHUTDOWN.equals(event.getState())) {
				if (!shuttingDown) {
					ShutdownUtils.gracefullyExit("Irregular shutdown detected.  Exiting the process.", (byte) 101,
							null);
				} else {
					LOGGER.info("Hazelcast Server is shutdown.");
				}
			}
		}
	}

	private void setPropertyIfNotNull(Config config, String property, Object value) {
		if (value != null) {
			config.setProperty(property, value.toString());
		}
	}

	public HazelcastInstance getInstance() {
		return instance;
	}

	private void setupSerializables(Config config) {
		config.getSerializationConfig().addDataSerializableFactory(2,
				(int id) -> (id == 2) ? new HazelcastSetCallable() : null);
		config.getSerializationConfig().addDataSerializableFactory(3,
				(int id) -> (id == 3) ? new HazelcastMemcacheMessage() : null);
		config.getSerializationConfig().addDataSerializableFactory(4,
				(int id) -> (id == 4) ? new HazelcastDeleteCallable() : null);
		config.getSerializationConfig().addDataSerializableFactory(5,
				(int id) -> (id == 5) ? new HazelcastIncDecCallable() : null);
		config.getSerializationConfig().addDataSerializableFactory(6,
				(int id) -> (id == 6) ? new HazelcastAppendPrependCallable() : null);
		config.getSerializationConfig().addDataSerializableFactory(7,
				(int id) -> (id == 7) ? new HazelcastTouchCallable() : null);
		config.getSerializationConfig().addDataSerializableFactory(8,
				(int id) -> (id == 8) ? new HazelcastGATCallable() : null);
	}

	public MemcacheMsgHandler createMsgHandler(BinaryMemcacheRequest request, AuthMsgHandler authMsgHandler) {
		return new HazelcastMemcacheMsgHandler(request, authMsgHandler, instance,
				appConfig.getMemcache().getMaxValueSize());
	}

	public boolean isCacheValid(String cacheName) {
		if (instance == null || instance.getReplicatedMap(DELETED_CACHES_KEY) == null) {
			return false;
		}
		return !instance.getReplicatedMap(DELETED_CACHES_KEY).containsKey(cacheName);
	}

	public void deleteCache(String name) {
		// I know this isn't a great solution but is better than it was. :)
		instance.getReplicatedMap(DELETED_CACHES_KEY).put(name, "");
		IMap<Object, Object> map = instance.getMap(name);
		if (map != null) {
			LOGGER.info("Destroying cache: " + name);
			map.destroy();
		}
	}

	@Override
	public ScheduledExecutorService getScheduledExecutorService() {
		return scheduledExecutorService;
	}

	@Override
	public String status() {
		if (shuttingDown) {
			return "Shuttingdown";
		}
		if (instance == null) {
			return "InstanceNull";
		}
		if (!instance.getLifecycleService().isRunning()) {
			return "NotRunning";
		}
		if (instance.getCluster().getClusterState() != ClusterState.ACTIVE) {
			return "ClusterNotActive: " + instance.getCluster().getClusterState();
		}
		if (!instance.getPartitionService().isClusterSafe()) {
			return "ClusterNotSafe";
		}
		if (!instance.getPartitionService().isLocalMemberSafe()) {
			return "LocalMemberNotSafe";
		}
		return OK_STATUS;
	}

	@Override
	public void close() throws Exception {
		if (memoryTrimmerExecutor != null) {
			memoryTrimmerExecutor.shutdownNow();
		}
		if (!shuttingDown) {
			shuttingDown = true;
			LOGGER.info("Shutting down Hazelcast.");
			ShutdownUtils.gracefullyCloseExecutorService(scheduledExecutorService, Duration.ofSeconds(5),
					Duration.ofSeconds(5));
			instance.shutdown();
		}
	}
}
