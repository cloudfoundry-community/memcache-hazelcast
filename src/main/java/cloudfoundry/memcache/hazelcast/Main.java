package cloudfoundry.memcache.hazelcast;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.JoinConfig;
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
import com.hazelcast.config.TcpIpConfig;

import cloudfoundry.memcache.AuthMsgHandlerFactory;
import cloudfoundry.memcache.MemcacheMsgHandlerFactory;
import cloudfoundry.memcache.MemcacheServer;
import cloudfoundry.memcache.SecretKeyAuthMsgHandlerFactory;
import cloudfoundry.memcache.StubAuthMsgHandlerFactory;
import cloudfoundry.memcache.web.HttpBasicAuthenticator;

@SpringBootApplication
@ComponentScan("cloudfoundry.memcache")
public class Main {

	private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

	@Bean
	HazelcastMemcacheMsgHandlerFactory hazelcastconfig(MemcacheHazelcastConfig springConfig) {
		Config config = new Config();
		for (Map.Entry<String, MemcacheHazelcastConfig.Plan> plan : springConfig.getPlans().entrySet()) {
			LOGGER.info("Configuring plan: " + plan.getKey());
			MapConfig mapConfig = new MapConfig(plan.getKey() + "*");
			mapConfig.setStatisticsEnabled(true);
			mapConfig.setBackupCount(plan.getValue().getBackup());
			mapConfig.setAsyncBackupCount(plan.getValue().getAsyncBackup());
			if(plan.getValue().getEvictionPolicy() == EvictionPolicy.LRU) {
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
		networkConfig.setPort(springConfig.getHazelcast().getPort());
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
		for (Map.Entry<String, List<String>> zone : springConfig.getHazelcast().getMachines().entrySet()) {
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
		config.setProperty("hazelcast.io.thread.count", Integer.toString(springConfig.getHazelcast().getIoThreadCount()));
		config.setProperty("hazelcast.operation.thread.count", Integer.toString(springConfig.getHazelcast().getOperationThreadCount()));
		config.setProperty("hazelcast.operation.generic.thread.count", Integer.toString(springConfig.getHazelcast().getOperationGenericThreadCount()));
		config.setProperty("hazelcast.event.thread.count", Integer.toString(springConfig.getHazelcast().getEventThreadCount()));
		config.setProperty("hazelcast.client.event.thread.count", Integer.toString(springConfig.getHazelcast().getClientEventThreadCount()));
		config.setProperty("hazelcast.partition.count", Integer.toString(springConfig.getHazelcast().getPartitionCount()));
		config.setProperty("hazelcast.max.no.heartbeat.seconds", Integer.toString(springConfig.getHazelcast().getMaxNoHeartbeatSeconds()));
		config.setProperty("hazelcast.operation.call.timeout.millis", Integer.toString(springConfig.getHazelcast().getOperationCallTimeout()));
		config.setProperty("hazelcast.socket.receive.buffer.size", Integer.toString(springConfig.getHazelcast().getReceiveBufferSize()));
		config.setProperty("hazelcast.socket.send.buffer.size", Integer.toString(springConfig.getHazelcast().getSendBufferSize()));
		return new HazelcastMemcacheMsgHandlerFactory(config,
				springConfig.getHazelcast().getLocalMemberSafeTimeout(),
				springConfig.getHazelcast().getMinimumClusterMembers(),
				springConfig.getHazelcast().getExecutorPoolSize(),
				springConfig.getHazelcast().getMaxCacheSize(),
				springConfig.getHazelcast().getPercentToTrim(),
				springConfig.getHazelcast().getTrimDelay());
	}

	@Bean
	AuthMsgHandlerFactory authHandlerFactory(MemcacheHazelcastConfig config) {
		if (config.getMemcache().getSecretKey() == null || config.getMemcache().getSecretKey().isEmpty()) {
			return new StubAuthMsgHandlerFactory();
		}
		return new SecretKeyAuthMsgHandlerFactory(config.getMemcache().getSecretKey());
	}

	@Bean
	HttpBasicAuthenticator basicAuthenticator(MemcacheHazelcastConfig config) {
		return new HttpBasicAuthenticator("", config.getHost().getUsername(), config.getHost().getPassword());
	}

	@Bean
	MemcacheServer memcacheServer(MemcacheMsgHandlerFactory handlerFactory, AuthMsgHandlerFactory authFactory,
			MemcacheHazelcastConfig config) {
		MemcacheServer server = new MemcacheServer(handlerFactory, config.getMemcache().getPort(), authFactory,
				config.getMemcache().getMaxQueueSize());
		return server;
	}

	public static void main(String[] args) throws Exception {
		SpringApplication.run(Main.class, args);
		LOGGER.info("Memcache server initialized.");
	}
}
