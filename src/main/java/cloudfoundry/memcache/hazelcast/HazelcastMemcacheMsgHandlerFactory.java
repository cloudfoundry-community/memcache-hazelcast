package cloudfoundry.memcache.hazelcast;

import io.netty.handler.codec.memcache.binary.BinaryMemcacheRequest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cloudfoundry.memcache.AuthMsgHandler;
import cloudfoundry.memcache.MemcacheMsgHandler;
import cloudfoundry.memcache.MemcacheMsgHandlerFactory;

import com.hazelcast.config.Config;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.PartitionService;

public class HazelcastMemcacheMsgHandlerFactory implements MemcacheMsgHandlerFactory {
	private static final Logger LOGGER = LoggerFactory.getLogger(HazelcastMemcacheMsgHandlerFactory.class);

	private final HazelcastInstance instance;

	public HazelcastMemcacheMsgHandlerFactory(Config config) {
		SerializerConfig serializerConfig = new SerializerConfig().setImplementation(new HazelcastMemcacheCacheValueSerializer()).setTypeClass(
				HazelcastMemcacheCacheValue.class);
		config.getSerializationConfig().addSerializerConfig(serializerConfig);
		setupSerializables(config);
		config.addReplicatedMapConfig(new ReplicatedMapConfig().setName(Stat.STAT_MAP));
		config.setProperty("hazelcast.memcache.enabled", "false");
		config.setProperty("hazelcast.rest.enabled", "false");
		config.setProperty("hazelcast.shutdownhook.enabled", "false");
		config.setProperty("hazelcast.logging.type", "slf4j");
		config.setProperty("hazelcast.version.check.enabled", "false");

		ExecutorConfig executorConfig = new ExecutorConfig();
		executorConfig.setPoolSize(16).setStatisticsEnabled( false );
		
		config.setExecutorConfigs(Collections.singletonMap("exec", executorConfig));

		instance = Hazelcast.newHazelcastInstance(config);
		instance.getReplicatedMap(Stat.STAT_MAP).putIfAbsent(Stat.UPTIME_KEY, System.currentTimeMillis());
	}

	private void setupSerializables(Config config) {
		config.getSerializationConfig().addDataSerializableFactory(1, (int id) -> (id == 1) ? new HazelcastGetCallable() : null);
		config.getSerializationConfig().addDataSerializableFactory(2, (int id) -> (id == 2) ? new HazelcastSetCallable() : null);
		config.getSerializationConfig().addDataSerializableFactory(3, (int id) -> (id == 3) ? new HazelcastMemcacheMessage() : null);
		config.getSerializationConfig().addDataSerializableFactory(4, (int id) -> (id == 4) ? new HazelcastDeleteCallable() : null);
		config.getSerializationConfig().addDataSerializableFactory(5, (int id) -> (id == 5) ? new HazelcastIncDecCallable() : null);
		config.getSerializationConfig().addDataSerializableFactory(6, (int id) -> (id == 6) ? new HazelcastAppendPrependCallable() : null);
		config.getSerializationConfig().addDataSerializableFactory(7, (int id) -> (id == 7) ? new HazelcastTouchCallable() : null);
		config.getSerializationConfig().addDataSerializableFactory(8, (int id) -> (id == 8) ? new HazelcastGATCallable() : null);
	}

	public MemcacheMsgHandler createMsgHandler(BinaryMemcacheRequest request, AuthMsgHandler authMsgHandler) {
		return new HazelcastMemcacheMsgHandler(request, authMsgHandler, instance);
	}

	@Override
	public List<String> getCaches() {
		List<String> caches = new ArrayList<>();
		for(DistributedObject object : instance.getDistributedObjects()) {
			if(object instanceof IMap) {
				caches.add(object.getName());
			}
		}
		return caches;
	}
	
	@Override
	public void createCache(String name) {
		instance.getMap(name);
	}
	
	public void deleteCache(String name) {
		instance.getMap(name).destroy();
	};
	
	@PreDestroy
	public void shutdown() {
		LOGGER.info("Shutting down Hazelcast.");
		try {
			PartitionService partitionService = instance.getPartitionService();
			if (!partitionService.isLocalMemberSafe()) {
				partitionService.forceLocalMemberToBeSafe(30, TimeUnit.SECONDS);
			}
		} finally {
			instance.shutdown();
		}
	}
}
