package cloudfoundry.memcache.hazelcast;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cloudfoundry.memcache.AuthMsgHandler;
import cloudfoundry.memcache.MemcacheMsgHandler;
import cloudfoundry.memcache.MemcacheMsgHandlerFactory;

import com.hazelcast.config.Config;
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
		// Hazelcast.setOutOfMemoryHandler(outOfMemoryHandler);
		SerializerConfig serializerConfig = new SerializerConfig().setImplementation(new HazelcastMemcacheCacheValueSerializer()).setTypeClass(
				HazelcastMemcacheCacheValue.class);
		config.getSerializationConfig().addSerializerConfig(serializerConfig);
		config.addReplicatedMapConfig(new ReplicatedMapConfig().setName(Stat.STAT_MAP));
		config.setProperty("hazelcast.memcache.enabled", "false");
		config.setProperty("hazelcast.rest.enabled", "false");
		config.setProperty("hazelcast.shutdownhook.enabled", "false");
		config.setProperty("hazelcast.logging.type", "slf4j");
		config.setProperty("hazelcast.version.check.enabled", "false");

		instance = Hazelcast.newHazelcastInstance(config);
		instance.getReplicatedMap(Stat.STAT_MAP).putIfAbsent(Stat.UPTIME_KEY, System.currentTimeMillis());
	}

	public MemcacheMsgHandler createMsgHandler(AuthMsgHandler authMsgHandler) {
		return new HazelcastMemcacheMsgHandler(authMsgHandler, instance);
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
