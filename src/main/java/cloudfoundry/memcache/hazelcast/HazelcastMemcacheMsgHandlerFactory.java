package cloudfoundry.memcache.hazelcast;

import javax.annotation.PreDestroy;

import cloudfoundry.memcache.AuthMsgHandler;
import cloudfoundry.memcache.MemcacheMsgHandler;
import cloudfoundry.memcache.MemcacheMsgHandlerFactory;

import com.hazelcast.config.Config;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class HazelcastMemcacheMsgHandlerFactory implements MemcacheMsgHandlerFactory {
	private final HazelcastInstance instance;
	
	public HazelcastMemcacheMsgHandlerFactory(Config config) {
		SerializerConfig serializerConfig = new SerializerConfig()
			.setImplementation(new HazelcastMemcacheCacheValueSerializer())
			.setTypeClass(HazelcastMemcacheCacheValue.class);
		config.getSerializationConfig().addSerializerConfig(serializerConfig);
		config.addReplicatedMapConfig(new ReplicatedMapConfig()
				.setName(Stat.STAT_MAP));
		instance = Hazelcast.newHazelcastInstance(config);
		instance.getReplicatedMap(Stat.STAT_MAP).put(Stat.UPTIME_KEY, System.currentTimeMillis());
	}
	
	public MemcacheMsgHandler createMsgHandler(AuthMsgHandler authMsgHandler) {
		return new HazelcastMemcacheMsgHandler(authMsgHandler, instance);
	}
	
	@PreDestroy
	public void shutdown() {
		instance.shutdown();
	}
}
