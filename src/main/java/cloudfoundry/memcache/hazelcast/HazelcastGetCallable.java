package cloudfoundry.memcache.hazelcast;

import java.io.Serializable;
import java.util.concurrent.Callable;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;

public class HazelcastGetCallable implements HazelcastInstanceAware, Callable<HazelcastMemcacheCacheValue>, Serializable {
	private transient HazelcastInstance instance;
	private String cacheName;
	private String key;

	public HazelcastGetCallable() {
	}

	public HazelcastGetCallable(String cacheName, String key) {
		super();
		this.cacheName = cacheName;
		this.key = key;
	}

	@Override
	public HazelcastMemcacheCacheValue call() throws Exception {

		IMap<String, HazelcastMemcacheCacheValue> cache = getCache();

		return cache.get(key);
	}
	
	public IMap<String, HazelcastMemcacheCacheValue> getCache() {
		return instance.getMap(cacheName);
	}

	@Override
	public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
		this.instance = hazelcastInstance;
	}
}
