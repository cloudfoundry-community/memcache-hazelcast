package cloudfoundry.memcache.hazelcast;

import java.io.IOException;
import java.util.concurrent.Callable;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class HazelcastGetCallable implements HazelcastInstanceAware, Callable<HazelcastMemcacheCacheValue>, IdentifiedDataSerializable {
	private transient HazelcastInstance instance;
	private String cacheName;
	private byte[] key;

	public HazelcastGetCallable() {
	}

	public HazelcastGetCallable(String cacheName, byte[] key) {
		super();
		this.cacheName = cacheName;
		this.key = key;
	}

	@Override
	public HazelcastMemcacheCacheValue call() {

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

	@Override
	public int getFactoryId() {
		return 1;
	}
	
	@Override
	public int getId() {
		return 1;
	}
	
	@Override
	public void readData(ObjectDataInput in) throws IOException {
		cacheName = in.readUTF();
		key = in.readByteArray();
	}
	
	@Override
	public void writeData(ObjectDataOutput out) throws IOException {
		out.writeUTF(cacheName);
		out.writeByteArray(key);
	}
}
