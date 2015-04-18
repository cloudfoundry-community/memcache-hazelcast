package cloudfoundry.memcache.hazelcast;

import java.io.IOException;
import java.util.concurrent.Callable;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class HazelcastDeleteCallable implements HazelcastInstanceAware, Callable<Boolean>, IdentifiedDataSerializable {
	private transient HazelcastInstance instance;
	private String cacheName;
	private byte[] key;

	public HazelcastDeleteCallable() {
	}

	public HazelcastDeleteCallable(String cacheName, byte[] key) {
		super();
		this.cacheName = cacheName;
		this.key = key;
	}

	@Override
	public Boolean call() {

		IMap<byte[], HazelcastMemcacheCacheValue> cache = getCache();

		return cache.remove(key) == null ? false : true;
	}
	
	public IMap<byte[], HazelcastMemcacheCacheValue> getCache() {
		return instance.getMap(cacheName);
	}

	@Override
	public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
		this.instance = hazelcastInstance;
	}

	@Override
	public int getFactoryId() {
		return 4;
	}
	
	@Override
	public int getId() {
		return 4;
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
