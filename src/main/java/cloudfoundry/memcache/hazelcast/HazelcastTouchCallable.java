package cloudfoundry.memcache.hazelcast;

import io.netty.handler.codec.memcache.binary.BinaryMemcacheResponseStatus;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class HazelcastTouchCallable implements HazelcastInstanceAware, Callable<HazelcastMemcacheMessage>, IdentifiedDataSerializable {
	private transient HazelcastInstance instance;
	private String cacheName;
	private byte[] key;
	private long expiration;

	public HazelcastTouchCallable() {	}

	public HazelcastTouchCallable(String cacheName, byte[] key, long expiration) {
		this.cacheName = cacheName;
		this.key = key;
		this.expiration = expiration;
	}


	@Override
	public HazelcastMemcacheMessage call() {
		IMap<byte[], HazelcastMemcacheCacheValue> cache = getCache();

		long currentTimeInSeconds = System.currentTimeMillis() / 1000;
		long expirationInSeconds;
		if (expiration <= HazelcastMemcacheMsgHandler.MAX_EXPIRATION_SEC) {
			expirationInSeconds = expiration;
		} else if (expiration > currentTimeInSeconds) {
			expirationInSeconds = expiration - currentTimeInSeconds;
		} else {
			cache.remove(key);
			return new HazelcastMemcacheMessage(true);
		}

		try {
			cache.lock(key);
			HazelcastMemcacheCacheValue value = cache.get(key);

			if (value == null) {
				return new HazelcastMemcacheMessage(false, BinaryMemcacheResponseStatus.KEY_ENOENT, "Unable to find Key: " + key);
			}
			if (value.getExpiration() == expiration) {
				cache.set(key, value, expirationInSeconds, TimeUnit.SECONDS);
			} else {
				HazelcastMemcacheCacheValue newValue = new HazelcastMemcacheCacheValue(value.getValueLength(), value.getFlags(), expiration);
				newValue.writeValue(value.getValue());
				cache.set(key, newValue, expirationInSeconds, TimeUnit.SECONDS);
			}
		} finally {
			cache.unlock(key);
		}
		return new HazelcastMemcacheMessage(true);
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
		return 7;
	}

	@Override
	public int getId() {
		return 7;
	}

	@Override
	public void readData(ObjectDataInput in) throws IOException {
		cacheName = in.readUTF();
		key = in.readByteArray();
		expiration = in.readLong();
	}

	@Override
	public void writeData(ObjectDataOutput out) throws IOException {
		out.writeUTF(cacheName);
		out.writeByteArray(key);
		out.writeLong(expiration);
	}
}
