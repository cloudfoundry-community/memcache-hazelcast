package cloudfoundry.memcache.hazelcast;

import io.netty.handler.codec.memcache.binary.BinaryMemcacheResponseStatus;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cloudfoundry.memcache.MemcacheUtils;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class HazelcastGATCallable
		implements HazelcastInstanceAware, Callable<HazelcastMemcacheMessage>, IdentifiedDataSerializable {
	private static final Logger LOGGER = LoggerFactory.getLogger(HazelcastGATCallable.class);
	private HazelcastInstance instance;
	private String cacheName;
	private byte[] key;
	private long expiration;

	public HazelcastGATCallable() {
	}

	public HazelcastGATCallable(String cacheName, byte[] key, long expiration) {
		this.cacheName = cacheName;
		this.key = key;
		this.expiration = expiration;
	}

	@Override
	public HazelcastMemcacheMessage call() {
		try {
			IMap<byte[], HazelcastMemcacheCacheValue> cache = getCache();

			long expirationInSeconds;
			long currentTimeInSeconds = System.currentTimeMillis() / 1000;
			if (expiration <= HazelcastMemcacheMsgHandler.MAX_EXPIRATION_SEC) {
				expirationInSeconds = expiration;
			} else if (expiration > currentTimeInSeconds) {
				expirationInSeconds = expiration - currentTimeInSeconds;
			} else {
				HazelcastMemcacheCacheValue value = cache.remove(key);
				if (value == null) {
					return new HazelcastMemcacheMessage(false, BinaryMemcacheResponseStatus.KEY_ENOENT,
							"Unable to find Key: " + key);
				} else {
					return new HazelcastMemcacheMessage(true, value);
				}
			}

			HazelcastMemcacheCacheValue value;
			if (cache.tryLock(key, 1, TimeUnit.SECONDS, 1, TimeUnit.SECONDS)) {
				try {
					value = cache.get(key);

					if (value == null) {
						return new HazelcastMemcacheMessage(false, BinaryMemcacheResponseStatus.KEY_ENOENT,
								"Unable to find Key: " + key);
					}
					if (value.getExpiration() == expiration) {
						cache.set(key, value, expirationInSeconds, TimeUnit.SECONDS);
						return new HazelcastMemcacheMessage(true, value);
					} else {
						HazelcastMemcacheCacheValue newValue = new HazelcastMemcacheCacheValue(value.getValueLength(),
								value.getFlags(), expiration);
						newValue.writeValue(value.getValue());
						cache.set(key, newValue, expirationInSeconds, TimeUnit.SECONDS);
						return new HazelcastMemcacheMessage(true, newValue);
					}
				} finally {
					cache.unlock(key);
				}
			} else {
				return new HazelcastMemcacheMessage(false, MemcacheUtils.INTERNAL_ERROR, "Failed to aquire lock on key.");
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return new HazelcastMemcacheMessage(false, MemcacheUtils.INTERNAL_ERROR, "Thread interrupted.");
		} catch (Exception e) {
			LOGGER.error("Unexpected Error:", e);
			return new HazelcastMemcacheMessage(false, MemcacheUtils.INTERNAL_ERROR, "Unexpected Error.  Message: " + e.getMessage());
		}
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
		return 8;
	}

	@Override
	public int getId() {
		return 8;
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
