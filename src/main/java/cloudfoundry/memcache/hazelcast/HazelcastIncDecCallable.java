package cloudfoundry.memcache.hazelcast;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheResponseStatus;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class HazelcastIncDecCallable implements HazelcastInstanceAware, Callable<HazelcastMemcacheMessage>, IdentifiedDataSerializable {
	private transient HazelcastInstance instance;
	private String cacheName;
	private byte[] key;
	long expirationInSeconds;
	boolean increment;
	long delta;
	long expiration;
	long initialValue;

	public HazelcastIncDecCallable() {	}

	public HazelcastIncDecCallable(String cacheName, byte[] key, long expirationInSeconds, boolean increment, long delta, long expiration, long initialValue) {
		this.cacheName = cacheName;
		this.key = key;
		this.expirationInSeconds = expirationInSeconds;
		this.increment = increment;
		this.delta = delta;
		this.expiration = expiration;
		this.initialValue = initialValue;
	}


	@Override
	public HazelcastMemcacheMessage call() {
		IMap<byte[], HazelcastMemcacheCacheValue> cache = getCache();
		try {
			cache.lock(key);
			HazelcastMemcacheCacheValue value = cache.get(key);
			if (value != null) {
				long currentValue = 0;
				if (value.getFlagLength() == 0) {
					currentValue = value.getValue().getLong(0);
				} else {
					byte[] valueBytes = value.getCacheEntry();
					if (valueBytes.length > 21) {
						return new HazelcastMemcacheMessage(false, BinaryMemcacheResponseStatus.DELTA_BADVAL,
								"The ASCII value currently in key has too many digits.");
					}
					try {
						currentValue = Long.parseUnsignedLong(new String(valueBytes, "ASCII"));
					} catch (NumberFormatException e) {
						if (valueBytes.length == 8) {
							currentValue = value.getValue().getLong(0);
						} else {
							return new HazelcastMemcacheMessage(false, BinaryMemcacheResponseStatus.DELTA_BADVAL, "Unable to parse existing value.");
						}
					} catch (UnsupportedEncodingException e) {
						throw new RuntimeException(e);
					}
				}
				if (increment) {
					currentValue += delta;
				} else {
					if (Long.compareUnsigned(currentValue, delta) < -0) {
						currentValue = 0;
					} else {
						currentValue -= delta;
					}
				}
				value = new HazelcastMemcacheCacheValue(8, Unpooled.EMPTY_BUFFER, value.getExpiration());
				value.writeValue(Unpooled.copyLong(currentValue));
			} else {
				value = new HazelcastMemcacheCacheValue(8, Unpooled.EMPTY_BUFFER, expiration);
				value.writeValue(Unpooled.copyLong(initialValue));
			}
			cache.set(key, value, expirationInSeconds, TimeUnit.SECONDS);
			return new HazelcastMemcacheMessage(true, value);
		} finally {
			cache.unlock(key);
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
		return 5;
	}

	@Override
	public int getId() {
		return 5;
	}

	@Override
	public void readData(ObjectDataInput in) throws IOException {
		cacheName = in.readUTF();
		key = in.readByteArray();
		expirationInSeconds = in.readLong();
		increment = in.readBoolean();
		delta = in.readLong();
		expiration = in.readLong();
		initialValue = in.readLong();
	}

	@Override
	public void writeData(ObjectDataOutput out) throws IOException {
		out.writeUTF(cacheName);
		out.writeByteArray(key);
		out.writeLong(expirationInSeconds);
		out.writeBoolean(increment);
		out.writeLong(delta);
		out.writeLong(expiration);
		out.writeLong(initialValue);
	}
}
