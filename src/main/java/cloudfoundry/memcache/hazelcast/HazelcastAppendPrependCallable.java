package cloudfoundry.memcache.hazelcast;

import io.netty.handler.codec.memcache.binary.BinaryMemcacheResponseStatus;

import java.io.EOFException;
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

public class HazelcastAppendPrependCallable
		implements HazelcastInstanceAware, Callable<HazelcastMemcacheMessage>, IdentifiedDataSerializable {
	private static final Logger LOGGER = LoggerFactory.getLogger(HazelcastAppendPrependCallable.class);
	private HazelcastInstance instance;
	private String cacheName;
	private byte[] key;
	HazelcastMemcacheCacheValue cacheValue;
	boolean append;
	private long maxValueSize;

	public HazelcastAppendPrependCallable() {
	}

	public HazelcastAppendPrependCallable(String cacheName, byte[] key, HazelcastMemcacheCacheValue cacheValue,
			boolean append, long maxValueSize) {
		this.maxValueSize = maxValueSize;
		this.cacheName = cacheName;
		this.key = key;
		this.cacheValue = cacheValue;
		this.append = append;
	}

	@Override
	public HazelcastMemcacheMessage call() {
		try {
			IMap<byte[], HazelcastMemcacheCacheValue> cache = getCache();
			if (cache.tryLock(key, 1, TimeUnit.SECONDS, 1, TimeUnit.SECONDS)) {
				try {
					HazelcastMemcacheCacheValue value = cache.get(key);
					if (value == null) {
						return new HazelcastMemcacheMessage(false, BinaryMemcacheResponseStatus.KEY_ENOENT,
								"No value exists to append/prepend to the specified key.");
					}
					if (value.getFlagLength() == 0) {
						return new HazelcastMemcacheMessage(false, BinaryMemcacheResponseStatus.DELTA_BADVAL,
								"Value appears to be an inc/dec number.  Cannot append/prepend to this.");
					}
					int totalValueLength = value.getValueLength() + cacheValue.getValueLength();
					if (totalValueLength > maxValueSize) {
						return new HazelcastMemcacheMessage(false, BinaryMemcacheResponseStatus.E2BIG,
								"New value too big.  Max Value is " + maxValueSize);
					}
					HazelcastMemcacheCacheValue newValue = new HazelcastMemcacheCacheValue(totalValueLength,
							value.getFlags(), value.getExpiration());
					if (append) {
						newValue.writeValue(value.getValue());
						newValue.writeValue(cacheValue.getValue());
					} else {
						newValue.writeValue(cacheValue.getValue());
						newValue.writeValue(value.getValue());
					}

					cache.set(key, newValue, newValue.getExpiration(), TimeUnit.SECONDS);
					return new HazelcastMemcacheMessage(true, newValue.getCAS());
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
		return 6;
	}

	@Override
	public int getId() {
		return 6;
	}

	@Override
	public void readData(ObjectDataInput in) throws IOException {
		cacheName = in.readUTF();
		key = in.readByteArray();
		cacheValue = in.readObject();
		append = in.readBoolean();
		try {
			maxValueSize = in.readLong();
		} catch (EOFException e) {
			maxValueSize = 1048576;
		}
	}

	@Override
	public void writeData(ObjectDataOutput out) throws IOException {
		out.writeUTF(cacheName);
		out.writeByteArray(key);
		out.writeObject(cacheValue);
		out.writeBoolean(append);
		out.writeLong(maxValueSize);
	}
}
