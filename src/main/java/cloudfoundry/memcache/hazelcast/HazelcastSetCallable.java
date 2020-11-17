package cloudfoundry.memcache.hazelcast;

import cloudfoundry.memcache.MemcacheUtils;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheOpcodes;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheResponseStatus;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HazelcastSetCallable
		implements HazelcastInstanceAware, Callable<HazelcastMemcacheMessage>, IdentifiedDataSerializable {
	private static final Logger LOGGER = LoggerFactory.getLogger(HazelcastSetCallable.class);
	private HazelcastInstance instance;
	private String cacheName;
	private byte nonQuietOpcode;
	private byte[] key;
	private long cas;
	HazelcastMemcacheCacheValue cacheValue;
	long expirationInSeconds;

	public HazelcastSetCallable() {
	}

	public HazelcastSetCallable(String cacheName, byte nonQuietOpcode, byte[] key, long cas,
			HazelcastMemcacheCacheValue cacheValue, long expirationInSeconds) {
		this.cacheName = cacheName;
		this.nonQuietOpcode = nonQuietOpcode;
		this.key = key;
		this.cas = cas;
		this.cacheValue = cacheValue;
		this.expirationInSeconds = expirationInSeconds;
	}

	@Override
	public HazelcastMemcacheMessage call() {
		try {
			IMap<byte[], HazelcastMemcacheCacheValue> cache = getCache();
			if (cas == 0) {
				return setValue(cache);
			} else {
				if (cache.tryLock(key, 1, TimeUnit.SECONDS, 1, TimeUnit.SECONDS)) {
					try {
						HazelcastMemcacheCacheValue value = cache.get(key);
						if (value == null) {
							return new HazelcastMemcacheMessage(false, BinaryMemcacheResponseStatus.KEY_ENOENT,
									"No entry exists to check CAS against.");
						}
						long valueCAS = value.getCAS();
						if (cas != valueCAS) {
							return new HazelcastMemcacheMessage(false, BinaryMemcacheResponseStatus.KEY_EEXISTS,
									"CAS values don't match.");
						}
						return setValue(cache);
					} finally {
						cache.unlock(key);
					}
				} else {
					return new HazelcastMemcacheMessage(false, MemcacheUtils.INTERNAL_ERROR, "Failed to aquire lock on key.");
				}
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return new HazelcastMemcacheMessage(false, MemcacheUtils.INTERNAL_ERROR, "Thread interrupted.");
		} catch (Exception e) {
			LOGGER.error("Unexpected Error:", e);
			return new HazelcastMemcacheMessage(false, MemcacheUtils.INTERNAL_ERROR, "Unexpected Error.  Message: " + e.getMessage());
		}
	}

	private HazelcastMemcacheMessage setValue(IMap<byte[], HazelcastMemcacheCacheValue> cache)
			throws InterruptedException {
		boolean success = false;
		if (nonQuietOpcode == BinaryMemcacheOpcodes.SET) {
			cache.set(key, cacheValue, expirationInSeconds, TimeUnit.SECONDS);
			success = true;
		} else if (nonQuietOpcode == BinaryMemcacheOpcodes.ADD) {
			success = cache.putIfAbsent(key, cacheValue, expirationInSeconds, TimeUnit.SECONDS) == null ? true : false;
		} else if (nonQuietOpcode == BinaryMemcacheOpcodes.REPLACE) {
			if (cache.tryLock(key, 1, TimeUnit.SECONDS, 1, TimeUnit.SECONDS)) {
				try {
					if (cache.containsKey(key)) {
						cache.set(key, cacheValue, expirationInSeconds, TimeUnit.SECONDS);
						success = true;
					} else {
						success = false;
					}
				} finally {
					cache.unlock(key);
				}
			} else {
				return new HazelcastMemcacheMessage(false, MemcacheUtils.INTERNAL_ERROR, "Failed to aquire lock on key.");
			}
		}
		if (!success) {
			if (nonQuietOpcode == BinaryMemcacheOpcodes.SET) {
				return new HazelcastMemcacheMessage(false, BinaryMemcacheResponseStatus.NOT_STORED,
						"Couldn't set the value for some reason.");
			} else if (nonQuietOpcode == BinaryMemcacheOpcodes.ADD) {

				return new HazelcastMemcacheMessage(false, BinaryMemcacheResponseStatus.KEY_EEXISTS,
						"An value already exists with for the given key.");
			} else if (nonQuietOpcode == BinaryMemcacheOpcodes.REPLACE) {
				return new HazelcastMemcacheMessage(false, BinaryMemcacheResponseStatus.KEY_ENOENT,
						"No value to replace for the given key.");
			}
		}
		return new HazelcastMemcacheMessage(true, cacheValue.getCAS());
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
		return 2;
	}

	@Override
	public int getId() {
		return 2;
	}

	@Override
	public void readData(ObjectDataInput in) throws IOException {
		cacheName = in.readUTF();
		nonQuietOpcode = in.readByte();
		key = in.readByteArray();
		cas = in.readLong();
		cacheValue = in.readObject();
		expirationInSeconds = in.readLong();
	}

	@Override
	public void writeData(ObjectDataOutput out) throws IOException {
		out.writeUTF(cacheName);
		out.writeByte(nonQuietOpcode);
		out.writeByteArray(key);
		out.writeLong(cas);
		out.writeObject(cacheValue);
		out.writeLong(expirationInSeconds);
	}
}
