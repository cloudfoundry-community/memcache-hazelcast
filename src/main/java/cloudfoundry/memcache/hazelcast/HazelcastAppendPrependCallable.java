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

public class HazelcastAppendPrependCallable implements HazelcastInstanceAware, Callable<HazelcastMemcacheMessage>, IdentifiedDataSerializable {
	private transient HazelcastInstance instance;
	private String cacheName;
	private byte[] key;
	HazelcastMemcacheCacheValue cacheValue;
	boolean append;

	public HazelcastAppendPrependCallable() {	}

	public HazelcastAppendPrependCallable(String cacheName, byte[] key, HazelcastMemcacheCacheValue cacheValue, boolean append) {
		this.cacheName = cacheName;
		this.key = key;
		this.cacheValue = cacheValue;
		this.append = append;
	}


	@Override
	public HazelcastMemcacheMessage call() {
		IMap<byte[], HazelcastMemcacheCacheValue> cache = getCache();
		try {
			cache.lock(key);
			HazelcastMemcacheCacheValue value = cache.get(key);
			if (value == null) {
				return new HazelcastMemcacheMessage(false, BinaryMemcacheResponseStatus.KEY_ENOENT, "No value exists to append/prepend to the specified key.");
			}
			if (value.getFlagLength() == 0) {
				return new HazelcastMemcacheMessage(false, BinaryMemcacheResponseStatus.DELTA_BADVAL, "Value appears to be an inc/dec number.  Cannot append/prepend to this.");
			}
			int totalValueLength = value.getValueLength() + cacheValue.getValueLength();
			if (totalValueLength > HazelcastMemcacheMsgHandler.MAX_VALUE_SIZE) {
				return new HazelcastMemcacheMessage(false, BinaryMemcacheResponseStatus.E2BIG, "New value too big.  Max Value is " + HazelcastMemcacheMsgHandler.MAX_VALUE_SIZE);
			}
			HazelcastMemcacheCacheValue newValue = new HazelcastMemcacheCacheValue(totalValueLength, value.getFlags(), value.getExpiration());
			if(append) {
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
	}

	@Override
	public void writeData(ObjectDataOutput out) throws IOException {
		out.writeUTF(cacheName);
		out.writeByteArray(key);
		out.writeObject(cacheValue);
		out.writeBoolean(append);
	}
}
