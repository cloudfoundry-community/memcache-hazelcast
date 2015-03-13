package cloudfoundry.memcache.hazelcast;

import java.io.IOException;

import com.hazelcast.nio.serialization.ByteArraySerializer;

public class HazelcastMemcacheCacheValueSerializer implements ByteArraySerializer<HazelcastMemcacheCacheValue> {
	@Override
	public void destroy() {
	}
	
	@Override
	public int getTypeId() {
		return 100;
	}
	
	@Override
	public HazelcastMemcacheCacheValue read(byte[] buffer) throws IOException {
		return new HazelcastMemcacheCacheValue(buffer);
	}
	
	@Override
	public byte[] write(HazelcastMemcacheCacheValue value) throws IOException {
		return value.getCacheEntry();
	}

}
