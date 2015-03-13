package cloudfoundry.memcache.hazelcast;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import cloudfoundry.memcache.MemcacheUtils;


public class HazelcastMemcacheCacheValue {
	private static final int FLAG_LENGTH_OFFSET = 0;
	private static final int FLAG_LENGTH_LENGTH = 1;
	private static final int FLAG_OFFSET = FLAG_LENGTH_OFFSET+1;
	private final ByteBuf cacheEntry;

	public HazelcastMemcacheCacheValue(int valueLength, int flagLength) {
		cacheEntry = Unpooled.buffer(valueLength+flagLength+FLAG_LENGTH_LENGTH);
	}

	public HazelcastMemcacheCacheValue(byte[] cacheEntry) {
		this.cacheEntry = Unpooled.wrappedBuffer(cacheEntry);
	}
	
	public void setFlags(ByteBuf flags) {
		cacheEntry.writerIndex(FLAG_LENGTH_OFFSET);
		cacheEntry.writeByte(flags.capacity());
		cacheEntry.writeBytes(flags);
	}
	
	public void setValue(ByteBuf value) {
		cacheEntry.writerIndex(getValueOffset());
		cacheEntry.writeBytes(value);
	}
	
	public void setMoreValue(ByteBuf value) {
		cacheEntry.writeBytes(value);
	}

	public ByteBuf getFlags() {
		return cacheEntry.slice(FLAG_LENGTH_LENGTH, getFlagLength());
	}
	
	public ByteBuf getValue() {
		return cacheEntry.slice(getValueOffset(), getValueLength());
	}
	
	public int getValueLength() {
		return cacheEntry.capacity()-(getFlagLength()+FLAG_LENGTH_LENGTH);
	}
	
	public int getValueOffset() {
		return getFlagLength()+FLAG_LENGTH_LENGTH;
	}

	public byte getFlagLength() {
		return cacheEntry.getByte(FLAG_LENGTH_OFFSET);
	}
	
	public int getTotalFlagsAndValueLength() {
		return cacheEntry.capacity()-FLAG_LENGTH_LENGTH;
	}
	
	public byte[] getCacheEntry() {
		return cacheEntry.array();
	}
	
	public long getCAS(String key) {
		byte[] bytes = key.getBytes();
		long seed = MemcacheUtils.hash64(bytes, 0, bytes.length, 0);
		return MemcacheUtils.hash64(getCacheEntry(), FLAG_OFFSET, getCacheEntry().length, seed);
	}
}
