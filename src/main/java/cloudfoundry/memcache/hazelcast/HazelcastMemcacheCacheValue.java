package cloudfoundry.memcache.hazelcast;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import cloudfoundry.memcache.MemcacheUtils;


public class HazelcastMemcacheCacheValue {
	private static final int FLAG_LENGTH_OFFSET = 1;
	private static final int EXPIRATION_LENGTH_OFFSET = 0;
	private static final int FLAG_LENGTH_LENGTH = 1;
	private static final int EXPIRATION_LENGTH_LENGTH = 1;
	private static final int EXPIRATION_LENGTH = 4;
	private final ByteBuf cacheEntry;

	public HazelcastMemcacheCacheValue(int valueLength, ByteBuf flags, long expiration) {
		int expirationLength = deriveExpirationLength(expiration);
		int flagLength = deriveFlagLength(flags);
		cacheEntry = Unpooled.buffer(valueLength+flagLength+FLAG_LENGTH_LENGTH+EXPIRATION_LENGTH_LENGTH+expirationLength);
		setExpiration(expiration);
		setFlags(flags);
		cacheEntry.writerIndex(getValueOffset());
	}

	private int deriveFlagLength(ByteBuf flags) {
		return flags.capacity();
	}

	private int deriveExpirationLength(long expiration) {
		return expiration == 0 ? 0 : EXPIRATION_LENGTH;
	}

	public HazelcastMemcacheCacheValue(byte[] cacheEntry) {
		this.cacheEntry = Unpooled.wrappedBuffer(cacheEntry);
	}

	private void setFlags(ByteBuf flags) {
		cacheEntry.writerIndex(FLAG_LENGTH_OFFSET);
		cacheEntry.writeByte(deriveFlagLength(flags));
		cacheEntry.writerIndex(getFlagOffset());
		cacheEntry.writeBytes(flags);
	}
	
	public int getExpirationLength() {
		return cacheEntry.getByte(EXPIRATION_LENGTH_OFFSET);
	}

	private void setExpiration(long expiration) {
		cacheEntry.writerIndex(EXPIRATION_LENGTH_OFFSET);
		if(expiration != 0) {
    		cacheEntry.writeByte(EXPIRATION_LENGTH);
    		cacheEntry.writerIndex(getExpirationOffset());
    		cacheEntry.writeInt((int)expiration);
		} else {
			cacheEntry.writeByte(0);
		}
	}
	
	public long getExpiration() {
		if(getExpirationLength() != 0) {
			cacheEntry.readerIndex(getExpirationOffset());
			return cacheEntry.readUnsignedInt();
		}
		return 0;
	}

	public int getExpirationOffset() {
		return FLAG_LENGTH_LENGTH+EXPIRATION_LENGTH_LENGTH;
	}
	
	public int getFlagOffset() {
		return FLAG_LENGTH_LENGTH+EXPIRATION_LENGTH_LENGTH+getExpirationLength();
	}

	public byte getFlagLength() {
		return cacheEntry.getByte(FLAG_LENGTH_OFFSET);
	}
	
	public void writeValue(ByteBuf value) {
		cacheEntry.writeBytes(value);
	}
	
	public ByteBuf getFlags() {
		return cacheEntry.slice(getFlagOffset(), getFlagLength());
	}
	
	public ByteBuf getValue() {
		return cacheEntry.slice(getValueOffset(), getValueLength());
	}
	
	public int getValueLength() {
		return cacheEntry.capacity()-getValueOffset();
	}
	
	public int getValueOffset() {
		return getFlagLength()+FLAG_LENGTH_LENGTH+EXPIRATION_LENGTH_LENGTH+getExpirationLength();
	}

	public int getTotalFlagsAndValueLength() {
		return getValueLength()+getFlagLength();
	}
	
	public byte[] getCacheEntry() {
		return cacheEntry.array();
	}
	
	public long getCAS() {
		return MemcacheUtils.hash64(getCacheEntry(), getValueOffset(), getValueLength(), 0);
	}
}
