package cloudfoundry.memcache;

import io.netty.handler.codec.memcache.binary.BinaryMemcacheMessage;

public class MemcacheRequestKey {
	private final byte opcode;
	private final int opaque;
	private String key;
	public MemcacheRequestKey(BinaryMemcacheMessage message) {
		this.opcode = message.opcode();
		this.opaque = message.opaque();
		this.key = message.key();
	}
	
	public byte getOpcode() {
		return opcode;
	}
	
	public boolean matcheMessage(BinaryMemcacheMessage message) {
		if(message.opcode() == opcode && message.opaque() == opaque) {
			if(message.key() != null && key != null) {
				return message.key().equals(key);
			}
			return true;
		}
		return false;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + opaque;
		result = prime * result + opcode;
		return result;
	}


	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		MemcacheRequestKey other = (MemcacheRequestKey) obj;
		if (opaque != other.opaque) return false;
		if (opcode != other.opcode) return false;
		if(other.key != null && key != null) {
			return other.key.equals(key);
		}
		return true;
	}

	@Override
	public String toString() {
		return "MemcacheRequestKey [opcode=" + opcode + ", opaque=" + opaque + ", key=" + key + "]";
	}
}
