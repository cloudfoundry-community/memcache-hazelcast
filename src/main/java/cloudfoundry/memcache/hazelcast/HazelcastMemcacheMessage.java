package cloudfoundry.memcache.hazelcast;

import java.io.IOException;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class HazelcastMemcacheMessage implements IdentifiedDataSerializable {
	private boolean success;
	private short code;
	private String msg;
	private long cas;
	private HazelcastMemcacheCacheValue value;
	
	public HazelcastMemcacheMessage() { }

	public HazelcastMemcacheMessage(boolean success) {
		this.success = success;
	}

	public HazelcastMemcacheMessage(boolean success, HazelcastMemcacheCacheValue value) {
		this.success = success;
		this.value = value;
	}

	public HazelcastMemcacheMessage(boolean success, long cas) {
		this.success = success;
		this.cas = cas;
	}

	public HazelcastMemcacheMessage(boolean success, short code, String msg) {
		this.success = success;
		this.code = code;
		this.msg = msg;
	}
	
	public boolean isSuccess() {
		return success;
	}
	
	public short getCode() {
		return code;
	}
	
	public String getMsg() {
		return msg;
	}
	
	public long getCas() {
		return cas;
	}
	
	public HazelcastMemcacheCacheValue getValue() {
		return value;
	}
	
	@Override
	public int getFactoryId() {
		return 3;
	}
	
	@Override
	public int getId() {
		return 3;
	}
	
	@Override
	public void readData(ObjectDataInput in) throws IOException {
		success = in.readBoolean();
		code = in.readShort();
		msg = in.readUTF();
		value = in.readObject();
		cas = in.readLong();
	}
	
	@Override
	public void writeData(ObjectDataOutput out) throws IOException {
		out.writeBoolean(success);
		out.writeShort(code);
		out.writeUTF(msg);
		out.writeObject(value);
		out.writeLong(cas);
	}
}
