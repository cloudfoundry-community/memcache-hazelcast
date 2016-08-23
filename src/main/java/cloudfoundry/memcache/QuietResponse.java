package cloudfoundry.memcache;

import io.netty.handler.codec.memcache.binary.AbstractBinaryMemcacheMessage;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheResponse;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheResponseStatus;

public class QuietResponse extends AbstractBinaryMemcacheMessage {

	public QuietResponse(byte opcode, int opaque) {
		super(null, null);
		setOpcode(opcode);
		setOpaque(opaque);
	}
//	
//	@Override
//	public short status() {
//		// TODO Auto-generated method stub
//		return BinaryMemcacheResponseStatus.SUCCESS;
//	}
//	
//	@Override
//	public BinaryMemcacheResponse touch() {
//		// TODO Auto-generated method stub
//		super.touch();
//		return this;
//	}
//	
//	@Override
//	public BinaryMemcacheResponse touch(Object hint) {
//		// TODO Auto-generated method stub
//		super.touch(hint);
//		return this;
//	}
//	
//	@Override
//	public BinaryMemcacheResponse retain() {
//		// TODO Auto-generated method stub
//		super.retain();
//		return this;
//	}
//	
//	@Override
//	public BinaryMemcacheResponse retain(int increment) {
//		// TODO Auto-generated method stub
//		super.retain(increment);
//		return this;
//	}
//	
//	@Override
//	public BinaryMemcacheResponse setStatus(short status) {
//		// TODO Auto-generated method stub
//		return null;
//	}
}
