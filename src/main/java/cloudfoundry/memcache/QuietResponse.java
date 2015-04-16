package cloudfoundry.memcache;

import io.netty.handler.codec.memcache.binary.AbstractBinaryMemcacheMessage;

public class QuietResponse extends AbstractBinaryMemcacheMessage {

	public QuietResponse(byte opcode, int opaque) {
		super(null, null);
		setOpcode(opcode);
		setOpaque(opaque);
	}
}
