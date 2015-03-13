package cloudfoundry.memcache;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.memcache.DefaultLastMemcacheContent;
import io.netty.handler.codec.memcache.MemcacheContent;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheOpcodes;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheRequest;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheResponse;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheResponseStatus;
import io.netty.handler.codec.memcache.binary.DefaultBinaryMemcacheResponse;

import java.io.UnsupportedEncodingException;

public class StubAuthMsgHandler implements AuthMsgHandler {
	
	String username;
	
	int opaque;
	byte opcode;
	
	public StubAuthMsgHandler(String username) {
		this.username = username;
	}

	@Override
	public boolean listMechs(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		MemcacheUtils.logRequest(request);
		DefaultBinaryMemcacheResponse response = new DefaultBinaryMemcacheResponse();
		response.setStatus(BinaryMemcacheResponseStatus.SUCCESS);
		response.setOpcode(request.opcode());
		response.setOpaque(request.opaque());
		response.setTotalBodyLength(PlainAuthMsgHandler.SUPPORTED_SASL_MECHS.length());
		ctx.write(response);
		try {
			MemcacheContent content = new DefaultLastMemcacheContent(Unpooled.wrappedBuffer(PlainAuthMsgHandler.SUPPORTED_SASL_MECHS.getBytes("US-ASCII")));
			ctx.writeAndFlush(content);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
		return false;
	}

	@Override
	public boolean startAuth(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		MemcacheUtils.logRequest(request);
		opaque = request.opaque();
		return true;
	}

	@Override
	public boolean startAuth(ChannelHandlerContext ctx, MemcacheContent content) {
		byte[] arrayContent = new byte[content.content().capacity()];
		content.content().readBytes(arrayContent);
		username = MemcacheUtils.extractSaslUsername(arrayContent);
		System.out.println("Username: "+username);
		BinaryMemcacheResponse response = new DefaultBinaryMemcacheResponse();
		response.setStatus(BinaryMemcacheResponseStatus.SUCCESS);
		response.setOpcode(BinaryMemcacheOpcodes.SASL_AUTH);
		response.setOpaque(opaque);
		ctx.writeAndFlush(response);
		return false;
	}
	
	public boolean isAuthenticated() {
		return true;
	}
	
	@Override
	public String getUsername() {
		return username;
	}
}
