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

public class PlainAuthMsgHandler implements AuthMsgHandler {
	public static final String SUPPORTED_SASL_MECHS = "PLAIN";
	
	private String mech;
	private byte opcode;
	private int opaque;
	private int bodySize;
	private boolean authenticated = false;
	private String username;
	
	@Override
	public boolean listMechs(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		MemcacheUtils.logRequest(request);
		DefaultBinaryMemcacheResponse response = new DefaultBinaryMemcacheResponse();
		response.setStatus(BinaryMemcacheResponseStatus.SUCCESS);
		response.setOpcode(request.opcode());
		response.setOpaque(request.opaque());
		response.setTotalBodyLength(SUPPORTED_SASL_MECHS.length());
		ctx.write(response);
		try {
			MemcacheContent content = new DefaultLastMemcacheContent(Unpooled.wrappedBuffer(SUPPORTED_SASL_MECHS.getBytes("US-ASCII")));
			ctx.writeAndFlush(content);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
		return false;
	}

	@Override
	public boolean startAuth(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		MemcacheUtils.logRequest(request);
		if(!SUPPORTED_SASL_MECHS.contains(request.key())) {
			BinaryMemcacheResponse response = new DefaultBinaryMemcacheResponse();
			response.setStatus(BinaryMemcacheResponseStatus.AUTH_ERROR);
			response.setOpcode(BinaryMemcacheOpcodes.SASL_AUTH);
			response.setOpcode(request.opcode());
			response.setOpaque(request.opaque());
			ctx.writeAndFlush(response);
			return false;
		}
		mech = request.key();
		opaque = request.opaque();
		return true;
	}

	@Override
	public boolean startAuth(ChannelHandlerContext ctx, MemcacheContent content) {
		byte[] arrayContent = new byte[content.content().capacity()];
		content.content().readBytes(arrayContent);
		username = MemcacheUtils.extractSaslUsername(arrayContent);
		String password = MemcacheUtils.extractSaslPassword(arrayContent);
		System.out.println("Username: "+username);
		System.out.println("Password: "+password);
		authenticated = true;
		BinaryMemcacheResponse response = new DefaultBinaryMemcacheResponse();
		response.setStatus(BinaryMemcacheResponseStatus.SUCCESS);
		response.setOpcode(BinaryMemcacheOpcodes.SASL_AUTH);
		response.setOpaque(opaque);
		ctx.writeAndFlush(response);
		return false;
	}
	
	@Override
	public boolean isAuthenticated() {
		return authenticated;
	}
	
	@Override
	public String getUsername() {
		return username;
	}
	
	

}
