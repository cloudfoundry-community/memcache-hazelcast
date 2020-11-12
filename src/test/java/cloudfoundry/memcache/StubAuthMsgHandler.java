package cloudfoundry.memcache;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.memcache.MemcacheContent;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheOpcodes;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheRequest;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheResponse;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheResponseStatus;
import io.netty.handler.codec.memcache.binary.DefaultBinaryMemcacheResponse;
import io.netty.handler.codec.memcache.binary.DefaultFullBinaryMemcacheResponse;
import io.netty.handler.codec.memcache.binary.FullBinaryMemcacheResponse;
import java.io.UnsupportedEncodingException;
import java.util.UUID;

public class StubAuthMsgHandler implements AuthMsgHandler {
	
	String username = "small|123";
	UUID appGuid;
	
	int opaque;
	byte opcode;
	
	public StubAuthMsgHandler() {
		this.appGuid = UUID.randomUUID();
	}

	@Override
	public ChannelFuture listMechs(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		MemcacheUtils.logRequest(request);
		FullBinaryMemcacheResponse response;
		try {
			response = new DefaultFullBinaryMemcacheResponse(null, null, Unpooled.wrappedBuffer(SecretKeyAuthMsgHandler.SUPPORTED_SASL_MECHS.getBytes("US-ASCII")));
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
		response.setStatus(BinaryMemcacheResponseStatus.SUCCESS);
		response.setOpcode(request.opcode());
		response.setOpaque(request.opaque());
		response.setTotalBodyLength(SecretKeyAuthMsgHandler.SUPPORTED_SASL_MECHS.length());
		return MemcacheUtils.writeAndFlush(ctx, response);
	}

	@Override
	public ChannelFuture startAuth(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		MemcacheUtils.logRequest(request);
		opaque = request.opaque();
		return null;
	}

	@Override
	public ChannelFuture startAuth(ChannelHandlerContext ctx, MemcacheContent content) {
		byte[] arrayContent = new byte[content.content().capacity()];
		content.content().readBytes(arrayContent);
		username = MemcacheUtils.extractSaslUsername(arrayContent);
		BinaryMemcacheResponse response = new DefaultBinaryMemcacheResponse();
		response.setStatus(BinaryMemcacheResponseStatus.SUCCESS);
		response.setOpcode(BinaryMemcacheOpcodes.SASL_AUTH);
		response.setOpaque(opaque);
		return MemcacheUtils.writeAndFlush(ctx, response);
	}
	
	public boolean isAuthenticated() {
		return username != null;
	}
	
	@Override
	public UUID getBindGuid() {
		return appGuid;
	}
	
	@Override
	public String getCacheName() {
		return username;
	}
	
	@Override
	public String getUsername() {
		return username;
	}
}
