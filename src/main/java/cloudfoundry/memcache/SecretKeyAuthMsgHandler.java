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
import java.util.UUID;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.digest.DigestUtils;

public class SecretKeyAuthMsgHandler implements AuthMsgHandler {
	public static final String SUPPORTED_SASL_MECHS = "PLAIN";

	private int opaque;
	boolean authenticated = false;
	private String username;
	private String key;
	private UUID appGuid;
	private String cacheName;

	public SecretKeyAuthMsgHandler(String key) {
		this.key = key;
	}

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
		if (!SUPPORTED_SASL_MECHS.contains(request.key())) {
			return MemcacheUtils.returnFailure(BinaryMemcacheOpcodes.SASL_AUTH, opaque, BinaryMemcacheResponseStatus.AUTH_ERROR, "Invalid Authentication Mechanism: "+request.key()).send(ctx);
		}
		opaque = request.opaque();
		return true;
	}

	@Override
	public boolean startAuth(ChannelHandlerContext ctx, MemcacheContent content) {
		byte[] arrayContent = new byte[content.content().capacity()];
		content.content().readBytes(arrayContent);
		username = MemcacheUtils.extractSaslUsername(arrayContent);
		String password = MemcacheUtils.extractSaslPassword(arrayContent);
		boolean validPassword = validatePassword(username, password);
		if (validPassword) {
			authenticated = true;
			cacheName = username.substring(0, username.lastIndexOf('|'));
			appGuid = UUID.fromString(username.substring(username.lastIndexOf('|')+1, username.length()));
			BinaryMemcacheResponse response = new DefaultBinaryMemcacheResponse();
			response.setStatus(BinaryMemcacheResponseStatus.SUCCESS);
			response.setOpcode(BinaryMemcacheOpcodes.SASL_AUTH);
			response.setOpaque(opaque);
			ctx.writeAndFlush(response);
		} else {
			authenticated = false;
			return MemcacheUtils.returnFailure(BinaryMemcacheOpcodes.SASL_AUTH, opaque, BinaryMemcacheResponseStatus.AUTH_ERROR, "Invalid Username or Password").send(ctx);
		}
		return false;
	}

	private boolean validatePassword(String username, String password) {
		byte[] hash = DigestUtils.sha384((username + key));
		return Base64.encodeBase64String(hash).equals(password);
	}

	@Override
	public boolean isAuthenticated() {
		return authenticated;
	}

	@Override
	public String getUsername() {
		return username;
	}
	
	@Override
	public String getCacheName() {
		return cacheName;
	}

	@Override
	public UUID getAppGuid() {
		return appGuid;
	}
}
