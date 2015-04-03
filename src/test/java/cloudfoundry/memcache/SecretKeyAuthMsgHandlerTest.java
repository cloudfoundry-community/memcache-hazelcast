package cloudfoundry.memcache;

import static org.testng.Assert.*;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.memcache.DefaultMemcacheContent;
import io.netty.handler.codec.memcache.MemcacheContent;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheOpcodes;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheRequest;
import io.netty.handler.codec.memcache.binary.DefaultBinaryMemcacheRequest;

import java.util.UUID;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.digest.DigestUtils;
import org.easymock.EasyMock;
import org.testng.annotations.Test;

public class SecretKeyAuthMsgHandlerTest {

	private static final String SECRET_KEY = "some secret key";

	@Test
	public void testAuthSuccess() {
		AuthMsgHandler authHandler = new SecretKeyAuthMsgHandler(SECRET_KEY);
		assertFalse(authHandler.isAuthenticated());
		BinaryMemcacheRequest request = new DefaultBinaryMemcacheRequest("PLAIN");
		request.setOpaque(1234);
		request.setOpcode(BinaryMemcacheOpcodes.SASL_AUTH);
		String username = "plan"+UUID.randomUUID().toString();
		UUID appGuid = UUID.randomUUID();
		String password = Base64.encodeBase64String(DigestUtils.sha384((username+'|'+appGuid.toString()+SECRET_KEY).getBytes()));
		byte[] encodedAuth = createdEncodedAuth(username, appGuid, password);
		request.setTotalBodyLength(encodedAuth.length);
		ChannelHandlerContext ctxMock = EasyMock.createNiceMock(ChannelHandlerContext.class);
		EasyMock.replay(ctxMock);
		authHandler.startAuth(ctxMock , request);
		
		MemcacheContent content = new DefaultMemcacheContent(Unpooled.wrappedBuffer(encodedAuth));
		authHandler.startAuth(ctxMock, content);
		assertTrue(authHandler.isAuthenticated());
		assertEquals(authHandler.getAppGuid(), appGuid);
		assertEquals(authHandler.getCacheName(), username);
		assertEquals(authHandler.getUsername(), username+'|'+appGuid.toString());
	}

	@Test
	public void testAuthFailure() {
		AuthMsgHandler authHandler = new SecretKeyAuthMsgHandler(SECRET_KEY);
		assertFalse(authHandler.isAuthenticated());
		BinaryMemcacheRequest request = new DefaultBinaryMemcacheRequest("PLAIN");
		request.setOpaque(1234);
		request.setOpcode(BinaryMemcacheOpcodes.SASL_AUTH);
		UUID appGuid = UUID.randomUUID();
		String username = UUID.randomUUID().toString();
		String password = Base64.encodeBase64String(DigestUtils.sha384((username+SECRET_KEY+"Fake").getBytes()));
		byte[] encodedAuth = createdEncodedAuth(username, appGuid, password);
		request.setTotalBodyLength(encodedAuth.length);
		ChannelHandlerContext ctxMock = EasyMock.createNiceMock(ChannelHandlerContext.class);
		EasyMock.replay(ctxMock);
		authHandler.startAuth(ctxMock , request);
		
		MemcacheContent content = new DefaultMemcacheContent(Unpooled.wrappedBuffer(encodedAuth));
		authHandler.startAuth(ctxMock, content);
		assertFalse(authHandler.isAuthenticated());
	}

	private byte[] createdEncodedAuth(String username, UUID appGuid, String password) {
		String appGuidString = appGuid.toString();
		String fullUsername = username+'|'+appGuidString;
		byte[] encodedAuth =  new byte[1+fullUsername.length()+1+password.length()];
		System.arraycopy(fullUsername.getBytes(), 0, encodedAuth, 1, fullUsername.length());
		System.arraycopy(password.getBytes(), 0, encodedAuth, fullUsername.length()+2, password.length());
		return encodedAuth;
	}
}
