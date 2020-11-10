package cloudfoundry.memcache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
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
import org.junit.Test;

public class SecretKeyAuthMsgHandlerTest {

	private static final String SECRET_KEY = "some secret key";
	private static final String TEST_USER = "test-user";
	private static final String TEST_PASSWORD = "test-password";
	private static final String TEST_CACHE = "test-cache";
	
	@Test
	public void testAuthSuccess() {
		AuthMsgHandler authHandler = new SecretKeyAuthMsgHandler(new MemcacheMsgHandlerFactoryAuthStub(true), SECRET_KEY, TEST_USER, TEST_PASSWORD, TEST_CACHE);
		assertFalse(authHandler.isAuthenticated());
		BinaryMemcacheRequest request = new DefaultBinaryMemcacheRequest(Unpooled.wrappedBuffer("PLAIN".getBytes()));
		request.setOpaque(1234);
		request.setOpcode(BinaryMemcacheOpcodes.SASL_AUTH);
		String username = "plan"+UUID.randomUUID().toString();
		UUID appGuid = UUID.randomUUID();
		String password = Base64.encodeBase64String(DigestUtils.sha384((username+'|'+appGuid.toString()+SECRET_KEY).getBytes()));
		byte[] encodedAuth = createdEncodedAuth(username, appGuid, password);
		request.setTotalBodyLength(encodedAuth.length);
		ChannelHandlerContext ctxMock = EasyMock.createNiceMock(ChannelHandlerContext.class);
		Channel channelMock = EasyMock.createNiceMock(Channel.class);
		EasyMock.expect(channelMock.isOpen()).andReturn(true).anyTimes();
		EasyMock.expect(channelMock.isActive()).andReturn(true).anyTimes();
		EasyMock.replay(channelMock);
		EasyMock.expect(ctxMock.channel()).andReturn(channelMock).anyTimes();
		EasyMock.replay(ctxMock);
		authHandler.startAuth(ctxMock , request);
		
		MemcacheContent content = new DefaultMemcacheContent(Unpooled.wrappedBuffer(encodedAuth));
		authHandler.startAuth(ctxMock, content);
		assertTrue(authHandler.isAuthenticated());
		assertEquals(authHandler.getBindGuid(), appGuid);
		assertEquals(authHandler.getCacheName(), username);
		assertEquals(authHandler.getUsername(), username+'|'+appGuid.toString());
	}

	@Test
	public void testAuthFailure() {
		AuthMsgHandler authHandler = new SecretKeyAuthMsgHandler(new MemcacheMsgHandlerFactoryAuthStub(true), SECRET_KEY, TEST_USER, TEST_PASSWORD, TEST_CACHE);
		assertFalse(authHandler.isAuthenticated());
		BinaryMemcacheRequest request = new DefaultBinaryMemcacheRequest(Unpooled.wrappedBuffer("PLAIN".getBytes()));
		request.setOpaque(1234);
		request.setOpcode(BinaryMemcacheOpcodes.SASL_AUTH);
		UUID appGuid = UUID.randomUUID();
		String username = UUID.randomUUID().toString();
		String password = Base64.encodeBase64String(DigestUtils.sha384((username+SECRET_KEY+"Fake").getBytes()));
		byte[] encodedAuth = createdEncodedAuth(username, appGuid, password);
		request.setTotalBodyLength(encodedAuth.length);
		ChannelHandlerContext ctxMock = EasyMock.createNiceMock(ChannelHandlerContext.class);
		Channel channelMock = EasyMock.createNiceMock(Channel.class);
		EasyMock.expect(channelMock.isOpen()).andReturn(true).anyTimes();
		EasyMock.expect(channelMock.isActive()).andReturn(true).anyTimes();
		EasyMock.replay(channelMock);
		EasyMock.expect(ctxMock.channel()).andReturn(channelMock).anyTimes();
		EasyMock.replay(ctxMock);
		authHandler.startAuth(ctxMock , request);
		
		MemcacheContent content = new DefaultMemcacheContent(Unpooled.wrappedBuffer(encodedAuth));
		authHandler.startAuth(ctxMock, content);
		assertFalse(authHandler.isAuthenticated());
	}

	@Test
	public void testCacheFailure() {
		AuthMsgHandler authHandler = new SecretKeyAuthMsgHandler(new MemcacheMsgHandlerFactoryAuthStub(false), SECRET_KEY, TEST_USER, TEST_PASSWORD, TEST_CACHE);
		assertFalse(authHandler.isAuthenticated());
		BinaryMemcacheRequest request = new DefaultBinaryMemcacheRequest(Unpooled.wrappedBuffer("PLAIN".getBytes()));
		request.setOpaque(1234);
		request.setOpcode(BinaryMemcacheOpcodes.SASL_AUTH);
		String username = "plan"+UUID.randomUUID().toString();
		UUID appGuid = UUID.randomUUID();
		String password = Base64.encodeBase64String(DigestUtils.sha384((username+'|'+appGuid.toString()+SECRET_KEY).getBytes()));
		byte[] encodedAuth = createdEncodedAuth(username, appGuid, password);
		request.setTotalBodyLength(encodedAuth.length);
		ChannelHandlerContext ctxMock = EasyMock.createNiceMock(ChannelHandlerContext.class);
		Channel channelMock = EasyMock.createNiceMock(Channel.class);
		EasyMock.expect(channelMock.isOpen()).andReturn(true).anyTimes();
		EasyMock.expect(channelMock.isActive()).andReturn(true).anyTimes();
		EasyMock.replay(channelMock);
		EasyMock.expect(ctxMock.channel()).andReturn(channelMock).anyTimes();
		EasyMock.replay(ctxMock);
		authHandler.startAuth(ctxMock , request);
		
		MemcacheContent content = new DefaultMemcacheContent(Unpooled.wrappedBuffer(encodedAuth));
		authHandler.startAuth(ctxMock, content);
		assertFalse(authHandler.isAuthenticated());
	}

	@Test
	public void testTestUserSuccess() {
		AuthMsgHandler authHandler = new SecretKeyAuthMsgHandler(new MemcacheMsgHandlerFactoryAuthStub(true), SECRET_KEY, TEST_USER, TEST_PASSWORD, TEST_CACHE);
		assertFalse(authHandler.isAuthenticated());
		BinaryMemcacheRequest request = new DefaultBinaryMemcacheRequest(Unpooled.wrappedBuffer("PLAIN".getBytes()));
		request.setOpaque(1234);
		request.setOpcode(BinaryMemcacheOpcodes.SASL_AUTH);
		String username = TEST_USER;
		String password = TEST_PASSWORD;
		byte[] encodedAuth = createdEncodedAuth(username, null, password);
		request.setTotalBodyLength(encodedAuth.length);
		ChannelHandlerContext ctxMock = EasyMock.createNiceMock(ChannelHandlerContext.class);
		Channel channelMock = EasyMock.createNiceMock(Channel.class);
		EasyMock.expect(channelMock.isOpen()).andReturn(true).anyTimes();
		EasyMock.expect(channelMock.isActive()).andReturn(true).anyTimes();
		EasyMock.replay(channelMock);
		EasyMock.expect(ctxMock.channel()).andReturn(channelMock).anyTimes();
		EasyMock.replay(ctxMock);
		authHandler.startAuth(ctxMock , request);
		
		MemcacheContent content = new DefaultMemcacheContent(Unpooled.wrappedBuffer(encodedAuth));
		authHandler.startAuth(ctxMock, content);
		assertTrue(authHandler.isAuthenticated());
		assertEquals(authHandler.getCacheName(), TEST_CACHE);
	}

	private byte[] createdEncodedAuth(String username, UUID appGuid, String password) {
		String fullUsername = username;
		if(appGuid != null) {
			fullUsername = username+'|'+appGuid.toString();
		}
		byte[] encodedAuth =  new byte[1+fullUsername.length()+1+password.length()];
		System.arraycopy(fullUsername.getBytes(), 0, encodedAuth, 1, fullUsername.length());
		System.arraycopy(password.getBytes(), 0, encodedAuth, fullUsername.length()+2, password.length());
		return encodedAuth;
	}
}
