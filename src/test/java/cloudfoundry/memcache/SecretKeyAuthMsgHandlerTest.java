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
		String username = UUID.randomUUID().toString();
		String password = Base64.encodeBase64String(DigestUtils.sha384((username+SECRET_KEY).getBytes()));
		byte[] encodedAuth = createdEncodedAuth(username, password);
		request.setTotalBodyLength(encodedAuth.length);
		authHandler.startAuth(EasyMock.createNiceMock(ChannelHandlerContext.class) , request);
		
		MemcacheContent content = new DefaultMemcacheContent(Unpooled.wrappedBuffer(encodedAuth));
		authHandler.startAuth(EasyMock.createNiceMock(ChannelHandlerContext.class), content);
		assertTrue(authHandler.isAuthenticated());
	}

	@Test
	public void testAuthFailure() {
		AuthMsgHandler authHandler = new SecretKeyAuthMsgHandler(SECRET_KEY);
		assertFalse(authHandler.isAuthenticated());
		BinaryMemcacheRequest request = new DefaultBinaryMemcacheRequest("PLAIN");
		request.setOpaque(1234);
		request.setOpcode(BinaryMemcacheOpcodes.SASL_AUTH);
		String username = UUID.randomUUID().toString();
		String password = Base64.encodeBase64String(DigestUtils.sha384((username+SECRET_KEY+"Fake").getBytes()));
		byte[] encodedAuth = createdEncodedAuth(username, password);
		request.setTotalBodyLength(encodedAuth.length);
		authHandler.startAuth(EasyMock.createNiceMock(ChannelHandlerContext.class) , request);
		
		MemcacheContent content = new DefaultMemcacheContent(Unpooled.wrappedBuffer(encodedAuth));
		authHandler.startAuth(EasyMock.createNiceMock(ChannelHandlerContext.class), content);
		assertFalse(authHandler.isAuthenticated());
	}

	private byte[] createdEncodedAuth(String username, String password) {
		byte[] encodedAuth =  new byte[username.length()+password.length()+2];
		System.arraycopy(username.getBytes(), 0, encodedAuth, 1, username.length());
		System.arraycopy(password.getBytes(), 0, encodedAuth, username.length()+2, password.length());
		return encodedAuth;
	}
}
