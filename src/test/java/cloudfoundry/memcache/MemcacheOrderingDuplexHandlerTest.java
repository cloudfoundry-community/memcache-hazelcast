package cloudfoundry.memcache;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheOpcodes;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheRequest;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheResponse;
import io.netty.handler.codec.memcache.binary.DefaultBinaryMemcacheRequest;
import io.netty.handler.codec.memcache.binary.DefaultBinaryMemcacheResponse;

import org.easymock.EasyMock;
import org.testng.annotations.Test;

public class MemcacheOrderingDuplexHandlerTest {
	@Test
	public void testRead() throws Exception {
		MemcacheOrderingDuplexHandler handler = new MemcacheOrderingDuplexHandler(100);
		BinaryMemcacheRequest request1 = new DefaultBinaryMemcacheRequest();
		request1.setOpcode(BinaryMemcacheOpcodes.GET);
		request1.setOpaque(1);
		BinaryMemcacheRequest request2 = new DefaultBinaryMemcacheRequest();
		request2.setOpcode(BinaryMemcacheOpcodes.GET);
		request2.setOpaque(2);
		BinaryMemcacheRequest request3 = new DefaultBinaryMemcacheRequest();
		request3.setOpcode(BinaryMemcacheOpcodes.SET);
		request3.setOpaque(1);
		
		BinaryMemcacheResponse response1 = new DefaultBinaryMemcacheResponse();
		response1.setOpcode(BinaryMemcacheOpcodes.GET);
		response1.setOpaque(1);
		BinaryMemcacheResponse response2 = new DefaultBinaryMemcacheResponse();
		response2.setOpcode(BinaryMemcacheOpcodes.GET);
		response2.setOpaque(2);
		QuietResponse response3 = new QuietResponse(BinaryMemcacheOpcodes.SET, 1);
		
		
		ChannelPromise promise = EasyMock.createNiceMock(ChannelPromise.class);
		ChannelHandlerContext ctxMock = EasyMock.createNiceMock(ChannelHandlerContext.class);
		
		EasyMock.expect(ctxMock.write(response1, promise)).andReturn(null);
		EasyMock.expect(ctxMock.write(response2, promise)).andReturn(null);
		
		EasyMock.replay(ctxMock, promise);
		handler.channelRead(ctxMock, request1);
		handler.channelRead(ctxMock, request2);
		handler.channelRead(ctxMock, request3);

		handler.write(ctxMock, response3, promise);
		handler.write(ctxMock, response2, promise);
		handler.write(ctxMock, response1, promise);

		EasyMock.verify(ctxMock);
	}
}
