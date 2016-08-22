package cloudfoundry.memcache;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler;
import io.netty.handler.codec.memcache.FullMemcacheMessage;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheMessage;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheOpcodes;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheRequest;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheResponse;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheResponseStatus;
import io.netty.handler.codec.memcache.binary.DefaultFullBinaryMemcacheResponse;
import io.netty.handler.codec.memcache.binary.FullBinaryMemcacheResponse;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.io.UnsupportedEncodingException;
import java.net.SocketAddress;
import java.util.ArrayDeque;
import java.util.Deque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemcacheOrderingDuplexHandler extends ChannelDuplexHandler {
	private static final Logger LOGGER = LoggerFactory.getLogger(MemcacheOrderingDuplexHandler.class);

	private final Deque<DelayedMessage> msgOrderQueue = new ArrayDeque<>();
	
	private int maxQueueSize;
	
	

	public MemcacheOrderingDuplexHandler(int maxQueueSize) {
		super();
		this.maxQueueSize = maxQueueSize;
	}
	
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		if (msg instanceof BinaryMemcacheRequest) {
			BinaryMemcacheRequest request = (BinaryMemcacheRequest)msg;
			DelayedMessage delayedMessage = new DelayedMessage(new MemcacheRequestKey((BinaryMemcacheRequest) msg));
			msgOrderQueue.offer(delayedMessage);
			if(msgOrderQueue.size() >= maxQueueSize) {
				FullBinaryMemcacheResponse response;
				try {
					response = new DefaultFullBinaryMemcacheResponse(null, null, Unpooled.wrappedBuffer("Your request queue was full so this request was not processed.".getBytes("US-ASCII")));
				} catch (UnsupportedEncodingException e) {
					throw new RuntimeException(e);
				}
				response.setStatus(BinaryMemcacheResponseStatus.ENOMEM);
				response.setOpaque(request.opaque());
				response.setOpcode(request.opcode());
				write(ctx, response, ctx.newPromise());

				ReferenceCountUtil.release(msg);
				return;
			}
		}
		super.channelRead(ctx, msg);
	}
	
	

	@Override
	public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
		if (!(msg instanceof BinaryMemcacheMessage)) {
			throw new IllegalStateException("We only support MemcacheMessages.");
		}
		BinaryMemcacheMessage memcacheMessage = (BinaryMemcacheMessage) msg;
		MemcacheRequestKey key = new MemcacheRequestKey(memcacheMessage);
		if (msgOrderQueue.isEmpty()) {
			LOGGER.warn("We got a memcache message but the queue was empty.  Discarding.");
			return;
		}
		delayMsg(key, memcacheMessage, promise);
		while (!msgOrderQueue.isEmpty() && msgOrderQueue.peek().complete()) {
			DelayedMessage delayedMessage = msgOrderQueue.poll();
			if(LOGGER.isDebugEnabled()) {
				LOGGER.debug("Writing messages for: "+delayedMessage.requestKey);
			}
			while (!delayedMessage.responses.isEmpty()) {
				DelayedResponse delayedResponse = delayedMessage.responses.poll();
				if (delayedResponse.message instanceof QuietResponse) {
					continue;
				}
				ctx.write(delayedResponse.message, delayedResponse.promise);
			}
		}
		ctx.flush();
	}
	
	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		clearDelayedMessages();
	}
	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		LOGGER.error("Unexpected error.", cause);
		ctx.close();
	}

	private void delayMsg(MemcacheRequestKey key, BinaryMemcacheMessage memcacheMessage, ChannelPromise promise) {
		DelayedResponse delayedResponse = new DelayedResponse();
		delayedResponse.message = memcacheMessage;
		delayedResponse.promise = promise;
		
		for(DelayedMessage delayedMessage : msgOrderQueue) {
			if(delayedMessage.matchesKey(key) && !delayedMessage.complete()) {
				delayedMessage.responses.offer(delayedResponse);
				return;
			}
		}
	}


	private void clearDelayedMessages() {
		for (DelayedMessage delayedMsg : msgOrderQueue) {
			while (!delayedMsg.responses.isEmpty()) {
				DelayedResponse delayedMessage = delayedMsg.responses.poll();
				try {
					ReferenceCountUtil.release(delayedMessage.message);
				} catch (Exception e) {
					LOGGER.warn("Unexpected Error clearing delayedMsgQueue: " + e.getMessage());
				}
				try {
					delayedMessage.promise.cancel(false);
				} catch (Exception e) {
					LOGGER.warn("Unexpected Error failing promise: " + e.getMessage());
				}
			}
		}
	}
	
	private static class DelayedMessage {
		
		final MemcacheRequestKey requestKey;
		Deque<DelayedResponse> responses = new ArrayDeque<>();

		public DelayedMessage(MemcacheRequestKey requestKey) {
			this.requestKey = requestKey;
		}
		
		public boolean matchesKey(MemcacheRequestKey key) {
			return requestKey.equals(key);
		}

		@Override
		public int hashCode() {
			return requestKey.hashCode();
		}
		
		@Override
		public boolean equals(Object obj) {
			return requestKey.equals(obj);
		}
		
		private boolean complete() {
			if(responses.isEmpty()) {
				return false;
			}
			if (requestKey.getOpcode() == BinaryMemcacheOpcodes.STAT) {
				if (responses.peekLast().message instanceof FullMemcacheMessage) {
					FullBinaryMemcacheResponse lastMsgContent = (FullBinaryMemcacheResponse) responses.peekLast().message;
					if (lastMsgContent.keyLength() == 0 && lastMsgContent.totalBodyLength() == 0) {
						return true;
					}
				}
				return false;
			}
			return true;
		}
	}
	
	private static class DelayedResponse {
		BinaryMemcacheMessage message;
		ChannelPromise promise;
	}
}
