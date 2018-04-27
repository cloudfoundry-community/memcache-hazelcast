package cloudfoundry.memcache;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.memcache.FullMemcacheMessage;
import io.netty.handler.codec.memcache.LastMemcacheContent;
import io.netty.handler.codec.memcache.MemcacheContent;
import io.netty.handler.codec.memcache.MemcacheObject;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheMessage;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheOpcodes;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheRequest;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheResponseStatus;
import io.netty.handler.codec.memcache.binary.FullBinaryMemcacheResponse;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.GenericFutureListener;

public class MemcacheInboundHandlerAdapter extends ChannelDuplexHandler {
	
	private static final int MAX_KEY_SIZE = 250;

	private static final Logger LOGGER = LoggerFactory.getLogger(MemcacheInboundHandlerAdapter.class);

	private final MemcacheMsgHandlerFactory msgHandlerFactory;

	private byte opcode = -1;
	private MemcacheMsgHandler currentMsgHandler;
	private final AuthMsgHandler authMsgHandler;
	private final Deque<DelayedMessage> msgOrderQueue;
	private final MemcacheStats memcacheStats;
	private ResponseSender failureResponse;
	DelayedMessage delayedMessage;
	MemcacheServer memcacheServer;
	private final int requestRateLimit;
	private final int queueSizeLimit;
	private volatile boolean scheduledRead = false;
	private long triggeredRequestLimitDuration = 0;
	private long triggeredRequestLimitStart = 0;
	private boolean triggeredRequestRateLimit = false;
	private long loggableRequestRate = 0;
	private boolean triggeredQueueLimit = false;
	private long loggableQueueSize = 0;
	

	public MemcacheInboundHandlerAdapter(MemcacheMsgHandlerFactory msgHandlerFactory, AuthMsgHandler authMsgHandler, int queueSizeLimit, int requestRateLimit, MemcacheServer memcacheServer, MemcacheStats memcacheStats) {
		super();
		this.msgHandlerFactory = msgHandlerFactory;
		this.authMsgHandler = authMsgHandler;
		this.queueSizeLimit = queueSizeLimit;
		this.requestRateLimit = requestRateLimit;
		this.memcacheStats = memcacheStats;
		msgOrderQueue = new ArrayDeque<>(queueSizeLimit+100);
		msgHandlerFactory.getScheduledExecutorService().scheduleAtFixedRate(() -> {
			if(LOGGER.isDebugEnabled()) {
				LOGGER.debug("Current load rate for "+getCurrentUser()+" is "+loggableRequestRate+" requests/10sec");
				LOGGER.debug("Current request queue size for "+getCurrentUser()+" is "+loggableQueueSize);
			}
			if(triggeredRequestRateLimit) {
				LOGGER.warn("The user "+getCurrentUser()+"'s requests are being rate limited at "+loggableRequestRate+" requests/10sec and lasted for "+triggeredRequestLimitDuration+"ms");
				loggableRequestRate = 0;
				triggeredRequestRateLimit = false;
			}
			if(triggeredQueueLimit) {
				LOGGER.warn("The user "+getCurrentUser()+"'s queue is being rate limited at a size of "+loggableQueueSize);
				triggeredQueueLimit = false;
				loggableQueueSize = 0;
			}
		}, 60, 60, TimeUnit.SECONDS);
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		ctx.channel().closeFuture().addListener(new GenericFutureListener<io.netty.util.concurrent.Future<Void>>() {
			@Override
			public void operationComplete(io.netty.util.concurrent.Future<Void> future) throws Exception {
				clearDelayedMessages();
			}
		});
		readIfNotRateLimited(ctx);
	}
	
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		try {
			if (!(msg instanceof MemcacheObject)) {
				return;
			}
			BinaryMemcacheRequest request = null;
			if (msg instanceof BinaryMemcacheRequest) {
				request = (BinaryMemcacheRequest) msg;
				delayedMessage = new DelayedMessage(new MemcacheRequestKey(request));
				msgOrderQueue.offer(delayedMessage);
				opcode = request.opcode();
				memcacheStats.logHit(opcode, getCurrentUser());
				if(Short.toUnsignedInt(((BinaryMemcacheRequest) msg).keyLength()) > MAX_KEY_SIZE) {
					LOGGER.debug("Key too big.  Skipping this request.");
					failureResponse = MemcacheUtils.returnFailure(opcode, request.opaque(), BinaryMemcacheResponseStatus.E2BIG, "Key too big.  Max Key is " + MAX_KEY_SIZE);
					return;
				} else {
					if(currentMsgHandler == null) {
						if(getAuthMsgHandler().isAuthenticated()) {
							currentMsgHandler = msgHandlerFactory.createMsgHandler(request, getAuthMsgHandler());
						} else {
							currentMsgHandler = new NoAuthMemcacheMsgHandler(request);
						}
					}
				}
			} else if(failureResponse != null) {
				if(msg instanceof LastMemcacheContent) {
					completeRequest(failureResponse.send(ctx));
					return;
				}
				return;
			} else if(currentMsgHandler == null) {
				return;
			}
			
			switch (opcode) {
			case BinaryMemcacheOpcodes.GET:
			case BinaryMemcacheOpcodes.GETQ:
			{
				Future<?> task = currentMsgHandler.get(ctx, request);
				if(task != null) {
					completeRequest(task);
				}
				break;
			}
			case BinaryMemcacheOpcodes.GETK:
			case BinaryMemcacheOpcodes.GETKQ:
			{
				Future<?> task = currentMsgHandler.getK(ctx, request);
				if(task != null) {
					completeRequest(task);
				}
				break;
			}
			case BinaryMemcacheOpcodes.SET:
			case BinaryMemcacheOpcodes.SETQ:
			{
				Future<?> task;
				if(msg instanceof BinaryMemcacheRequest) {
					task = currentMsgHandler.set(ctx, request);
				} else {
					task = currentMsgHandler.set(ctx, (MemcacheContent)msg);
				}
				if(task != null) {
					completeRequest(task);
				}
				break;
			}
			case BinaryMemcacheOpcodes.ADD:
			case BinaryMemcacheOpcodes.ADDQ:
			{
				Future<?> task;
				if(msg instanceof BinaryMemcacheRequest) {
					task = currentMsgHandler.add(ctx, request);
				} else {
					task = currentMsgHandler.add(ctx, (MemcacheContent)msg);
				}
				if(task != null) {
					completeRequest(task);
				}
				break;
			}
			case BinaryMemcacheOpcodes.REPLACE:
			case BinaryMemcacheOpcodes.REPLACEQ:
			{
				Future<?> task;
				if(msg instanceof BinaryMemcacheRequest) {
					task = currentMsgHandler.replace(ctx, request);
				} else {
					task = currentMsgHandler.replace(ctx, (MemcacheContent)msg);
				}
				if(task != null) {
					completeRequest(task);
				}
				break;
			}
			case BinaryMemcacheOpcodes.DELETE:
			case BinaryMemcacheOpcodes.DELETEQ:
			{
				Future<?> task = currentMsgHandler.delete(ctx, request);
				if(task != null) {
					completeRequest(task);
				}
				break;
			}
			case BinaryMemcacheOpcodes.INCREMENT:
			case BinaryMemcacheOpcodes.INCREMENTQ:
			{
				Future<?> task = currentMsgHandler.increment(ctx, request);
				if(task != null) {
					completeRequest(task);
				}
				break;
			}
			case BinaryMemcacheOpcodes.DECREMENT:
			case BinaryMemcacheOpcodes.DECREMENTQ:
			{
				Future<?> task = currentMsgHandler.decrement(ctx, request);
				if(task != null) {
					completeRequest(task);
				}
				break;
			}
			case BinaryMemcacheOpcodes.QUIT:
			case BinaryMemcacheOpcodes.QUITQ:
			{
				Future<?> task = currentMsgHandler.quit(ctx, request);
				if(task != null) {
					completeRequest(task);
				}
				break;
			}
			case BinaryMemcacheOpcodes.FLUSH:
			case BinaryMemcacheOpcodes.FLUSHQ:
			{
				Future<?> task = currentMsgHandler.flush(ctx, request);
				if(task != null) {
					completeRequest(task);
				}
				break;
			}
			case BinaryMemcacheOpcodes.NOOP:
			{
				Future<?> task = currentMsgHandler.noop(ctx, request);
				if(task != null) {
					completeRequest(task);
				}
				break;
			}
			case BinaryMemcacheOpcodes.VERSION:
			{
				Future<?> task = currentMsgHandler.version(ctx, request);
				if(task != null) {
					completeRequest(task);
				}
				break;
			}
			case BinaryMemcacheOpcodes.APPEND:
			case BinaryMemcacheOpcodes.APPENDQ:
			{
				Future<?> task;
				if(msg instanceof BinaryMemcacheRequest) {
					task = currentMsgHandler.append(ctx, request);
				} else {
					task = currentMsgHandler.append(ctx, (MemcacheContent)msg);
				}
				if(task != null) {
					completeRequest(task);
				}
				break;
			}
			case BinaryMemcacheOpcodes.PREPEND:
			case BinaryMemcacheOpcodes.PREPENDQ:
			{
				Future<?> task;
				if(msg instanceof BinaryMemcacheRequest) {
					task = currentMsgHandler.prepend(ctx, request);
				} else {
					task = currentMsgHandler.prepend(ctx, (MemcacheContent)msg);
				}
				if(task != null) {
					completeRequest(task);
				}
				break;
			}
			case BinaryMemcacheOpcodes.STAT:
			{
				Future<?> task = currentMsgHandler.stat(ctx, request);
				if(task != null) {
					completeRequest(task);
				}
				break;
			}
			case BinaryMemcacheOpcodes.TOUCH:
			{
				Future<?> task = currentMsgHandler.touch(ctx, request);
				if(task != null) {
					completeRequest(task);
				}
				break;
			}
			case BinaryMemcacheOpcodes.GAT:
			case BinaryMemcacheOpcodes.GATQ:
			{
				Future<?> task = currentMsgHandler.gat(ctx, request);
				if(task != null) {
					completeRequest(task);
				}
				break;
			}
			case BinaryMemcacheOpcodes.GATK:
			case BinaryMemcacheOpcodes.GATKQ:
			{
				Future<?> task = currentMsgHandler.gat(ctx, request);
				if(task != null) {
					completeRequest(task);
				}
				break;
			}
			case BinaryMemcacheOpcodes.SASL_LIST_MECHS:
			{
				Future<?> task = getAuthMsgHandler().listMechs(ctx, request);
				if(task != null) {
					completeRequest(task);
				}
				break;
			}
			case BinaryMemcacheOpcodes.SASL_AUTH:
			{
				Future<?> task;
				if(msg instanceof BinaryMemcacheRequest) {
					task = getAuthMsgHandler().startAuth(ctx, request);
				} else {
					task = getAuthMsgHandler().startAuth(ctx, (MemcacheContent)msg);
				}
				if(task != null) {
					completeRequest(task);
				}
				break;
			}
			case BinaryMemcacheOpcodes.SASL_STEP:
				if(msg instanceof BinaryMemcacheRequest) {
					MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.AUTH_ERROR, "We don't support any auth mechanisms that require a step.").send(ctx);
				} else {
					LOGGER.error("Received Non memcache request with SASL_STEP optcode.  This is an invalid state. Closing connection.");
					try {
						ctx.channel().close().await(1, TimeUnit.SECONDS);
					} catch(Exception e) {
						LOGGER.debug("Failure closing connection. ", e);
					}
				}
				break;
			default:
				LOGGER.info("Failed to handle request with optcode: "+opcode);
				if(msg instanceof BinaryMemcacheRequest) {
					MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.UNKNOWN_COMMAND, "Unable to handle command: 0x"+Integer.toHexString(opcode)).send(ctx);
				} else {
					LOGGER.error("Received unsupported opcode as a non request.  This is an invalid state. Closing connection.");
					try {
						ctx.channel().close().await(1, TimeUnit.SECONDS);
					} catch(Exception e) {
						LOGGER.debug("Failure closing connection. ", e);
					}
				}
			}
		} catch(IllegalStateException e) {
			LOGGER.error("IllegalStateException thrown.  Shutting down the server because we don't know the state we're in.", e);
			try {
				msgHandlerFactory.shutdown();
			} catch(Throwable t) { }
			try {
				memcacheServer.shutdown();
			} catch(Throwable t) { }
		} catch(Throwable e) {
			LOGGER.error("Error while invoking MemcacheMsgHandler.  Closing the Channel in case we're in an odd state.  Current User: "+getCurrentUser(), e);
			try {
				ctx.channel().close().await(1, TimeUnit.SECONDS);
			} catch(Exception e2) {
				LOGGER.debug("Failure closing connection. ", e2);
			}
		} finally {
			ReferenceCountUtil.release(msg);
		}
	}

	@Override
	public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
		if (!(msg instanceof BinaryMemcacheMessage)) {
			throw new IllegalStateException("We only support MemcacheMessages.");
		}
		BinaryMemcacheMessage memcacheMessage = (BinaryMemcacheMessage) msg;
		MemcacheRequestKey key = new MemcacheRequestKey(memcacheMessage);
		if (msgOrderQueue.isEmpty()) {
			LOGGER.warn("We got a memcache message but the queue was empty.  Discarding.  User="+getCurrentUser());
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
		if(!msgOrderQueue.isEmpty() && System.currentTimeMillis()-msgOrderQueue.peek().getCreated() > 60000) {
			LOGGER.warn("Message at bottom of queue has been in the queue longer than 1 mintue.  Terminating the connection.  User="+getCurrentUser());
			try {
				ctx.channel().close().await(1, TimeUnit.SECONDS);
			} catch(Exception e) {
				LOGGER.debug("Failure closing connection. ", e);
			}
			return;
		}
		readIfNotRateLimited(ctx);
	}

	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
		readIfNotRateLimited(ctx);
		super.channelReadComplete(ctx);
	}

	private void readIfNotRateLimited(ChannelHandlerContext ctx) {
		boolean requestLimitHit = false;
		long load = memcacheStats.requestsInWindow(getCurrentUser());

		if(load > loggableRequestRate) {
			loggableRequestRate = load;
		}

		if(load > requestRateLimit) {
			requestLimitHit = true;
			if(!scheduledRead) {
				scheduledRead = true;
				if(triggeredRequestLimitStart == 0) {
					triggeredRequestLimitStart = System.currentTimeMillis();
				}
				msgHandlerFactory.getScheduledExecutorService().schedule(() -> {
					scheduledRead = false;
					readIfNotRateLimited(ctx);
				}, memcacheStats.msLeftInWindow(getCurrentUser()), TimeUnit.MILLISECONDS);
			}
		}

		boolean queueLimitHit = false;
		int queueSize = msgOrderQueue.size();
		if(queueSize > loggableQueueSize) {
			loggableQueueSize = queueSize;
		}
		if(queueSize >= queueSizeLimit) {
			queueLimitHit = true;
			triggeredQueueLimit = true;
		}
		
		if(!requestLimitHit && !queueLimitHit) {
			if(triggeredRequestLimitStart != 0) {
				triggeredRequestRateLimit = true;
				long duration = System.currentTimeMillis() - triggeredRequestLimitStart;
				if(duration > triggeredRequestLimitDuration) {
					triggeredRequestLimitDuration = duration;
				}
				triggeredRequestLimitStart = 0;
			}
			ctx.read();
		}
	}

	private String getCurrentUser() {
		if(getAuthMsgHandler() == null || getAuthMsgHandler().getUsername() == null) {
			return "NotYetAuthenticatedUser";
		}
		return getAuthMsgHandler().getUsername();
	}

	private AuthMsgHandler getAuthMsgHandler() {
		return authMsgHandler;
	}
	
	private void completeRequest(Future<?> task) {
		delayedMessage.setTask(task);
		currentMsgHandler = null;
		opcode = -1;
		delayedMessage = null;
		failureResponse = null;
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		if(cause != null && cause.getMessage() != null && cause.getMessage().contains("Connection reset")) {
			LOGGER.info("Channel for "+getCurrentUser()+" Unexpectedly reset.");
		} else {
			LOGGER.error("Unexpected Error for user: "+getCurrentUser(), cause);
		}
		try {
			ctx.channel().close().await(1, TimeUnit.SECONDS);
		} catch(Exception e) {
			LOGGER.debug("Failure closing connection. ", e);
		}
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
			delayedMsg.clear();
		}
	}

	private static class DelayedMessage {

		final MemcacheRequestKey requestKey;
		Deque<DelayedResponse> responses = new ArrayDeque<>();
		Future<?> task = null;
		long created = System.currentTimeMillis();

		public DelayedMessage(MemcacheRequestKey requestKey) {
			this.requestKey = requestKey;
		}

		public boolean matchesKey(MemcacheRequestKey key) {
			return requestKey.equals(key);
		}
		
		public void setTask(Future<?> task) {
			this.task = task;
		}
		
		public long getCreated() {
			return created;
		}

		@Override
		public int hashCode() {
			return requestKey.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			return requestKey.equals(obj);
		}
		
		private void clear() {
			if(task != null) {
				try {
					task.cancel(true);
				} catch (Exception e) {
					LOGGER.warn("Unexpected Error cancelling task key '"+requestKey+"': " + e.getMessage());
				}
			}
			while (!this.responses.isEmpty()) {
				DelayedResponse delayedMessage = this.responses.poll();
				try {
					ReferenceCountUtil.release(delayedMessage.message);
				} catch (Exception e) {
					LOGGER.warn("Unexpected Error clearing delayedMsgQueue for "+requestKey+": " + e.getMessage());
				}
				try {
					delayedMessage.promise.cancel(true);
				} catch (Exception e) {
					LOGGER.warn("Unexpected Error cancelling promise for "+requestKey+": " + e.getMessage());
				}
			}
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

	public static class DelayedResponse {
		BinaryMemcacheMessage message;
		ChannelPromise promise;
	}

}
