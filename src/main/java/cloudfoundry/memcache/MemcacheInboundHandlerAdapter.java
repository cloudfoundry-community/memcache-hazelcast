package cloudfoundry.memcache;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.memcache.FullMemcacheMessage;
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
	
	private static final Logger LOGGER = LoggerFactory.getLogger(MemcacheInboundHandlerAdapter.class);

	private final MemcacheMsgHandlerFactory msgHandlerFactory;

	private byte opcode = -1;
	private MemcacheMsgHandler currentMsgHandler;
	private final AuthMsgHandler authMsgHandler;
	private final Deque<DelayedMessage> msgOrderQueue;
	private final MemcacheStats memcacheStats;
	DelayedMessage delayedMessage;
	MemcacheServer memcacheServer;
	
	private int maxQueueSize;

	public MemcacheInboundHandlerAdapter(MemcacheMsgHandlerFactory msgHandlerFactory, AuthMsgHandler authMsgHandler, int maxQueueSize, MemcacheServer memcacheServer, MemcacheStats memcacheStats) {
		super();
		this.msgHandlerFactory = msgHandlerFactory;
		this.authMsgHandler = authMsgHandler;
		this.maxQueueSize = maxQueueSize;
		this.memcacheStats = memcacheStats;
		msgOrderQueue = new ArrayDeque<>(maxQueueSize+20);
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		ctx.channel().closeFuture().addListener(new GenericFutureListener<io.netty.util.concurrent.Future<Void>>() {
			@Override
			public void operationComplete(io.netty.util.concurrent.Future<Void> future) throws Exception {
				clearDelayedMessages();
			}
		});
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
				//Apply back pressure
				if(msgOrderQueue.size() > maxQueueSize) {
					LOGGER.info("Applying some back presure to client "+getAuthMsgHandler().getUsername()+" because queuesize is: "+msgOrderQueue.size());
					try {
						for(DelayedMessage delayedMessage : msgOrderQueue) {
							if(System.currentTimeMillis()-delayedMessage.getCreated() > 300000) {
								LOGGER.warn("Message at bottom of queue has been in the queue longer than 5 mintues.  Terminating the connection.");
								ctx.channel().close();
								return;
							}
							try {
								if(delayedMessage.sync()) {
									break;
								}
							} catch(InterruptedException e) {
								LOGGER.info("Attempt to apply backpresure halted by recieving Interrupt.");
								break;
							}
						}
					} catch (Exception e) {
						LOGGER.error("Unexpected failure applying back presure.  Closing the connection.", e);
						ctx.channel().close();
						return;
					}
				}

				delayedMessage = new DelayedMessage(new MemcacheRequestKey(request));
				msgOrderQueue.offer(delayedMessage);

				opcode = request.opcode();
				if(currentMsgHandler == null) {
					if(getAuthMsgHandler().isAuthenticated()) {
						currentMsgHandler = msgHandlerFactory.createMsgHandler(request, getAuthMsgHandler());
					} else {
						currentMsgHandler = new NoAuthMemcacheMsgHandler(request);
					}
					memcacheStats.logHit(opcode);
				}
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
					LOGGER.error("Recieved Non memcache request with SASL_STEP optcode.  This is an invalid state. Closing connection.");
					ctx.channel().close();
				}
				break;
			default:
				LOGGER.info("Failed to handle request with optcode: "+opcode);
				if(msg instanceof BinaryMemcacheRequest) {
					MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.UNKNOWN_COMMAND, "Unable to handle command: 0x"+Integer.toHexString(opcode)).send(ctx);
				} else {
					LOGGER.error("Recieved unsupported opcode as a non request.  This is an invalid state. Closing connection.");
					ctx.channel().close();
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
			LOGGER.error("Error while invoking MemcacheMsgHandler.  Closing the Channel in case we're in an odd state.", e);
			ctx.channel().close();
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

	private AuthMsgHandler getAuthMsgHandler() {
		return authMsgHandler;
	}
	
	private void completeRequest(Future<?> task) {
		delayedMessage.setTask(task);
		currentMsgHandler = null;
		opcode = -1;
		delayedMessage = null;
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		LOGGER.error("Unexpected Error.", cause);
		ctx.channel().close();
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
		
		public boolean sync() throws InterruptedException, ExecutionException {
			if(task == null || task.isDone() || task.isCancelled()) {
				return false;
			}
			task.get();
			return task.isDone();
		}
		
		private void clear() {
			if(task != null) {
				try {
					task.cancel(true);
				} catch (Exception e) {
					LOGGER.warn("Unexpected Error cancelling task for "+requestKey+": " + e.getMessage());
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
					LOGGER.warn("Unexpected Error failing promise for "+requestKey+": " + e.getMessage());
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
