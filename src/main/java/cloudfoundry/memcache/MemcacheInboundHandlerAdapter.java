package cloudfoundry.memcache;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFutureListener;
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
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.GenericFutureListener;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemcacheInboundHandlerAdapter extends ChannelDuplexHandler {

	private static final Duration REQUEST_QUEUE_MAX_AGE = Duration.ofMinutes(1);

	private static final int MAX_KEY_SIZE = 250;

	private static final Logger LOGGER = LoggerFactory.getLogger(MemcacheInboundHandlerAdapter.class);

	public static final GenericFutureListener<io.netty.util.concurrent.Future<Void>> FAILURE_LOGGING_LISTENER = future -> {
		try {
			future.get();
		} catch (Exception e) {
			LOGGER.warn("Error closing Channel. {}", e.getMessage());
		}
	};

	private final MemcacheMsgHandlerFactory msgHandlerFactory;

	private MemcacheMsgHandler currentMsgHandler;
	private final AuthMsgHandler authMsgHandler;
	private final Deque<Request> requestQueue;
	private final MemcacheStats memcacheStats;
	private ResponseSender failureResponse;
	Request currentRequest;
	private final int requestRateLimit;
	private final int queueSizeLimit;
	private volatile boolean scheduledRead = false;
	private long triggeredRequestLimitDuration = 0;
	private long triggeredRequestLimitStart = 0;
	private boolean triggeredRequestRateLimit = false;
	private long loggableRequestRate = 0;
	private boolean triggeredQueueLimit = false;
	private long loggableQueueSize = 0;
	ScheduledFuture<?> rateMonitoringScheduler;

	public MemcacheInboundHandlerAdapter(MemcacheMsgHandlerFactory msgHandlerFactory, AuthMsgHandler authMsgHandler,
			int queueSizeLimit, int requestRateLimit, MemcacheStats memcacheStats) {
		super();
		this.msgHandlerFactory = msgHandlerFactory;
		this.authMsgHandler = authMsgHandler;
		this.queueSizeLimit = queueSizeLimit;
		this.requestRateLimit = requestRateLimit;
		this.memcacheStats = memcacheStats;
		requestQueue = new ArrayDeque<>(queueSizeLimit + 100);
		rateMonitoringScheduler = msgHandlerFactory.getScheduledExecutorService().scheduleAtFixedRate(() -> {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Current load rate requests/10sec: rate={} user={}", loggableRequestRate,
						getCurrentUser());
				LOGGER.debug("Current request queue size: size={} user={}", loggableQueueSize, getCurrentUser());
			}
			if (triggeredRequestRateLimit) {
				LOGGER.warn("Requests are being rate limited at requests/10sec: rate={} pause={}ms user={}",
						loggableRequestRate, triggeredRequestLimitDuration, getCurrentUser());
				loggableRequestRate = 0;
				triggeredRequestRateLimit = false;
			}
			if (triggeredQueueLimit) {
				LOGGER.warn("Queue is being rate limited: size={} user={}", loggableQueueSize, getCurrentUser());
				triggeredQueueLimit = false;
				loggableQueueSize = 0;
			}
		}, 60, 60, TimeUnit.SECONDS);
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		ctx.channel().closeFuture().addListener(future -> {
			try {
				rateMonitoringScheduler.cancel(false);
			} catch (Exception t) {
				LOGGER.warn("Error cancelling rate monitoring scheduled task.", t);
			}
			clearMessageQueue();
		});
		readIfNotRateLimited(ctx);
		super.channelActive(ctx);
	}

	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
		readIfNotRateLimited(ctx);
		super.channelReadComplete(ctx);
	}

	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
		if (evt instanceof IdleStateEvent) {
			IdleStateEvent e = (IdleStateEvent) evt;
			if (e.state() == IdleState.READER_IDLE) {
				// try a read just to make sure the channel is still functioning.
				readIfNotRateLimited(ctx);
			} else if (e.state() == IdleState.WRITER_IDLE && !requestQueue.isEmpty() && System.currentTimeMillis()
					- requestQueue.peek().getCreated() > REQUEST_QUEUE_MAX_AGE.toMillis()) {
				throw new IllegalStateException(
						"Message at bottom of queue has been in the queue longer than: " + REQUEST_QUEUE_MAX_AGE);
			}
		}
		super.userEventTriggered(ctx, evt);
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		try {
			if (!(msg instanceof MemcacheObject)) {
				throw new IllegalArgumentException(
						"Got a message that was not a MemcacheObject. Was instead a " + msg.getClass());
			}
			MemcacheObject memcacheObject = (MemcacheObject) msg;

			if (isChannelReadyForNewRequest() && memcacheObject instanceof LastMemcacheContent) {
				// Currently in this implementation we just discard "LastMemcacheContent"
				// packets if not in a request context assuming the request handler knew if it
				// was done with the request before getting the LastContent packet.
				return;
			}
			// If this is not a new request packet request should be null
			BinaryMemcacheRequest request = setupRequest(memcacheObject);

			if (failureResponse != null) {
				// when we have a failureResponse we want to dump all messages until the last of
				// that request.
				if (msg instanceof LastMemcacheContent) {
					completeRequest(failureResponse.send(ctx));
				}
				return;
			}
			switch (currentRequest.getRequestKey().getOpcode()) {
			case BinaryMemcacheOpcodes.GET:
			case BinaryMemcacheOpcodes.GETQ:
				handleMessageHandlerResponse(currentMsgHandler.get(ctx, requestIsNotNull(request)));
				return;
			case BinaryMemcacheOpcodes.GETK:
			case BinaryMemcacheOpcodes.GETKQ:
				handleMessageHandlerResponse(currentMsgHandler.getK(ctx, requestIsNotNull(request)));
				return;
			case BinaryMemcacheOpcodes.SET:
			case BinaryMemcacheOpcodes.SETQ:
				if (request != null) {
					handleMessageHandlerResponse(currentMsgHandler.set(ctx, request));
				} else {
					handleMessageHandlerResponse(
							currentMsgHandler.set(ctx, castMemcacheObjectToMemcacheContent(memcacheObject)));
				}
				return;
			case BinaryMemcacheOpcodes.ADD:
			case BinaryMemcacheOpcodes.ADDQ:
				if (request != null) {
					handleMessageHandlerResponse(currentMsgHandler.add(ctx, request));
				} else {
					handleMessageHandlerResponse(
							currentMsgHandler.add(ctx, castMemcacheObjectToMemcacheContent(memcacheObject)));
				}
				return;
			case BinaryMemcacheOpcodes.REPLACE:
			case BinaryMemcacheOpcodes.REPLACEQ:
				if (request != null) {
					handleMessageHandlerResponse(currentMsgHandler.replace(ctx, request));
				} else {
					handleMessageHandlerResponse(
							currentMsgHandler.replace(ctx, castMemcacheObjectToMemcacheContent(memcacheObject)));
				}
				return;
			case BinaryMemcacheOpcodes.DELETE:
			case BinaryMemcacheOpcodes.DELETEQ:
				handleMessageHandlerResponse(currentMsgHandler.delete(ctx, requestIsNotNull(request)));
				return;
			case BinaryMemcacheOpcodes.INCREMENT:
			case BinaryMemcacheOpcodes.INCREMENTQ:
				handleMessageHandlerResponse(currentMsgHandler.increment(ctx, requestIsNotNull(request)));
				return;
			case BinaryMemcacheOpcodes.DECREMENT:
			case BinaryMemcacheOpcodes.DECREMENTQ:
				handleMessageHandlerResponse(currentMsgHandler.decrement(ctx, requestIsNotNull(request)));
				return;
			case BinaryMemcacheOpcodes.QUIT:
			case BinaryMemcacheOpcodes.QUITQ:
				handleMessageHandlerResponse(currentMsgHandler.quit(ctx, requestIsNotNull(request)));
				return;
			case BinaryMemcacheOpcodes.FLUSH:
			case BinaryMemcacheOpcodes.FLUSHQ:
				handleMessageHandlerResponse(currentMsgHandler.flush(ctx, requestIsNotNull(request)));
				return;
			case BinaryMemcacheOpcodes.NOOP:
				handleMessageHandlerResponse(currentMsgHandler.noop(ctx, requestIsNotNull(request)));
				return;
			case BinaryMemcacheOpcodes.VERSION:
				handleMessageHandlerResponse(currentMsgHandler.version(ctx, requestIsNotNull(request)));
				return;
			case BinaryMemcacheOpcodes.APPEND:
			case BinaryMemcacheOpcodes.APPENDQ:
				if (request != null) {
					handleMessageHandlerResponse(currentMsgHandler.append(ctx, request));
				} else {
					handleMessageHandlerResponse(
							currentMsgHandler.append(ctx, castMemcacheObjectToMemcacheContent(memcacheObject)));
				}
				return;
			case BinaryMemcacheOpcodes.PREPEND:
			case BinaryMemcacheOpcodes.PREPENDQ:
				if (request != null) {
					handleMessageHandlerResponse(currentMsgHandler.prepend(ctx, request));
				} else {
					handleMessageHandlerResponse(
							currentMsgHandler.prepend(ctx, castMemcacheObjectToMemcacheContent(memcacheObject)));
				}
				return;
			case BinaryMemcacheOpcodes.STAT:
				handleMessageHandlerResponse(currentMsgHandler.stat(ctx, requestIsNotNull(request)));
				return;
			case BinaryMemcacheOpcodes.TOUCH:
				handleMessageHandlerResponse(currentMsgHandler.touch(ctx, requestIsNotNull(request)));
				return;
			case BinaryMemcacheOpcodes.GAT:
			case BinaryMemcacheOpcodes.GATQ:
				handleMessageHandlerResponse(currentMsgHandler.gat(ctx, requestIsNotNull(request)));
				return;
			case BinaryMemcacheOpcodes.GATK:
			case BinaryMemcacheOpcodes.GATKQ:
				handleMessageHandlerResponse(currentMsgHandler.gat(ctx, requestIsNotNull(request)));
				return;
			case BinaryMemcacheOpcodes.SASL_LIST_MECHS:
				if (getAuthMsgHandler().isAuthenticated()) {
					throw new IllegalStateException("Attempting to authenticate in already authenticated channel.");
				}
				completeRequest(getAuthMsgHandler().listMechs(ctx, requestIsNotNull(request)));
				return;
			case BinaryMemcacheOpcodes.SASL_AUTH:
				if (getAuthMsgHandler().isAuthenticated()) {
					throw new IllegalStateException("Attempting to authenticate in already authenticated channel.");
				}
				if (request != null) {
					handleMessageHandlerResponse(getAuthMsgHandler().startAuth(ctx, request));
				} else {
					if (memcacheObject instanceof MemcacheContent) {
						completeRequest(getAuthMsgHandler().startAuth(ctx, (MemcacheContent) memcacheObject));
					} else {
						throw new IllegalStateException(
								"SASL auth requires MemcacheContent instance if not a BinaryMemcacheRequest.  class="
										+ memcacheObject.getClass());
					}
				}
				return;
			case BinaryMemcacheOpcodes.SASL_STEP:
				if (request != null) {
					completeRequest(MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.AUTH_ERROR,
							"We don't support any auth mechanisms that require a step.").send(ctx));
				} else {
					throw new IllegalStateException(
							"Received Non memcache request with SASL_STEP optcode.  This is an invalid state.");
				}
				return;
			default:
				if (memcacheObject instanceof BinaryMemcacheRequest) {
					LOGGER.info("Failed to handle request: optcode={}", currentRequest.getRequestKey().getOpcode());
					MemcacheUtils
							.returnFailure(request, BinaryMemcacheResponseStatus.UNKNOWN_COMMAND,
									"Unable to handle command: 0x"
											+ Integer.toHexString(currentRequest.getRequestKey().getOpcode()))
							.send(ctx).addListener(ChannelFutureListener.CLOSE);
				} else {
					throw new IllegalStateException(
							"Got a message we didn't know how to handle: " + memcacheObject.getClass());
				}
			}
		} finally {
			ReferenceCountUtil.release(msg);
		}
	}

	private BinaryMemcacheRequest setupRequest(MemcacheObject memcacheObject) {
		if (!(memcacheObject instanceof BinaryMemcacheRequest)) {
			if (!isChannelReadyForNewRequest()) {
				// Not a new request nothing to setup.
				return null;
			}
			throw new IllegalStateException("Channel not in a request context and didn't get a request object.");
		}
		if (!isChannelReadyForNewRequest()) {
			throw new IllegalStateException("Got a new request without having completed the current one.");
		}
		BinaryMemcacheRequest request = (BinaryMemcacheRequest) memcacheObject;
		currentRequest = new Request(new MemcacheRequestKey(request));
		requestQueue.offer(currentRequest);
		memcacheStats.logHit(currentRequest.getRequestKey().getOpcode(), getCurrentUser());
		if (Short.toUnsignedInt(request.keyLength()) > MAX_KEY_SIZE) {
			LOGGER.debug("Key too big.  Skipping this request.");
			failureResponse = MemcacheUtils.returnFailure(currentRequest.getRequestKey().getOpcode(), request.opaque(),
					BinaryMemcacheResponseStatus.E2BIG, "Key too big.  Max Key is " + MAX_KEY_SIZE);
			return null;
		}
		if (getAuthMsgHandler().isAuthenticated()) {
			currentMsgHandler = msgHandlerFactory.createMsgHandler(request, getAuthMsgHandler());
		} else {
			currentMsgHandler = new NoAuthMemcacheMsgHandler(request);
		}
		return request;
	}

	@Override
	public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
		if (!(msg instanceof BinaryMemcacheMessage)) {
			throw new IllegalStateException("We only support MemcacheMessages.");
		}
		BinaryMemcacheMessage memcacheMessage = (BinaryMemcacheMessage) msg;
		MemcacheRequestKey key = new MemcacheRequestKey(memcacheMessage);
		if (requestQueue.isEmpty()) {
			throw new IllegalStateException("We got a write event but the request queue was empty.");
		}
		queueMsg(key, memcacheMessage, promise);

		while (!requestQueue.isEmpty() && requestQueue.peek().complete()) {
			Request message = requestQueue.poll();
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Writing messages for: key={}", message.requestKey);
			}
			while (!message.responses.isEmpty()) {
				MessageResponse response = message.responses.poll();
				if (response.message instanceof QuietResponse) {
					continue;
				}
				ctx.write(response.message, response.promise);
			}
		}
		ctx.flush();
	}

	private void readIfNotRateLimited(ChannelHandlerContext ctx) {
		boolean requestLimitHit = false;
		long load = memcacheStats.requestsInWindow(getCurrentUser());

		if (load > loggableRequestRate) {
			loggableRequestRate = load;
		}

		if (load > requestRateLimit) {
			requestLimitHit = true;
			if (!scheduledRead) {
				scheduledRead = true;
				if (triggeredRequestLimitStart == 0) {
					triggeredRequestLimitStart = System.currentTimeMillis();
				}
				msgHandlerFactory.getScheduledExecutorService().schedule(() -> {
					scheduledRead = false;
					readIfNotRateLimited(ctx);
				}, memcacheStats.msLeftInWindow(getCurrentUser()), TimeUnit.MILLISECONDS);
			}
		}

		boolean queueLimitHit = false;
		int queueSize = requestQueue.size();
		if (queueSize > loggableQueueSize) {
			loggableQueueSize = queueSize;
		}
		if (queueSize >= queueSizeLimit) {
			queueLimitHit = true;
			triggeredQueueLimit = true;
		}

		if (!requestLimitHit && !queueLimitHit) {
			if (triggeredRequestLimitStart != 0) {
				triggeredRequestRateLimit = true;
				long duration = System.currentTimeMillis() - triggeredRequestLimitStart;
				if (duration > triggeredRequestLimitDuration) {
					triggeredRequestLimitDuration = duration;
				}
				triggeredRequestLimitStart = 0;
			}
			ctx.read();
		}
	}

	private String getCurrentUser() {
		if (getAuthMsgHandler() == null || getAuthMsgHandler().getUsername() == null) {
			return "NotYetAuthenticatedUser";
		}
		return getAuthMsgHandler().getUsername();
	}

	private AuthMsgHandler getAuthMsgHandler() {
		return authMsgHandler;
	}

	private MemcacheContent castMemcacheObjectToMemcacheContent(MemcacheObject memcacheObject) {
		if (memcacheObject instanceof MemcacheContent) {
			return (MemcacheContent) memcacheObject;
		}
		throw new IllegalStateException(
				"Expected message object to be of type MemcacheContent.  Instead it was: " + memcacheObject.getClass());
	}

	private BinaryMemcacheRequest requestIsNotNull(BinaryMemcacheRequest request) {
		if (request == null) {
			throw new IllegalStateException(
					"Trying to handle non request packet for opcode that takes only request packets. optcode="
							+ currentRequest.getRequestKey().getOpcode());
		}
		return request;
	}

	private void handleMessageHandlerResponse(Future<?> task) {
		// Task is null so assuming we just need to keep reading.
		if (task != null) {
			completeRequest(task);
		}
	}

	private boolean isChannelReadyForNewRequest() {
		if (currentMsgHandler == null && currentRequest == null && failureResponse == null) {
			return true;
		}
		return false;
	}

	private void completeRequest(Future<?> task) {
		currentRequest.setTask(task);
		currentMsgHandler = null;
		currentRequest = null;
		failureResponse = null;
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		if (ctx.channel().isOpen()) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Got error on closed channel", cause);
			}
			return;
		}
		try {
			if (isAnyCauseInstanceOf(cause, CancellationException.class)) {
				// Ignore this type of error.
			} else if (isAnyCauseInstanceOf(cause, MemcacheBackendStateException.class)) {
				ShutdownUtils.gracefullyExit(
						"MemcacheClusterStateException thrown. Closing this connection and shutting down the server because we don't know the state we're in.",
						(byte) 99, cause);
			} else if (cause != null && cause.getMessage() != null && cause.getMessage().contains("Connection reset")) {
				LOGGER.info("Connection unexpectedly reset.  Closing connection. user={}", getCurrentUser());
			} else if (isAnyCauseInstanceOf(cause, IllegalStateException.class)) {
				LOGGER.warn("Connection in unknown state. Closing connection. user={} msg={}", getCurrentUser(),
						cause.getMessage());
			} else {
				LOGGER.error("Unexpected Error. Closing connection. user={}", getCurrentUser(), cause);
			}
		} finally {
			ctx.channel().close().addListener(FAILURE_LOGGING_LISTENER);
		}
	}

	private boolean isAnyCauseInstanceOf(Throwable cause, Class<? extends Throwable> type) {
		if (cause == null) {
			return false;
		}
		if (type.isInstance(cause)) {
			return true;
		}
		return isAnyCauseInstanceOf(cause.getCause(), type);
	}

	private void queueMsg(MemcacheRequestKey key, BinaryMemcacheMessage memcacheMessage, ChannelPromise promise) {
		MessageResponse response = new MessageResponse();
		response.message = memcacheMessage;
		response.promise = promise;

		for (Request queuedMessage : requestQueue) {
			if (queuedMessage.matchesKey(key) && !queuedMessage.complete()) {
				queuedMessage.responses.offer(response);
				return;
			}
		}
		throw new IllegalStateException("Got response for message that is already complete. opcode=" + key.getOpcode());
	}

	private void clearMessageQueue() {
		for (Request request : requestQueue) {
			request.clear();
		}
	}

	private static class Request {

		final MemcacheRequestKey requestKey;
		final Deque<MessageResponse> responses = new ArrayDeque<>();
		Future<?> task = null;
		long created = System.currentTimeMillis();

		public Request(MemcacheRequestKey requestKey) {
			this.requestKey = requestKey;
		}

		public MemcacheRequestKey getRequestKey() {
			return requestKey;
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
			if (task != null) {
				try {
					task.cancel(true);
				} catch (Exception e) {
					LOGGER.debug("Error cancelling task key: {}", requestKey, e);
				}
			}
			while (!this.responses.isEmpty()) {
				MessageResponse response = this.responses.poll();
				try {
					ReferenceCountUtil.release(response.message);
				} catch (Exception e) {
					LOGGER.debug("Unexpected Error clearing message queue of request: {}", requestKey, e);
				}
				try {
					response.promise.cancel(true);
				} catch (Exception e) {
					LOGGER.debug("Unexpected Error cancelling promise for request: {}", requestKey, e);
				}
			}
		}

		private boolean complete() {
			if (responses.isEmpty()) {
				return false;
			}
			if (requestKey.getOpcode() == BinaryMemcacheOpcodes.STAT) {
				if (responses.peekLast().message instanceof FullMemcacheMessage) {
					FullBinaryMemcacheResponse lastMsgContent = (FullBinaryMemcacheResponse) responses
							.peekLast().message;
					if (lastMsgContent.keyLength() == 0 && lastMsgContent.totalBodyLength() == 0) {
						return true;
					}
				}
				return false;
			}
			return true;
		}
	}

	public static class MessageResponse {
		BinaryMemcacheMessage message;
		ChannelPromise promise;
	}

}
