package cloudfoundry.memcache;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
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
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemcacheInboundHandlerAdapter extends ChannelDuplexHandler {

	private static final Duration CONNECTION_STATISTICS_LOG_INTERVAL = Duration.ofMinutes(15);
	private static final Duration RATE_LIMIT_LOG_INTERVAL = Duration.ofMinutes(1);

	private static final Duration REQUEST_QUEUE_MAX_AGE = Duration.ofMinutes(1);

	private static final int MAX_KEY_SIZE = 250;

	private static final Logger LOGGER = LoggerFactory.getLogger(MemcacheInboundHandlerAdapter.class);

	public static final ChannelFutureListener FAILURE_LOGGING_LISTENER = future -> {
		LogUtils.setupChannelMdc(future.channel().id().asShortText());
		try {
			future.get();
		} catch (Exception e) {
			LOGGER.warn("Error closing Channel. {}", e.getMessage());
		} finally {
			LogUtils.cleanupChannelMdc();
		}
	};

	private final String channelId;
	private final MemcacheMsgHandlerFactory msgHandlerFactory;
	private final AuthMsgHandler authMsgHandler;
	private final Deque<Request> requestQueue;
	private final MemcacheStats memcacheStats;
	private final int requestRateLimit;
	private final int queueSizeLimit;
	private final ScheduledFuture<?> userLimitReporter;
	private final ScheduledFuture<?> connectionStatisticsReporter;
	private final UserRateLimitLogger rateLimitLogger = new UserRateLimitLogger();
	private final ConnectionStatisticsLogger connectionStatisticsLogger = new ConnectionStatisticsLogger();

	private MemcacheMsgHandler currentMsgHandler;
	private ResponseSender failureResponse;
	private Request currentRequest;
	private volatile boolean scheduledRead = false;
	private ChannelFutureListener authCompleteListener;

	public MemcacheInboundHandlerAdapter(String channelId, MemcacheMsgHandlerFactory msgHandlerFactory,
			AuthMsgHandler authMsgHandler, int queueSizeLimit, int requestRateLimit, MemcacheStats memcacheStats) {
		super();
		this.channelId = channelId;
		this.msgHandlerFactory = msgHandlerFactory;
		this.authMsgHandler = authMsgHandler;
		this.queueSizeLimit = queueSizeLimit;
		this.requestRateLimit = requestRateLimit;
		this.memcacheStats = memcacheStats;
		this.requestQueue = new ArrayDeque<>(queueSizeLimit + 100);
		this.userLimitReporter = msgHandlerFactory.getScheduledExecutorService().scheduleAtFixedRate(rateLimitLogger,
				RATE_LIMIT_LOG_INTERVAL.toMillis(), RATE_LIMIT_LOG_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
		this.connectionStatisticsReporter = msgHandlerFactory.getScheduledExecutorService().scheduleAtFixedRate(
				connectionStatisticsLogger, CONNECTION_STATISTICS_LOG_INTERVAL.toMillis(),
				CONNECTION_STATISTICS_LOG_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
		this.authCompleteListener = future -> {
			LogUtils.setupChannelMdc(channelId);
			try {
				if (getAuthMsgHandler() != null && getAuthMsgHandler().isAuthenticated()) {
					LOGGER.info("Connection Authenticated: user={}", getAuthMsgHandler().getUsername());
				} else {
					if (getAuthMsgHandler() != null) {
						LOGGER.warn("Connection Authentication FAILED: user={}", getAuthMsgHandler().getUsername());
					} else {
						LOGGER.warn("Connection Authentication FAILED");
					}
					future.channel().close().addListener(FAILURE_LOGGING_LISTENER);
				}
			} finally {
				LogUtils.cleanupChannelMdc();
			}
		};

	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		LogUtils.setupChannelMdc(channelId);
		try {
			LOGGER.info("Connection Created: remote={} local={}", ctx.channel().remoteAddress(),
					ctx.channel().localAddress());
			ctx.channel().closeFuture().addListener(future -> {
				LogUtils.setupChannelMdc(channelId);
				try {
					try {
						userLimitReporter.cancel(true);
					} catch (Exception t) {
						LOGGER.warn("Error cancelling limit reporter scheduled task.", t);
					}
					try {
						connectionStatisticsReporter.cancel(true);
					} catch (Exception t) {
						LOGGER.warn("Error cancelling statistics reporter scheduled task.", t);
					}
					clearMessageQueue();
					LOGGER.info("Connection Closed.");
				} finally {
					LogUtils.cleanupChannelMdc();
				}
			});
			readIfNotRateLimited(ctx);
			super.channelActive(ctx);
		} finally {
			LogUtils.cleanupChannelMdc();
		}
	}

	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
		LogUtils.setupChannelMdc(channelId);
		try {
			readIfNotRateLimited(ctx);
			super.channelReadComplete(ctx);
		} finally {
			LogUtils.cleanupChannelMdc();
		}
	}

	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
		LogUtils.setupChannelMdc(channelId);
		try {
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
		} finally {
			LogUtils.cleanupChannelMdc();
		}
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		LogUtils.setupChannelMdc(channelId);
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
					ChannelFuture response = getAuthMsgHandler().startAuth(ctx, request);
					if (response != null) {
						completeRequest(response.addListener(authCompleteListener));
					}
				} else {
					if (memcacheObject instanceof MemcacheContent) {
						completeRequest(getAuthMsgHandler().startAuth(ctx, (MemcacheContent) memcacheObject)
								.addListener(authCompleteListener));
					} else {
						throw new IllegalStateException(
								"SASL auth requires MemcacheContent instance if not a BinaryMemcacheRequest.  class="
										+ memcacheObject.getClass());
					}
				}
				return;
			case BinaryMemcacheOpcodes.SASL_STEP:
				if (request != null) {
					completeRequest(MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.AUTH_ERROR, true,
							"We don't support any auth mechanisms that require a step.").send(ctx));
				} else {
					throw new IllegalStateException(
							"Received Non memcache request with SASL_STEP optcode.  This is an invalid state.");
				}
				return;
			default:
				if (memcacheObject instanceof BinaryMemcacheRequest) {
					LOGGER.info("Failed to handle request: optcode={}", currentRequest.getRequestKey().getOpcode());
					completeRequest(MemcacheUtils
							.returnFailure(request, BinaryMemcacheResponseStatus.UNKNOWN_COMMAND, true,
									"Unable to handle command: 0x"
											+ Integer.toHexString(currentRequest.getRequestKey().getOpcode()))
							.send(ctx));
				} else {
					throw new IllegalStateException(
							"Got a message we didn't know how to handle: " + memcacheObject.getClass());
				}
			}
		} finally {
			try {
				ReferenceCountUtil.release(msg);
			} finally {
				LogUtils.cleanupChannelMdc();
			}
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
		connectionStatisticsLogger.logRequest();
		if (Short.toUnsignedInt(request.keyLength()) > MAX_KEY_SIZE) {
			LOGGER.debug("Key too big.  Skipping this request.");
			failureResponse = MemcacheUtils.returnFailure(currentRequest.getRequestKey().getOpcode(), request.opaque(),
					BinaryMemcacheResponseStatus.E2BIG, false, "Key too big.  Max Key is " + MAX_KEY_SIZE);
			return null;
		}
		if (getAuthMsgHandler().isAuthenticated()) {
			currentMsgHandler = msgHandlerFactory.createMsgHandler(request, getAuthMsgHandler(), channelId);
		} else {
			currentMsgHandler = new NoAuthMemcacheMsgHandler(request);
		}
		return request;
	}

	@Override
	public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
		LogUtils.setupChannelMdc(channelId);
		try {
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
				while (!message.getResponses().isEmpty()) {
					MessageResponse response = message.getResponses().poll();
					if (response.message instanceof QuietResponse) {
						continue;
					}
					ctx.write(response.message, response.promise);
				}
			}
			ctx.flush();
		} finally {
			LogUtils.cleanupChannelMdc();
		}
	}

	private void readIfNotRateLimited(ChannelHandlerContext ctx) {
		long load = memcacheStats.requestsInWindow(getCurrentUser());
		long timeLeftInWindow = memcacheStats.msLeftInWindow(getCurrentUser());

		boolean requestLimitHit = false;
		if (load > requestRateLimit) {
			requestLimitHit = true;
			rateLimitLogger.requestRateLimitTriggered(load, timeLeftInWindow);
			if (!scheduledRead) {
				scheduledRead = true;
				msgHandlerFactory.getScheduledExecutorService().schedule(() -> {
					LogUtils.setupChannelMdc(channelId);
					try {
						scheduledRead = false;
						readIfNotRateLimited(ctx);
					} finally {
						LogUtils.cleanupChannelMdc();
					}
				}, timeLeftInWindow, TimeUnit.MILLISECONDS);
			}
		}

		boolean queueLimitHit = false;
		int queueSize = requestQueue.size();
		if (queueSize >= queueSizeLimit) {
			queueLimitHit = true;
			rateLimitLogger.queueRateLimitTriggered(queueSize);
		}

		if (!requestLimitHit && !queueLimitHit) {
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
		return currentMsgHandler == null && currentRequest == null && failureResponse == null;
	}

	private void completeRequest(Future<?> task) {
		currentRequest.setTask(task);
		currentMsgHandler = null;
		currentRequest = null;
		failureResponse = null;
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		LogUtils.setupChannelMdc(channelId);
		try {
			if (!ctx.channel().isOpen()) {
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
				} else if (cause != null && cause.getMessage() != null
						&& cause.getMessage().contains("Connection reset")) {
					LOGGER.warn("Connection unexpectedly reset.");
				} else if (isAnyCauseInstanceOf(cause, IllegalStateException.class)) {
					LOGGER.warn("Connection in unknown state. msg={}", cause.getMessage());
				} else {
					LOGGER.error("Unexpected Error.", cause);
				}
			} finally {
				ctx.channel().close().addListener(FAILURE_LOGGING_LISTENER);
			}
		} finally {
			LogUtils.cleanupChannelMdc();
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
				queuedMessage.offerResponse(response);
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

	private class ConnectionStatisticsLogger implements Runnable {
		private long requestsLastReport = 0;
		private LongAdder requests = new LongAdder();

		@Override
		public void run() {
			LogUtils.setupChannelMdc(channelId);
			try {
				long total = requests.sum();
				LOGGER.info("Statistics: user={} requestDelta={} queueSize={}", getCurrentUser(),
						total - requestsLastReport, requestQueue.size());
				requestsLastReport = total;
			} finally {
				LogUtils.cleanupChannelMdc();
			}
		}

		public void logRequest() {
			requests.increment();
		}
	}

	private class UserRateLimitLogger implements Runnable {
		private long triggeredRequestLimitDuration = 0;
		private boolean triggeredRequestRateLimit = false;
		private long triggeredRequestLimitRate = 0;
		private boolean triggeredQueueLimit = false;
		private long triggeredQueueSize = 0;

		@Override
		public void run() {
			if (triggeredRequestRateLimit) {
				LOGGER.warn("Requests are being rate limited at requests/10sec: rate={} pause={}ms user={}",
						triggeredRequestLimitRate, triggeredRequestLimitDuration, getCurrentUser());
				triggeredRequestLimitRate = 0;
				triggeredRequestLimitDuration = 0;
				triggeredRequestRateLimit = false;
			}
			if (triggeredQueueLimit) {
				LOGGER.warn("Queue is being rate limited: size={} user={}", triggeredQueueSize, getCurrentUser());
				triggeredQueueLimit = false;
				triggeredQueueSize = 0;
			}
		}

		public void requestRateLimitTriggered(long triggeredLimit, long triggeredDuration) {
			if (!triggeredRequestRateLimit) {
				triggeredRequestRateLimit = true;
				this.triggeredRequestLimitDuration = triggeredDuration;
				this.triggeredRequestLimitRate = triggeredLimit;
			}
		}

		public void queueRateLimitTriggered(long queueSize) {
			if (!triggeredQueueLimit) {
				this.triggeredQueueLimit = true;
				this.triggeredQueueSize = queueSize;
			}
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

		public void offerResponse(MessageResponse response) {
			responses.offer(response);
		}

		public Deque<MessageResponse> getResponses() {
			return responses;
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
