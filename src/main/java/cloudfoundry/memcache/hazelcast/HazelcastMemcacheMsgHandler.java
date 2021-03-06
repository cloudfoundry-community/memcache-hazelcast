package cloudfoundry.memcache.hazelcast;

import cloudfoundry.memcache.AuthMsgHandler;
import cloudfoundry.memcache.CompletedFuture;
import cloudfoundry.memcache.LogUtils;
import cloudfoundry.memcache.MemcacheMsgHandler;
import cloudfoundry.memcache.MemcacheUtils;
import cloudfoundry.memcache.ResponseSender;
import cloudfoundry.memcache.ShutdownUtils;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.HazelcastOverloadException;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IMap;
import com.hazelcast.monitor.LocalMapStats;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.memcache.LastMemcacheContent;
import io.netty.handler.codec.memcache.MemcacheContent;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheOpcodes;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheRequest;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheResponseStatus;
import io.netty.handler.codec.memcache.binary.DefaultFullBinaryMemcacheResponse;
import io.netty.handler.codec.memcache.binary.FullBinaryMemcacheResponse;
import io.netty.util.concurrent.GenericFutureListener;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HazelcastMemcacheMsgHandler implements MemcacheMsgHandler {
	private static final Executor FAKE_EXECUTOR = Runnable::run;

	private static final Logger LOGGER = LoggerFactory.getLogger(HazelcastMemcacheMsgHandler.class);

	public static final GenericFutureListener<io.netty.util.concurrent.Future<Void>> FAILURE_LOGGING_LISTENER = future -> {
		if (LOGGER.isWarnEnabled()) {
			try {
				future.get();
			} catch (Exception e) {
				LOGGER.warn("Error closing Channel. {}", e.getMessage());
			}
		}
	};

	final HazelcastInstance instance;
	HazelcastMemcacheCacheValue cacheValue;
	final AuthMsgHandler authMsgHandler;
	ResponseSender failureResponse;
	public static final int MAX_EXPIRATION_SEC = 60 * 60 * 24 * 30;
	public static final long MAX_EXPIRATION = 0xFFFFFFFF;
	final byte opcode;
	final int opaque;
	final byte[] key;
	final long cas;
	final int maxValueSize;
	final String channelId;
	long expirationInSeconds;

	public HazelcastMemcacheMsgHandler(BinaryMemcacheRequest request, AuthMsgHandler authMsgHandler,
			HazelcastInstance instance, Integer maxValueSize, String channelId) {
		this.authMsgHandler = authMsgHandler;
		this.instance = instance;
		this.opcode = request.opcode();
		this.opaque = request.opaque();
		this.key = request.key() == null ? null : Unpooled.copiedBuffer(request.key()).array();
		this.cas = request.cas();
		this.maxValueSize = maxValueSize;
		this.channelId = channelId;
	}

	@Override
	public byte getOpcode() {
		return opcode;
	}

	@Override
	public int getOpaque() {
		return opaque;
	}

	public IMap<byte[], HazelcastMemcacheCacheValue> getCache() {
		return instance.getMap(authMsgHandler.getCacheName());
	}

	public IExecutorService getExecutor() {
		return instance.getExecutorService(HazelcastMemcacheMsgHandlerFactory.EXECUTOR_INSTANCE_NAME);
	}

	public HazelcastInstance getInstance() {
		return instance;
	}

	private Future<?> handleException(ChannelHandlerContext ctx, Throwable t) {
		if (t instanceof CancellationException) {
			return CompletedFuture.INSTANCE;
		}
		if (t instanceof HazelcastInstanceNotActiveException) {
			ShutdownUtils.gracefullyExit("Hazelcast instance not active.", (byte) 1, t);
		}
		if (t instanceof HazelcastOverloadException) {
			LOGGER.error("Failed to invoke operation. System is currently overloaded.  Sending Failure and closing connection.");
			return MemcacheUtils.returnFailure(getOpcode(), getOpaque(), MemcacheUtils.INTERNAL_ERROR, true, t.getMessage()).send(ctx);
		}
		if (t instanceof HazelcastException) {
			LOGGER.error("Hazelcast Failed to handle the request.  Sending Failure response and closing connection.");
			return MemcacheUtils.returnFailure(getOpcode(), getOpaque(), MemcacheUtils.INTERNAL_ERROR, true, t.getMessage()).send(ctx);
		}
		if (t instanceof IllegalStateException) {
			LOGGER.error("Connection is in an invalid state.  Closing Connection: opcode={} msg={}", opcode, t.getMessage());
			return MemcacheUtils.returnFailure(getOpcode(), getOpaque(), MemcacheUtils.INTERNAL_ERROR, true, t.getMessage()).send(ctx);
		}
		LOGGER.error("Error executing operations.  Closing Connection: opcode={}", opcode, t);
		return MemcacheUtils.returnFailure(getOpcode(), getOpaque(), MemcacheUtils.INTERNAL_ERROR, true, t.getMessage()).send(ctx);
	}

	@Override
	public Future<?> get(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return handleGetRequest(ctx, request, BinaryMemcacheOpcodes.GET, false);
	}

	private Future<?> handleGetRequest(ChannelHandlerContext ctx, BinaryMemcacheRequest request, byte nonQuietOpcode,
			boolean includeKey) {
		try {
			MemcacheUtils.logRequest(request);

			IMap<byte[], HazelcastMemcacheCacheValue> map = getCache();

			ICompletableFuture<HazelcastMemcacheCacheValue> future = map.getAsync(key);
			ExecutionCallback<HazelcastMemcacheCacheValue> callback = new ExecutionCallback<HazelcastMemcacheCacheValue>() {
				@Override
				public void onResponse(HazelcastMemcacheCacheValue value) {
					LogUtils.setupChannelMdc(channelId);
					try {
						if (value == null && opcode == nonQuietOpcode) {
							MemcacheUtils.returnFailure(opcode, opaque, BinaryMemcacheResponseStatus.KEY_ENOENT, false,
									"Unable to find Key: " + key).send(ctx);
							return;
						} else if (value == null) {
							MemcacheUtils.returnQuiet(opcode, opaque).send(ctx);
							return;
						}

						ByteBuf responseValue = value.getValue();
						ByteBuf responseFlags = value.getFlags();

						if (value.getFlagLength() == 0 && value.getTotalFlagsAndValueLength() == 8) {
							long incDecValue = value.getValue().readLong();
							responseValue = Unpooled
									.wrappedBuffer(Long.toUnsignedString(incDecValue).getBytes(StandardCharsets.UTF_8));
							responseFlags = Unpooled.wrappedBuffer(new byte[4]);
						}

						FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse(null, responseFlags,
								responseValue);
						response.setStatus(BinaryMemcacheResponseStatus.SUCCESS);
						response.setOpcode(opcode);
						response.setCas(value.getCAS());
						response.setOpaque(opaque);
						if (includeKey) {
							ByteBuf responseKey = Unpooled.wrappedBuffer(key);
							response.setKey(responseKey);
							response.setTotalBodyLength(
									responseFlags.capacity() + responseValue.capacity() + key.length);
						} else {
							response.setTotalBodyLength(responseFlags.capacity() + responseValue.capacity());
						}
						MemcacheUtils.writeAndFlush(ctx, response);
						if (LOGGER.isDebugEnabled()) {
							LOGGER.debug("Current Socket: {} Flushed response for key: {}", ctx.channel().id(), key);
						}
					} catch (Exception e) {
						handleException(ctx, e);
					} finally {
						LogUtils.cleanupChannelMdc();
					}

				}

				@Override
				public void onFailure(Throwable t) {
					LogUtils.setupChannelMdc(channelId);
					try {
						handleException(ctx, t);
					} finally {
						LogUtils.cleanupChannelMdc();
					}
				}
			};
			executeOrAddCallback(future, callback);
			return future;
		} catch (Exception e) {
			return handleException(ctx, e);
		}
	}

	private <T> void executeOrAddCallback(ICompletableFuture<T> future, ExecutionCallback<T> callback) {
		if (future.isDone()) {
			future.andThen(callback, FAKE_EXECUTOR);
		} else {
			future.andThen(callback);
		}
	}

	@Override
	public Future<?> getK(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return handleGetRequest(ctx, request, BinaryMemcacheOpcodes.GETK, true);
	}

	@Override
	public Future<?> set(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return processSetAddReplaceRequest(ctx, request);
	}

	private Future<?> processSetAddReplaceRequest(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		try {
			MemcacheUtils.logRequest(request);

			int valueSize = request.totalBodyLength() - Short.toUnsignedInt(request.keyLength())
					- Byte.toUnsignedInt(request.extrasLength());
			if (valueSize > maxValueSize) {
				failureResponse = MemcacheUtils.returnFailure(opcode, opaque, BinaryMemcacheResponseStatus.E2BIG, false,
						"Value too big.  Max Value is " + maxValueSize);
			}
			ByteBuf extras = request.extras();
			ByteBuf flagSlice = extras.slice(0, 4);
			long expiration = extras.getUnsignedInt(4);
			cacheValue = new HazelcastMemcacheCacheValue(valueSize, flagSlice, expiration);
			long currentTimeInSeconds = System.currentTimeMillis() / 1000;
			if (expiration <= MAX_EXPIRATION_SEC) {
				expirationInSeconds = expiration;
			} else if (expiration > currentTimeInSeconds) {
				expirationInSeconds = expiration - currentTimeInSeconds;
			} else {
				failureResponse = MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.NOT_STORED, false,
						"Expiration is in the past.");
			}
			return null;
		} catch (Exception e) {
			return handleException(ctx, e);
		}
	}

	@Override
	public Future<?> set(ChannelHandlerContext ctx, MemcacheContent content) {
		return processSetAddReplaceContent(ctx, content, BinaryMemcacheOpcodes.SET);
	}

	private Future<?> processSetAddReplaceContent(ChannelHandlerContext ctx, MemcacheContent content,
			byte nonQuietOpcode) {
		try {
			if (failureResponse == null) {
				cacheValue.writeValue(content.content());
			}
			if (content instanceof LastMemcacheContent) {
				if (failureResponse != null) {
					cacheValue = null;
					return failureResponse.send(ctx);
				}
				IExecutorService executor = getExecutor();

				ICompletableFuture<HazelcastMemcacheMessage> future = (ICompletableFuture<HazelcastMemcacheMessage>) executor
						.submitToKeyOwner(new HazelcastSetCallable(authMsgHandler.getCacheName(), nonQuietOpcode, key,
								cas, cacheValue, expirationInSeconds), key);
				ExecutionCallback<HazelcastMemcacheMessage> callback = new ExecutionCallback<HazelcastMemcacheMessage>() {
					@Override
					public void onResponse(HazelcastMemcacheMessage response) {
						LogUtils.setupChannelMdc(channelId);
						try {
							if (response.isSuccess()) {
								if (opcode == nonQuietOpcode) {
									MemcacheUtils.returnSuccess(opcode, opaque, response.getCas(), null).send(ctx);
								} else {
									MemcacheUtils.returnQuiet(opcode, opaque).send(ctx);
								}
							} else {
								MemcacheUtils.returnFailure(opcode, opaque, response.getCode(), false, response.getMsg())
										.send(ctx);
							}
						} catch (Exception e) {
							handleException(ctx, e);
						} finally {
							LogUtils.cleanupChannelMdc();
						}
					}

					public void onFailure(Throwable t) {
						LogUtils.setupChannelMdc(channelId);
						try {
							handleException(ctx, t);
						} finally {
							LogUtils.cleanupChannelMdc();
						}
					}
				};
				// Null out so it can get GCed No need to keep it now.
				cacheValue = null;
				executeOrAddCallback(future, callback);
				return future;
			}
			return null;
		} catch (Exception e) {
			return handleException(ctx, e);
		}
	}

	@Override
	public Future<?> add(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return processSetAddReplaceRequest(ctx, request);
	}

	@Override
	public Future<?> add(ChannelHandlerContext ctx, MemcacheContent content) {
		return processSetAddReplaceContent(ctx, content, BinaryMemcacheOpcodes.ADD);
	}

	@Override
	public Future<?> replace(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return processSetAddReplaceRequest(ctx, request);
	}

	@Override
	public Future<?> replace(ChannelHandlerContext ctx, MemcacheContent content) {
		return processSetAddReplaceContent(ctx, content, BinaryMemcacheOpcodes.REPLACE);
	}

	@Override
	public Future<?> delete(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		try {
			MemcacheUtils.logRequest(request);
			IExecutorService executor = getExecutor();

			ICompletableFuture<Boolean> future = (ICompletableFuture<Boolean>) executor
					.submitToKeyOwner(new HazelcastDeleteCallable(authMsgHandler.getCacheName(), key), key);

			ExecutionCallback<Boolean> callback = new ExecutionCallback<Boolean>() {
				@Override
				public void onResponse(Boolean response) {
					LogUtils.setupChannelMdc(channelId);
					try {
						if (response.booleanValue() && opcode == BinaryMemcacheOpcodes.DELETE) {
							MemcacheUtils.returnSuccess(request.opcode(), request.opaque(), 0, null).send(ctx);
						} else if (!response.booleanValue()) {
							MemcacheUtils
									.returnFailure(request, BinaryMemcacheResponseStatus.KEY_ENOENT, false, "Entry not found.")
									.send(ctx);
						} else {
							MemcacheUtils.returnQuiet(request.opcode(), request.opaque()).send(ctx);
						}
					} catch (Exception e) {
						handleException(ctx, e);
					} finally {
						LogUtils.cleanupChannelMdc();
					}
				}

				public void onFailure(Throwable t) {
					LogUtils.setupChannelMdc(channelId);
					try {
						handleException(ctx, t);
					} finally {
						LogUtils.cleanupChannelMdc();
					}
				}
			};

			executeOrAddCallback(future, callback);

			return future;
		} catch (Exception e) {
			return handleException(ctx, e);
		}
	}

	@Override
	public Future<?> increment(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return handleIncrementAndDecrement(ctx, request, true);
	}

	private Future<?> handleIncrementAndDecrement(ChannelHandlerContext ctx, BinaryMemcacheRequest request,
			boolean increment) {
		try {
			MemcacheUtils.logRequest(request);
			ByteBuf extras = request.extras();
			long delta = extras.getLong(0);
			long expiration = extras.getUnsignedInt(16);
			long currentTimeInSeconds = System.currentTimeMillis() / 1000;
			long initialValue = extras.slice(8, 8).readLong();
			if (expiration == MAX_EXPIRATION) {
				return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.KEY_ENOENT, false,
						"Expiration is MAX Value.  Not allowed according to spec for some reason.").send(ctx);
			}
			if (expiration <= MAX_EXPIRATION_SEC) {
				expirationInSeconds = expiration;
			} else if (expiration > currentTimeInSeconds) {
				expirationInSeconds = expiration - currentTimeInSeconds;
			} else {
				return MemcacheUtils
						.returnFailure(request, BinaryMemcacheResponseStatus.NOT_STORED, false, "Expiration is in the past.")
						.send(ctx);
			}
			IExecutorService executor = getExecutor();

			ICompletableFuture<HazelcastMemcacheMessage> future = (ICompletableFuture<HazelcastMemcacheMessage>) executor
					.submitToKeyOwner(new HazelcastIncDecCallable(authMsgHandler.getCacheName(), key,
							expirationInSeconds, increment, delta, expiration, initialValue), key);

			ExecutionCallback<HazelcastMemcacheMessage> callback = new ExecutionCallback<HazelcastMemcacheMessage>() {
				@Override
				public void onResponse(HazelcastMemcacheMessage msg) {
					LogUtils.setupChannelMdc(channelId);
					try {
						if (!msg.isSuccess()) {
							MemcacheUtils.returnFailure(opcode, opaque, msg.getCode(), false, msg.getMsg()).send(ctx);
							return;
						}
						if (opcode == BinaryMemcacheOpcodes.INCREMENT || opcode == BinaryMemcacheOpcodes.DECREMENT) {
							FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse(null, null,
									msg.getValue().getValue());
							response.setStatus(BinaryMemcacheResponseStatus.SUCCESS);
							response.setOpcode(opcode);
							response.setCas(msg.getValue().getCAS());
							response.setOpaque(opaque);
							response.setTotalBodyLength(msg.getValue().getTotalFlagsAndValueLength());
							MemcacheUtils.writeAndFlush(ctx, response);
						} else {
							MemcacheUtils.returnQuiet(request.opcode(), request.opaque()).send(ctx);
						}
					} catch (Exception e) {
						handleException(ctx, e);
					} finally {
						LogUtils.cleanupChannelMdc();
					}
				}

				public void onFailure(Throwable t) {
					LogUtils.setupChannelMdc(channelId);
					try {
						handleException(ctx, t);
					} finally {
						LogUtils.cleanupChannelMdc();
					}
				}
			};
			executeOrAddCallback(future, callback);
			return future;
		} catch (Exception e) {
			return handleException(ctx, e);
		}
	}

	@Override
	public Future<?> decrement(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return handleIncrementAndDecrement(ctx, request, false);
	}

	@Override
	public Future<?> quit(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		try {
			MemcacheUtils.logRequest(request);
			if (request.opcode() == BinaryMemcacheOpcodes.QUIT) {
				return MemcacheUtils.returnSuccess(request.opcode(), request.opaque(), 0, null).send(ctx)
						.addListener(ChannelFutureListener.CLOSE);
			}
			return ctx.channel().close().addListener(FAILURE_LOGGING_LISTENER);
		} catch (Exception e) {
			return handleException(ctx, e);
		}
	}

	@Override
	public Future<?> flush(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		try {
			MemcacheUtils.logRequest(request);
			getCache().evictAll();
			if (request.opcode() == BinaryMemcacheOpcodes.FLUSH) {
				return MemcacheUtils.returnSuccess(request.opcode(), request.opaque(), 0, null).send(ctx);
			}
			return MemcacheUtils.returnQuiet(request.opcode(), request.opaque()).send(ctx);
		} catch (Exception e) {
			return handleException(ctx, e);
		}
	}

	@Override
	public Future<?> noop(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		try {
			MemcacheUtils.logRequest(request);
			return MemcacheUtils.returnSuccess(request.opcode(), request.opaque(), 0, null).send(ctx);
		} catch (Exception e) {
			return handleException(ctx, e);
		}
	}

	@Override
	public Future<?> version(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		try {
			MemcacheUtils.logRequest(request);
			return MemcacheUtils.returnSuccess(request.opcode(), request.opaque(), 0, "CF Memcache 2.0").send(ctx);
		} catch (Exception e) {
			return handleException(ctx, e);
		}
	}

	private Future<?> appendPrepend(BinaryMemcacheRequest request) {
		MemcacheUtils.logRequest(request);

		int valueSize = request.totalBodyLength() - Short.toUnsignedInt(request.keyLength())
				- Byte.toUnsignedInt(request.extrasLength());

		if (valueSize > maxValueSize) {
			failureResponse = MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.E2BIG, false,
					"Value too big.  Max Value is " + maxValueSize);
		}
		cacheValue = new HazelcastMemcacheCacheValue(valueSize, Unpooled.EMPTY_BUFFER, 0);
		return null;
	}

	private Future<?> appendPrepend(ChannelHandlerContext ctx, MemcacheContent content, boolean append) {
		try {
			if (failureResponse == null) {
				cacheValue.writeValue(content.content());
			}
			if (content instanceof LastMemcacheContent) {
				if (failureResponse != null) {
					cacheValue = null;
					return failureResponse.send(ctx);
				}
				IExecutorService executor = getExecutor();

				ICompletableFuture<HazelcastMemcacheMessage> future = (ICompletableFuture<HazelcastMemcacheMessage>) executor
						.submitToKeyOwner(new HazelcastAppendPrependCallable(authMsgHandler.getCacheName(), key,
								cacheValue, append, maxValueSize), key);

				ExecutionCallback<HazelcastMemcacheMessage> callback = new ExecutionCallback<HazelcastMemcacheMessage>() {
					@Override
					public void onResponse(HazelcastMemcacheMessage msg) {
						LogUtils.setupChannelMdc(channelId);
						try {
							if (!msg.isSuccess()) {
								MemcacheUtils.returnFailure(opcode, opaque, msg.getCode(), false, msg.getMsg()).send(ctx);
							}
							if (opcode == BinaryMemcacheOpcodes.APPEND || opcode == BinaryMemcacheOpcodes.PREPEND) {
								MemcacheUtils.returnSuccess(opcode, opaque, msg.getCas(), null).send(ctx);
							} else {
								MemcacheUtils.returnQuiet(opcode, opaque).send(ctx);
							}
						} catch (Exception e) {
							handleException(ctx, e);
						} finally {
							LogUtils.cleanupChannelMdc();
						}
					}

					public void onFailure(Throwable t) {
						LogUtils.setupChannelMdc(channelId);
						try {
							handleException(ctx, t);
						} finally {
							LogUtils.cleanupChannelMdc();
						}
					}
				};
				// null out so it can get GCed while waiting for a response.
				cacheValue = null;
				executeOrAddCallback(future, callback);
				return future;
			}
			return null;
		} catch (Exception e) {
			return handleException(ctx, e);
		}
	}

	@Override
	public Future<?> append(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return appendPrepend(request);
	}

	@Override
	public Future<?> append(ChannelHandlerContext ctx, MemcacheContent content) {
		return appendPrepend(ctx, content, true);
	}

	@Override
	public Future<?> prepend(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return appendPrepend(request);
	}

	@Override
	public Future<?> prepend(ChannelHandlerContext ctx, MemcacheContent content) {
		return appendPrepend(ctx, content, false);
	}

	private static final Map<byte[], Stat> STATS;

	static {
		STATS = new LinkedHashMap<>();
		STATS.put("pid".getBytes(), handler -> ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
		STATS.put("uptime".getBytes(), handler -> {
			long startTime = (Long) handler.getInstance().getReplicatedMap(Stat.STAT_MAP).get(Stat.UPTIME_KEY);
			return String.valueOf((System.currentTimeMillis() - startTime) / 1000);
		});
		STATS.put("time".getBytes(), handler -> String.valueOf(System.currentTimeMillis() / 1000));
		STATS.put("version".getBytes(), handler -> "1.0");
		STATS.put("bytes".getBytes(), handler -> String.valueOf(handler.getCache().getLocalMapStats().getHeapCost()));
		STATS.put("limit_maxbytes".getBytes(), handler -> {
			MapConfig mapConfig = handler.getInstance().getConfig().findMapConfig(handler.getCache().getName());
			if (mapConfig == null) {
				return null;
			}
			return String.valueOf(mapConfig.getMaxSizeConfig().getSize());
		});
		STATS.put("get_hits".getBytes(), handler -> {
			LocalMapStats mapStats = handler.getCache().getLocalMapStats();
			return String.valueOf(mapStats.getHits());
		});
		STATS.put("get_misses".getBytes(), handler -> {
			LocalMapStats mapStats = handler.getCache().getLocalMapStats();
			return String.valueOf(mapStats.getGetOperationCount() - mapStats.getHits());
		});
		STATS.put("backup_count".getBytes(), handler -> {
			LocalMapStats mapStats = handler.getCache().getLocalMapStats();
			return String.valueOf(mapStats.getBackupCount());
		});
		STATS.put("size".getBytes(), handler -> String.valueOf(handler.getCache().size()));
		STATS.put("owned_entry_count".getBytes(), handler -> {
			LocalMapStats mapStats = handler.getCache().getLocalMapStats();
			return String.valueOf(mapStats.getOwnedEntryCount());
		});
		STATS.put("owned_entry_bytes".getBytes(), handler -> {
			LocalMapStats mapStats = handler.getCache().getLocalMapStats();
			return String.valueOf(mapStats.getOwnedEntryMemoryCost());
		});
		STATS.put("backup_entry_count".getBytes(), handler -> {
			LocalMapStats mapStats = handler.getCache().getLocalMapStats();
			return String.valueOf(mapStats.getBackupEntryCount());
		});
		STATS.put("backup_entry_bytes".getBytes(), handler -> {
			LocalMapStats mapStats = handler.getCache().getLocalMapStats();
			return String.valueOf(mapStats.getBackupEntryMemoryCost());
		});
		STATS.put("get_operation_count".getBytes(), handler -> {
			LocalMapStats mapStats = handler.getCache().getLocalMapStats();
			return String.valueOf(mapStats.getGetOperationCount());
		});
		STATS.put("put_operation_count".getBytes(), handler -> {
			LocalMapStats mapStats = handler.getCache().getLocalMapStats();
			return String.valueOf(mapStats.getPutOperationCount());
		});
		STATS.put("remove_operation_count".getBytes(), handler -> {
			LocalMapStats mapStats = handler.getCache().getLocalMapStats();
			return String.valueOf(mapStats.getRemoveOperationCount());
		});
		STATS.put("event_operation_count".getBytes(), handler -> {
			LocalMapStats mapStats = handler.getCache().getLocalMapStats();
			return String.valueOf(mapStats.getEventOperationCount());
		});
		STATS.put("other_operation_count".getBytes(), handler -> {
			LocalMapStats mapStats = handler.getCache().getLocalMapStats();
			return String.valueOf(mapStats.getOtherOperationCount());
		});
		STATS.put("total_operation_count".getBytes(), handler -> {
			LocalMapStats mapStats = handler.getCache().getLocalMapStats();
			return String.valueOf(mapStats.total());
		});
		STATS.put("max_get_latency".getBytes(), handler -> {
			LocalMapStats mapStats = handler.getCache().getLocalMapStats();
			return String.valueOf(mapStats.getMaxGetLatency());
		});
		STATS.put("total_get_latency".getBytes(), handler -> {
			LocalMapStats mapStats = handler.getCache().getLocalMapStats();
			return String.valueOf(mapStats.getTotalGetLatency());
		});
		STATS.put("total_put_latency".getBytes(), handler -> {
			LocalMapStats mapStats = handler.getCache().getLocalMapStats();
			return String.valueOf(mapStats.getTotalPutLatency());
		});
		STATS.put("total_remove_latency".getBytes(), handler -> {
			LocalMapStats mapStats = handler.getCache().getLocalMapStats();
			return String.valueOf(mapStats.getTotalRemoveLatency());
		});
	}

	@Override
	public Future<?> stat(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		try {
			MemcacheUtils.logRequest(request);
			if (key != null) {
				sendStat(ctx, opaque, key, STATS.get(key).getStat(this));
			} else {
				for (Map.Entry<byte[], Stat> stat : STATS.entrySet()) {
					sendStat(ctx, opaque, stat.getKey(), stat.getValue().getStat(this));
				}
			}
			return MemcacheUtils.returnSuccess(request.opcode(), request.opaque(), 0, null).send(ctx);
		} catch (Exception e) {
			return handleException(ctx, e);
		}
	}

	private void sendStat(ChannelHandlerContext ctx, int opaque, byte[] key, String value) {
		try {
			if (value == null) {
				return;
			}
			FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse(Unpooled.wrappedBuffer(key),
					null, Unpooled.wrappedBuffer(value.getBytes()));
			response.setStatus(BinaryMemcacheResponseStatus.SUCCESS);
			response.setOpcode(opcode);
			response.setOpaque(opaque);
			response.setTotalBodyLength(key.length + value.length());
			MemcacheUtils.writeAndFlush(ctx, response);
		} catch (Exception e) {
			handleException(ctx, e);
		}
	}

	@Override
	public Future<?> touch(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		try {
			MemcacheUtils.logRequest(request);
			long expiration = request.extras().readUnsignedInt();

			IExecutorService executor = getExecutor();

			ICompletableFuture<HazelcastMemcacheMessage> future = (ICompletableFuture<HazelcastMemcacheMessage>) executor
					.submitToKeyOwner(new HazelcastTouchCallable(authMsgHandler.getCacheName(), key, expiration), key);
			ExecutionCallback<HazelcastMemcacheMessage> callback = new ExecutionCallback<HazelcastMemcacheMessage>() {
				@Override
				public void onResponse(HazelcastMemcacheMessage msg) {
					LogUtils.setupChannelMdc(channelId);
					try {
						if (!msg.isSuccess()) {
							MemcacheUtils.returnFailure(opcode, opaque, msg.getCode(), false, msg.getMsg()).send(ctx);
							return;
						}
						MemcacheUtils.returnSuccess(opcode, opaque, 0, null).send(ctx);
					} catch (Exception e) {
						handleException(ctx, e);
					} finally {
						LogUtils.cleanupChannelMdc();
					}
				}

				public void onFailure(Throwable t) {
					LogUtils.setupChannelMdc(channelId);
					try {
						handleException(ctx, t);
					} finally {
						LogUtils.cleanupChannelMdc();
					}
				}
			};
			executeOrAddCallback(future, callback);
			return future;
		} catch (Exception e) {
			return handleException(ctx, e);
		}
	}

	@Override
	public Future<?> gat(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		try {
			MemcacheUtils.logRequest(request);

			long expiration = request.extras().readUnsignedInt();

			IExecutorService executor = getExecutor();
			ICompletableFuture<HazelcastMemcacheMessage> future = (ICompletableFuture<HazelcastMemcacheMessage>) executor
					.submitToKeyOwner(new HazelcastGATCallable(authMsgHandler.getCacheName(), key, expiration), key);
			ExecutionCallback<HazelcastMemcacheMessage> callback = new ExecutionCallback<HazelcastMemcacheMessage>() {
				@Override
				public void onResponse(HazelcastMemcacheMessage msg) {
					LogUtils.setupChannelMdc(channelId);
					try {
						if (!msg.isSuccess()) {
							if (msg.getCode() == BinaryMemcacheResponseStatus.KEY_ENOENT
									&& (opcode == BinaryMemcacheOpcodes.GATQ
											|| opcode == BinaryMemcacheOpcodes.GATKQ)) {
								MemcacheUtils.returnQuiet(opcode, opaque).send(ctx);
							} else {
								MemcacheUtils.returnFailure(opcode, opaque, msg.getCode(), false, msg.getMsg()).send(ctx);
							}
							return;
						}
						HazelcastMemcacheCacheValue value = msg.getValue();
						ByteBuf responseValue = value.getValue();
						ByteBuf responseFlags = value.getFlags();

						if (value.getFlagLength() == 0 && value.getTotalFlagsAndValueLength() == 8) {
							long incDecValue = value.getValue().readLong();
							responseValue = Unpooled
									.wrappedBuffer(Long.toUnsignedString(incDecValue).getBytes(StandardCharsets.UTF_8));
							responseFlags = Unpooled.wrappedBuffer(new byte[4]);
						}

						FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse(null, responseFlags,
								responseValue);
						response.setStatus(BinaryMemcacheResponseStatus.SUCCESS);
						response.setOpcode(opcode);
						response.setCas(msg.getValue().getCAS());
						response.setOpaque(opaque);
						if (opcode == BinaryMemcacheOpcodes.GATK || opcode == BinaryMemcacheOpcodes.GETKQ) {
							ByteBuf responseKey = Unpooled.wrappedBuffer(key);
							response.setKey(responseKey);
							response.setTotalBodyLength(
									responseFlags.capacity() + responseValue.capacity() + key.length);
						} else {
							response.setTotalBodyLength(responseFlags.capacity() + responseValue.capacity());
						}
						MemcacheUtils.writeAndFlush(ctx, response);
					} catch (Exception e) {
						handleException(ctx, e);
					} finally {
						LogUtils.cleanupChannelMdc();
					}
				}

				public void onFailure(Throwable t) {
					LogUtils.setupChannelMdc(channelId);
					try {
						handleException(ctx, t);
					} finally {
						LogUtils.cleanupChannelMdc();
					}
				}
			};
			executeOrAddCallback(future, callback);
			return future;
		} catch (Exception e) {
			return handleException(ctx, e);
		}
	}
}
