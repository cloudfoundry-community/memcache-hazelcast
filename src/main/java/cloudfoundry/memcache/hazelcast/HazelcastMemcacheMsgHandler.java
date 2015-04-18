package cloudfoundry.memcache.hazelcast;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.memcache.LastMemcacheContent;
import io.netty.handler.codec.memcache.MemcacheContent;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheOpcodes;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheRequest;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheResponseStatus;
import io.netty.handler.codec.memcache.binary.DefaultFullBinaryMemcacheResponse;
import io.netty.handler.codec.memcache.binary.FullBinaryMemcacheResponse;

import java.lang.management.ManagementFactory;
import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cloudfoundry.memcache.AuthMsgHandler;
import cloudfoundry.memcache.MemcacheMsgHandler;
import cloudfoundry.memcache.MemcacheUtils;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IMap;

public class HazelcastMemcacheMsgHandler implements MemcacheMsgHandler {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(HazelcastMemcacheMsgHandler.class);
	public static final int MAX_VALUE_SIZE = 1048576;
	final HazelcastInstance instance;
	HazelcastMemcacheCacheValue cacheValue;
	final AuthMsgHandler authMsgHandler;
	public static final int MAX_EXPIRATION_SEC = 60 * 60 * 24 * 30;
	public static final long MAX_EXPIRATION = 0xFFFFFFFF;
	final byte opcode;
	final int opaque;
	final byte[] key;
	final long cas;
	long expirationInSeconds;
	

	public HazelcastMemcacheMsgHandler(BinaryMemcacheRequest request, AuthMsgHandler authMsgHandler, HazelcastInstance instance) {
		this.authMsgHandler = authMsgHandler;
		this.instance = instance;
		this.opcode = request.opcode();
		this.opaque = request.opaque();
		this.key = request.key() == null ? null : request.key().getBytes();
		this.cas = request.cas();
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
		return instance.getExecutorService("exec");
	}

	public HazelcastInstance getInstance() {
		return instance;
	}

	@Override
	public boolean get(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return handleGetRequest(ctx, request, BinaryMemcacheOpcodes.GET, false);
	}

	private boolean handleGetRequest(ChannelHandlerContext ctx, BinaryMemcacheRequest request, byte nonQuietOpcode, boolean includeKey) {
		MemcacheUtils.logRequest(request);

		IExecutorService executor = getExecutor();

		executor.submitToKeyOwner(new HazelcastGetCallable(authMsgHandler.getCacheName(), key), key, new ExecutionCallback<HazelcastMemcacheCacheValue>() {
			@Override
			public void onResponse(HazelcastMemcacheCacheValue value) {
			
				if (value == null && opcode == nonQuietOpcode) {
					MemcacheUtils.returnFailure(opcode, opaque, BinaryMemcacheResponseStatus.KEY_ENOENT, "Unable to find Key: " + key).send(ctx);
					return;
				} else if (value == null) {
					MemcacheUtils.returnQuiet(opcode,  opaque).send(ctx);
					return;
				}

				FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse(new String(key), value.getFlags(), value.getValue());
				response.setStatus(BinaryMemcacheResponseStatus.SUCCESS);
				response.setOpcode(opcode);
				response.setCas(value.getCAS());
				response.setExtrasLength(value.getFlagLength());
				response.setOpaque(opaque);
				if (includeKey) {
					response.setKeyLength((short) key.length);
					response.setTotalBodyLength(value.getTotalFlagsAndValueLength() + key.length);
				} else {
					response.setKey(null);
					response.setTotalBodyLength(value.getTotalFlagsAndValueLength());
				}
				ctx.writeAndFlush(response.retain());
				if(LOGGER.isDebugEnabled()) {
					LOGGER.debug("Current Socket: "+ctx.channel().id()+" Flushed response for key: "+key);
				}
			}
			
			@Override
			public void onFailure(Throwable t) {
				LOGGER.error("Error invoking GET asyncronously", t);
				MemcacheUtils.returnFailure(getOpcode(), getOpaque(), (short)0x0084, t.getMessage()).send(ctx);
			}
		});

		return false;
	}

	@Override
	public boolean getK(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return handleGetRequest(ctx, request, BinaryMemcacheOpcodes.GETK, true);
	}

	@Override
	public boolean set(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return processSetAddReplaceRequest(ctx, request);
	}

	private boolean processSetAddReplaceRequest(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		MemcacheUtils.logRequest(request);

		int valueSize = request.totalBodyLength() - request.keyLength() - request.extrasLength();
		if (valueSize > MAX_VALUE_SIZE) {
			return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.E2BIG, "Value too big.  Max Value is " + MAX_VALUE_SIZE).send(ctx);
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
			return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.NOT_STORED, "Expiration is in the past.").send(ctx);
		}
		return true;
	}

	@Override
	public boolean set(ChannelHandlerContext ctx, MemcacheContent content) {
		return processSetAddReplaceContent(ctx, content, BinaryMemcacheOpcodes.SET);
	}

	private boolean processSetAddReplaceContent(ChannelHandlerContext ctx, MemcacheContent content, byte nonQuietOpcode) {
		cacheValue.writeValue(content.content());
		if (content instanceof LastMemcacheContent) {
			IExecutorService executor = getExecutor();

			executor.submitToKeyOwner(new HazelcastSetCallable(authMsgHandler.getCacheName(), nonQuietOpcode, key, cas, cacheValue, expirationInSeconds), key,
					new ExecutionCallback<HazelcastMemcacheMessage>() {
						@Override
						public void onResponse(HazelcastMemcacheMessage response) {
							if (response.isSuccess()) {
								if (opcode == nonQuietOpcode) {
									MemcacheUtils.returnSuccess(opcode, opaque, response.getCas(), null).send(ctx);
								} else {
									MemcacheUtils.returnQuiet(opcode, opaque).send(ctx);
								}
							} else {
								MemcacheUtils.returnFailure(opcode, opaque, response.getCode(), response.getMsg()).send(ctx);
							}
						}

						public void onFailure(Throwable t) {
							LOGGER.error("Error invoking GET asyncronously", t);
							MemcacheUtils.returnFailure(getOpcode(), getOpaque(), (short) 0x0084, t.getMessage()).send(ctx);
						}
					});
			//Null out so it can get GCed No need to keep it now.
			cacheValue = null;
			return false;
		}
		return true;
	}

	@Override
	public boolean add(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return processSetAddReplaceRequest(ctx, request);
	}

	@Override
	public boolean add(ChannelHandlerContext ctx, MemcacheContent content) {
		return processSetAddReplaceContent(ctx, content, BinaryMemcacheOpcodes.ADD);
	}

	@Override
	public boolean replace(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return processSetAddReplaceRequest(ctx, request);
	}

	@Override
	public boolean replace(ChannelHandlerContext ctx, MemcacheContent content) {
		return processSetAddReplaceContent(ctx, content, BinaryMemcacheOpcodes.REPLACE);
	}

	@Override
	public boolean delete(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		MemcacheUtils.logRequest(request);
		IExecutorService executor = getExecutor();

		executor.submitToKeyOwner(new HazelcastDeleteCallable(authMsgHandler.getCacheName(), key), key, new ExecutionCallback<Boolean>() {
			@Override
			public void onResponse(Boolean response) {
				if (response && opcode == BinaryMemcacheOpcodes.DELETE) {
					MemcacheUtils.returnSuccess(request.opcode(), request.opaque(), 0, null).send(ctx);
				} else if (!response) {
					MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.KEY_ENOENT, "Entry not found.").send(ctx);
				} else {
					MemcacheUtils.returnQuiet(request.opcode(), request.opaque()).send(ctx);
				}
			}

			public void onFailure(Throwable t) {
				LOGGER.error("Error invoking GET asyncronously", t);
				MemcacheUtils.returnFailure(getOpcode(), getOpaque(), (short) 0x0084, t.getMessage()).send(ctx);
			}
		});
		return false;
	}

	@Override
	public boolean increment(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return handleIncrementAndDecrement(ctx, request, true);
	}

	private boolean handleIncrementAndDecrement(ChannelHandlerContext ctx, BinaryMemcacheRequest request, boolean increment) {
		MemcacheUtils.logRequest(request);
		ByteBuf extras = request.extras();
		long delta = extras.getLong(0);
		long expiration = extras.getUnsignedInt(16);
		long currentTimeInSeconds = System.currentTimeMillis() / 1000;
		long initialValue = extras.slice(8, 8).readLong();
		if (expiration == MAX_EXPIRATION) {
			return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.KEY_ENOENT,
					"Expiration is MAX Value.  Not allowed according to spec for some reason.").send(ctx);
		}
		if (expiration <= MAX_EXPIRATION_SEC) {
			expirationInSeconds = expiration;
		} else if (expiration > currentTimeInSeconds) {
			expirationInSeconds = expiration - currentTimeInSeconds;
		} else {
			return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.NOT_STORED, "Expiration is in the past.").send(ctx);
		}
		IExecutorService executor = getExecutor();

		executor.submitToKeyOwner(new HazelcastIncDecCallable(authMsgHandler.getCacheName(), key, expirationInSeconds, increment, delta, expiration, initialValue), key, new ExecutionCallback<HazelcastMemcacheMessage>() {
			@Override
			public void onResponse(HazelcastMemcacheMessage msg) {
				if(!msg.isSuccess()) {
					MemcacheUtils.returnFailure(opcode, opaque, msg.getCode(), msg.getMsg()).send(ctx);
					return;
				}
				if (opcode == BinaryMemcacheOpcodes.INCREMENT || opcode == BinaryMemcacheOpcodes.DECREMENT) {
					FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse(null, null, msg.getValue().getValue());
					response.setStatus(BinaryMemcacheResponseStatus.SUCCESS);
					response.setOpcode(opcode);
					response.setCas(msg.getValue().getCAS());
					response.setOpaque(opaque);
					response.setTotalBodyLength(msg.getValue().getTotalFlagsAndValueLength());
					ctx.writeAndFlush(response.retain());
				}
				MemcacheUtils.returnQuiet(request.opcode(),  request.opaque()).send(ctx);
			}

			public void onFailure(Throwable t) {
				LOGGER.error("Error invoking GET asyncronously", t);
				MemcacheUtils.returnFailure(getOpcode(), getOpaque(), (short) 0x0084, t.getMessage()).send(ctx);
			}
		});
		return false;
	}

	@Override
	public boolean decrement(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return handleIncrementAndDecrement(ctx, request, false);
	}

	@Override
	public boolean quit(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		MemcacheUtils.logRequest(request);
		if (request.opcode() == BinaryMemcacheOpcodes.QUIT) {
			MemcacheUtils.returnSuccess(request.opcode(), request.opaque(), 0, null).send(ctx);
		}
		try {
			ctx.close().await();
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
		return false;
	}

	@Override
	public boolean flush(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		MemcacheUtils.logRequest(request);
		getCache().clear();
		if (request.opcode() == BinaryMemcacheOpcodes.FLUSH) {
			return MemcacheUtils.returnSuccess(request.opcode(), request.opaque(), 0, null).send(ctx);
		}
		return MemcacheUtils.returnQuiet(request.opcode(),  request.opaque()).send(ctx);
	}

	@Override
	public boolean noop(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		MemcacheUtils.logRequest(request);
		return MemcacheUtils.returnSuccess(request.opcode(), request.opaque(), 0, null).send(ctx);
	}

	@Override
	public boolean version(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		MemcacheUtils.logRequest(request);
		return MemcacheUtils.returnSuccess(request.opcode(), request.opaque(), 0, "CF Memcache 1.0").send(ctx);
	}

	private boolean appendPrepend(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		MemcacheUtils.logRequest(request);

		int valueSize = request.totalBodyLength() - request.keyLength() - request.extrasLength();

		if (valueSize > MAX_VALUE_SIZE) {
			return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.E2BIG, "Value too big.  Max Value is " + MAX_VALUE_SIZE).send(ctx);
		}
		cacheValue = new HazelcastMemcacheCacheValue(valueSize, Unpooled.EMPTY_BUFFER, 0);
		return true;
	}

	private boolean appendPrepend(ChannelHandlerContext ctx, MemcacheContent content, boolean append) {
		cacheValue.writeValue(content.content());
		if (content instanceof LastMemcacheContent) {
			IExecutorService executor = getExecutor();

			executor.submitToKeyOwner(new HazelcastAppendPrependCallable(authMsgHandler.getCacheName(), key, cacheValue, append), key, new ExecutionCallback<HazelcastMemcacheMessage>() {
				@Override
				public void onResponse(HazelcastMemcacheMessage msg) {
					if(!msg.isSuccess()) {
						MemcacheUtils.returnFailure(opcode, opaque, msg.getCode(), msg.getMsg()).send(ctx);
					}
					if (opcode == BinaryMemcacheOpcodes.APPEND || opcode == BinaryMemcacheOpcodes.PREPEND) {
						MemcacheUtils.returnSuccess(opcode, opaque, msg.getCas(), null).send(ctx);
					} else {
						MemcacheUtils.returnQuiet(opcode,  opaque).send(ctx);
					}
				}

				public void onFailure(Throwable t) {
					LOGGER.error("Error invoking Opcode "+opcode+" asyncronously", t);
					MemcacheUtils.returnFailure(opcode, opaque, (short) 0x0084, t.getMessage()).send(ctx);
				}
			});
			//null out so it can get GCed while waiting for a response.
			cacheValue = null;
			return false;
		}
		return true;
	}

	@Override
	public boolean append(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return appendPrepend(ctx, request);
	}

	@Override
	public boolean append(ChannelHandlerContext ctx, MemcacheContent content) {
		return appendPrepend(ctx, content, true);
	}

	@Override
	public boolean prepend(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return appendPrepend(ctx, request);
	}

	@Override
	public boolean prepend(ChannelHandlerContext ctx, MemcacheContent content) {
		return appendPrepend(ctx, content, false);
	}

	private static final Map<byte[], Stat> STATS;

	static {
		STATS = new LinkedHashMap<>();
		STATS.put("pid".getBytes(), (handler) -> {
			return ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
		});
		STATS.put("uptime".getBytes(), (handler) -> {
			long startTime = (Long) handler.getInstance().getReplicatedMap(Stat.STAT_MAP).get(Stat.UPTIME_KEY);
			return String.valueOf((System.currentTimeMillis() - startTime) / 1000);
		});
		STATS.put("time".getBytes(), (handler) -> {
			return String.valueOf(System.currentTimeMillis() / 1000);
		});
		STATS.put("version".getBytes(), (handler) -> {
			return "1.0";
		});
		STATS.put("bytes".getBytes(), (handler) -> {
			return String.valueOf(handler.getCache().getLocalMapStats().getHeapCost());
		});
	}

	@Override
	public boolean stat(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		MemcacheUtils.logRequest(request);
		if (key != null) {
			sendStat(ctx, opaque, key, STATS.get(key).getStat(this));
		} else {
			for (Map.Entry<byte[], Stat> stat : STATS.entrySet()) {
				sendStat(ctx, opaque, stat.getKey(), stat.getValue().getStat(this));
			}
		}
		return MemcacheUtils.returnSuccess(request.opcode(), request.opaque(), 0, null).send(ctx);
	}

	private void sendStat(ChannelHandlerContext ctx, int opaque, byte[] key, String value) {
		FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse(null, null, Unpooled.wrappedBuffer(value.getBytes()));
		response.setStatus(BinaryMemcacheResponseStatus.SUCCESS);
		response.setOpcode(opcode);
		response.setOpaque(opaque);
		response.setTotalBodyLength(key.length + value.length());
		ctx.writeAndFlush(response.retain());
	}

	@Override
	public boolean touch(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		MemcacheUtils.logRequest(request);
		long expiration = request.extras().readUnsignedInt();

		IExecutorService executor = getExecutor();

		executor.submitToKeyOwner(new HazelcastTouchCallable(authMsgHandler.getCacheName(), key, expiration), key,
				new ExecutionCallback<HazelcastMemcacheMessage>() {
					@Override
					public void onResponse(HazelcastMemcacheMessage msg) {
						if (!msg.isSuccess()) {
							MemcacheUtils.returnFailure(opcode, opaque, msg.getCode(), msg.getMsg()).send(ctx);
							return;
						}
						MemcacheUtils.returnSuccess(opcode, opaque, 0, null).send(ctx);
					}

					public void onFailure(Throwable t) {
						LOGGER.error("Error invoking GET asyncronously", t);
						MemcacheUtils.returnFailure(getOpcode(), getOpaque(), (short) 0x0084, t.getMessage()).send(ctx);
					}
				});
		return false;
	}

	@Override
	public boolean gat(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		MemcacheUtils.logRequest(request);

		long expiration = request.extras().readUnsignedInt();

		IExecutorService executor = getExecutor();
		executor.submitToKeyOwner(new HazelcastGATCallable(authMsgHandler.getCacheName(), key, expiration), key,
				new ExecutionCallback<HazelcastMemcacheMessage>() {
					@Override
					public void onResponse(HazelcastMemcacheMessage msg) {
						if (!msg.isSuccess()) {
							if (msg.getCode() == BinaryMemcacheResponseStatus.KEY_ENOENT && (opcode == BinaryMemcacheOpcodes.GATQ
									|| opcode == BinaryMemcacheOpcodes.GATKQ)) {
								MemcacheUtils.returnQuiet(opcode, opaque).send(ctx);
							} else {
								MemcacheUtils.returnFailure(opcode, opaque, msg.getCode(), msg.getMsg()).send(ctx);
							}
							return;
						}
						FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse(null, msg.getValue().getFlags(), msg.getValue().getValue());
						response.setStatus(BinaryMemcacheResponseStatus.SUCCESS);
						response.setOpcode(opcode);
						response.setCas(msg.getValue().getCAS());
						response.setExtrasLength(msg.getValue().getFlagLength());
						response.setOpaque(opaque);
						if (opcode == BinaryMemcacheOpcodes.GATK || opcode == BinaryMemcacheOpcodes.GETKQ) {
							response.setKey(new String(key));
							response.setKeyLength((short) key.length);
							response.setTotalBodyLength(msg.getValue().getTotalFlagsAndValueLength() + key.length);
						} else {
							response.setTotalBodyLength(msg.getValue().getTotalFlagsAndValueLength());
						}
						ctx.writeAndFlush(response.retain());
					}

					public void onFailure(Throwable t) {
						LOGGER.error("Error invoking GET asyncronously", t);
						MemcacheUtils.returnFailure(getOpcode(), getOpaque(), (short) 0x0084, t.getMessage()).send(ctx);
					}
				});
		return false;
	}
}
