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

import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
	
	final HazelcastInstance instance;
	HazelcastMemcacheCacheValue cacheValue;
	final AuthMsgHandler authMsgHandler;
	private static final int MAX_EXPIRATION_SEC = 60 * 60 * 24 * 30;
	private static final int MAX_VALUE_SIZE = 1048576;
	private static final long MAX_EXPIRATION = 0xFFFFFFFF;
	final byte opcode;
	final int opaque;
	final String key;
	final long cas;
	long expirationInSeconds;
	

	public HazelcastMemcacheMsgHandler(BinaryMemcacheRequest request, AuthMsgHandler authMsgHandler, HazelcastInstance instance) {
		this.authMsgHandler = authMsgHandler;
		this.instance = instance;
		this.opcode = request.opcode();
		this.opaque = request.opaque();
		this.key = request.key();
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

	public IMap<String, HazelcastMemcacheCacheValue> getCache() {
		return instance.getMap(authMsgHandler.getCacheName());
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

		IExecutorService executor = instance.getExecutorService("exec");
		
		executor.submitToKeyOwner(new HazelcastGetCallable(authMsgHandler.getCacheName(), key), key, new ExecutionCallback<HazelcastMemcacheCacheValue>() {
			@Override
			public void onResponse(HazelcastMemcacheCacheValue value) {
				if(value == null) {
					System.out.println("Heap Cost:"+getCache().getLocalMapStats().getHeapCost());
					System.out.println("No value returned.");
				}
				if (value == null && opcode == nonQuietOpcode) {
					MemcacheUtils.returnFailure(opcode, opaque, BinaryMemcacheResponseStatus.KEY_ENOENT, "Unable to find Key: " + key).send(ctx);
					return;
				} else if (value == null) {
					MemcacheUtils.returnQuiet(opcode,  opaque).send(ctx);
					return;
				}

				FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse(key, value.getFlags(), value.getValue());
				response.setStatus(BinaryMemcacheResponseStatus.SUCCESS);
				response.setOpcode(opcode);
				response.setCas(value.getCAS());
				response.setExtrasLength(value.getFlagLength());
				response.setOpaque(opaque);
				if (includeKey) {
					response.setKeyLength((short) key.length());
					response.setTotalBodyLength(value.getTotalFlagsAndValueLength() + key.length());
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
			IMap<String, HazelcastMemcacheCacheValue> cache = getCache();
			if (cas == 0) {
				setValue(ctx, cache, nonQuietOpcode);
				return false;
			} else {
				try {
					cache.lock(key);
					HazelcastMemcacheCacheValue value = cache.get(key);
					if (value == null) {
						return MemcacheUtils.returnFailure(opcode, opaque, BinaryMemcacheResponseStatus.KEY_ENOENT, "No entry exists to check CAS against.")
								.send(ctx);
					}
					long valueCAS = value.getCAS();
					if (cas != valueCAS) {
						return MemcacheUtils.returnFailure(opcode, opaque, BinaryMemcacheResponseStatus.KEY_EEXISTS, "CAS values don't match.").send(ctx);
					}
					setValue(ctx, cache, nonQuietOpcode);
					return false;
				} finally {
					cache.unlock(key);
				}
			}
		}
		return true;
	}

	private void setValue(ChannelHandlerContext ctx, IMap<String, HazelcastMemcacheCacheValue> cache, byte nonQuietOpcode) {
		boolean success = false;
		if (nonQuietOpcode == BinaryMemcacheOpcodes.SET) {
			cache.set(key, cacheValue, expirationInSeconds, TimeUnit.SECONDS);
			success = true;
		} else if (nonQuietOpcode == BinaryMemcacheOpcodes.ADD) {
			success = cache.putIfAbsent(key, cacheValue, expirationInSeconds, TimeUnit.SECONDS) == null ? true : false;
		} else if (nonQuietOpcode == BinaryMemcacheOpcodes.REPLACE) {
			try {
				cache.lock(key);
				if (cache.containsKey(key)) {
					cache.set(key, cacheValue, expirationInSeconds, TimeUnit.SECONDS);
					success = true;
				} else {
					success = false;
				}
			} finally {
				cache.unlock(key);
			}
		}
		if (success) {
			if (opcode == nonQuietOpcode) {
				MemcacheUtils.returnSuccess(opcode, opaque, cacheValue.getCAS(), null).send(ctx);
			} else {
				MemcacheUtils.returnQuiet(opcode,  opaque).send(ctx);
			}
		} else {
			if (nonQuietOpcode == BinaryMemcacheOpcodes.SET) {
				MemcacheUtils.returnFailure(opcode, opaque, BinaryMemcacheResponseStatus.NOT_STORED, "Couldn't set the value for some reason.").send(ctx);
			} else if (nonQuietOpcode == BinaryMemcacheOpcodes.ADD) {
				MemcacheUtils.returnFailure(opcode, opaque, BinaryMemcacheResponseStatus.KEY_EEXISTS, "An value already exists with for the given key.").send(
						ctx);
			} else if (nonQuietOpcode == BinaryMemcacheOpcodes.REPLACE) {
				MemcacheUtils.returnFailure(opcode, opaque, BinaryMemcacheResponseStatus.KEY_ENOENT, "No value to replace for the given key.").send(ctx);
			}
		}
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
		IMap<String, HazelcastMemcacheCacheValue> cache = getCache();
		boolean removed = cache.remove(request.key()) == null ? false : true;
		if (removed && request.opcode() == BinaryMemcacheOpcodes.DELETE) {
			return MemcacheUtils.returnSuccess(request.opcode(), request.opaque(), 0, null).send(ctx);
		} else if (!removed) {
			return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.KEY_ENOENT, "Entry not found.").send(ctx);
		}
		return MemcacheUtils.returnQuiet(request.opcode(),  request.opaque()).send(ctx);
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
		IMap<String, HazelcastMemcacheCacheValue> cache = getCache();
		HazelcastMemcacheCacheValue value;

		try {
			cache.lock(key);
			value = cache.get(key);
			if (value != null) {
				long currentValue = 0;
				if (value.getFlagLength() == 0) {
					currentValue = value.getValue().getLong(0);
				} else {
					byte[] valueBytes = value.getCacheEntry();
					if (valueBytes.length > 21) {
						return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.DELTA_BADVAL,
								"The ASCII value currently in key has too many digits.").send(ctx);
					}
					try {
						currentValue = Long.parseUnsignedLong(new String(valueBytes, "ASCII"));
					} catch (NumberFormatException e) {
						if (valueBytes.length == 8) {
							currentValue = value.getValue().getLong(0);
						} else {
							return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.DELTA_BADVAL, "Unable to parse existing value.").send(ctx);
						}
					} catch (UnsupportedEncodingException e) {
						throw new RuntimeException(e);
					}
				}
				if (increment) {
					currentValue += delta;
				} else {
					if (Long.compareUnsigned(currentValue, delta) < -0) {
						currentValue = 0;
					} else {
						currentValue -= delta;
					}
				}
				value = new HazelcastMemcacheCacheValue(8, Unpooled.EMPTY_BUFFER, value.getExpiration());
				value.writeValue(Unpooled.copyLong(currentValue));
			} else {
				value = new HazelcastMemcacheCacheValue(8, Unpooled.EMPTY_BUFFER, expiration);
				value.writeValue(extras.slice(8, 8));
			}
			cache.set(key, value, expirationInSeconds, TimeUnit.SECONDS);
		} finally {
			cache.unlock(key);
		}
		if (opcode == BinaryMemcacheOpcodes.INCREMENT || opcode == BinaryMemcacheOpcodes.DECREMENT) {
			FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse(null, null, value.getValue());
			response.setStatus(BinaryMemcacheResponseStatus.SUCCESS);
			response.setOpcode(opcode);
			response.setCas(value.getCAS());
			response.setOpaque(request.opaque());
			response.setTotalBodyLength(value.getTotalFlagsAndValueLength());
			ctx.writeAndFlush(response.retain());
		}
		return MemcacheUtils.returnQuiet(request.opcode(),  request.opaque()).send(ctx);
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

	@Override
	public boolean append(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		MemcacheUtils.logRequest(request);

		int valueSize = request.totalBodyLength() - request.keyLength() - request.extrasLength();

		if (valueSize > MAX_VALUE_SIZE) {
			return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.E2BIG, "Value too big.  Max Value is " + MAX_VALUE_SIZE).send(ctx);
		}
		cacheValue = new HazelcastMemcacheCacheValue(valueSize, Unpooled.EMPTY_BUFFER, 0);
		return true;
	}

	@Override
	public boolean append(ChannelHandlerContext ctx, MemcacheContent content) {
		cacheValue.writeValue(content.content());
		if (content instanceof LastMemcacheContent) {
			IMap<String, HazelcastMemcacheCacheValue> cache = getCache();
			try {
				cache.lock(key);
				HazelcastMemcacheCacheValue value = cache.get(key);
				if (value == null) {
					return MemcacheUtils.returnFailure(opcode, opaque, BinaryMemcacheResponseStatus.KEY_ENOENT,
							"No value exists to append at the specified key.").send(ctx);
				}
				if (value.getFlagLength() == 0) {
					return MemcacheUtils.returnFailure(opcode, opaque, BinaryMemcacheResponseStatus.DELTA_BADVAL,
							"Value appears to be an inc/dec number.  Cannot append to this.").send(ctx);
				}
				int totalValueLength = value.getValueLength() + cacheValue.getValueLength();
				if (totalValueLength > MAX_VALUE_SIZE) {
					return MemcacheUtils.returnFailure(opcode, opaque, BinaryMemcacheResponseStatus.E2BIG, "Value too big.  Max Value is " + MAX_VALUE_SIZE)
							.send(ctx);
				}
				HazelcastMemcacheCacheValue appendedValue = new HazelcastMemcacheCacheValue(totalValueLength, value.getFlags(), value.getExpiration());
				appendedValue.writeValue(value.getValue());
				appendedValue.writeValue(cacheValue.getValue());

				cache.set(key, appendedValue, appendedValue.getExpiration(), TimeUnit.SECONDS);
			} finally {
				cache.unlock(key);
			}
			if (opcode == BinaryMemcacheOpcodes.APPEND) {
				MemcacheUtils.returnSuccess(opcode, opaque, cacheValue.getCAS(), null).send(ctx);
			} else {
				MemcacheUtils.returnQuiet(opcode,  opaque).send(ctx);
			}
		}
		return true;
	}

	@Override
	public boolean prepend(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		MemcacheUtils.logRequest(request);

		int valueSize = request.totalBodyLength() - request.keyLength() - request.extrasLength();

		if (valueSize > MAX_VALUE_SIZE) {
			return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.E2BIG, "Value too big.  Max Value is " + MAX_VALUE_SIZE).send(ctx);
		}
		cacheValue = new HazelcastMemcacheCacheValue(valueSize, Unpooled.EMPTY_BUFFER, 0);
		return true;
	}

	@Override
	public boolean prepend(ChannelHandlerContext ctx, MemcacheContent content) {
		cacheValue.writeValue(content.content());
		if (content instanceof LastMemcacheContent) {
			IMap<String, HazelcastMemcacheCacheValue> cache = getCache();
			try {
				cache.lock(key);
				HazelcastMemcacheCacheValue value = cache.get(key);
				if (value == null) {
					return MemcacheUtils.returnFailure(opcode, opaque, BinaryMemcacheResponseStatus.KEY_ENOENT,
							"No value exists to append at the specified key.").send(ctx);
				}
				if (value.getFlagLength() == 0) {
					return MemcacheUtils.returnFailure(opcode, opaque, BinaryMemcacheResponseStatus.DELTA_BADVAL,
							"Value appears to be an inc/dec number.  Cannot append to this.").send(ctx);
				}
				int totalValueLength = value.getValueLength() + cacheValue.getValueLength();
				if (totalValueLength > MAX_VALUE_SIZE) {
					return MemcacheUtils.returnFailure(opcode, opaque, BinaryMemcacheResponseStatus.E2BIG, "Value too big.  Max Value is " + MAX_VALUE_SIZE)
							.send(ctx);
				}
				HazelcastMemcacheCacheValue appendedValue = new HazelcastMemcacheCacheValue(totalValueLength, value.getFlags(), value.getExpiration());
				appendedValue.writeValue(cacheValue.getValue());
				appendedValue.writeValue(value.getValue());

				cache.set(key, appendedValue, appendedValue.getExpiration(), TimeUnit.SECONDS);
			} finally {
				cache.unlock(key);
			}
			if (opcode == BinaryMemcacheOpcodes.PREPEND) {
				MemcacheUtils.returnSuccess(opcode, opaque, cacheValue.getCAS(), null).send(ctx);
			} else {
				MemcacheUtils.returnQuiet(opcode,  opaque).send(ctx);
			}
		}
		return true;
	}

	private static final Map<String, Stat> STATS;

	static {
		STATS = new LinkedHashMap<>();
		STATS.put("pid", (handler) -> {
			return ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
		});
		STATS.put("uptime", (handler) -> {
			long startTime = (Long) handler.getInstance().getReplicatedMap(Stat.STAT_MAP).get(Stat.UPTIME_KEY);
			return String.valueOf((System.currentTimeMillis() - startTime) / 1000);
		});
		STATS.put("time", (handler) -> {
			return String.valueOf(System.currentTimeMillis() / 1000);
		});
		STATS.put("version", (handler) -> {
			return "1.0";
		});
		STATS.put("bytes", (handler) -> {
			return String.valueOf(handler.getCache().getLocalMapStats().getHeapCost());
		});
	}

	@Override
	public boolean stat(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		MemcacheUtils.logRequest(request);
		if (key != null && !key.isEmpty()) {
			sendStat(ctx, opaque, key, STATS.get(key).getStat(this));
		} else {
			for (Map.Entry<String, Stat> stat : STATS.entrySet()) {
				sendStat(ctx, opaque, stat.getKey(), stat.getValue().getStat(this));
			}
		}
		return MemcacheUtils.returnSuccess(request.opcode(), request.opaque(), 0, null).send(ctx);
	}

	private void sendStat(ChannelHandlerContext ctx, int opaque, String key, String value) {
		FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse(null, null, Unpooled.wrappedBuffer(value.getBytes()));
		response.setStatus(BinaryMemcacheResponseStatus.SUCCESS);
		response.setOpcode(opcode);
		response.setOpaque(opaque);
		response.setTotalBodyLength(key.length() + value.length());
		ctx.writeAndFlush(response.retain());
	}

	@Override
	public boolean touch(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		MemcacheUtils.logRequest(request);

		IMap<String, HazelcastMemcacheCacheValue> cache = getCache();

		long expiration = request.extras().readUnsignedInt();
		long currentTimeInSeconds = System.currentTimeMillis() / 1000;
		if (expiration <= MAX_EXPIRATION_SEC) {
			expirationInSeconds = expiration;
		} else if (expiration > currentTimeInSeconds) {
			expirationInSeconds = expiration - currentTimeInSeconds;
		} else {
			cache.remove(key);
			return MemcacheUtils.returnSuccess(request.opcode(), request.opaque(), 0, null).send(ctx);
		}

		try {
			cache.lock(key);
			HazelcastMemcacheCacheValue value = cache.get(key);

			if (value == null) {
				return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.KEY_ENOENT, "Unable to find Key: " + key).send(ctx);
			}
			if (value.getExpiration() == expiration) {
				cache.set(key, value, expiration, TimeUnit.SECONDS);
			} else {
				HazelcastMemcacheCacheValue newValue = new HazelcastMemcacheCacheValue(value.getValueLength(), value.getFlags(), expiration);
				cache.set(key, newValue, expiration, TimeUnit.SECONDS);
			}
		} finally {
			cache.unlock(key);
		}
		return MemcacheUtils.returnSuccess(opcode, opaque, 0, null).send(ctx);
	}

	@Override
	public boolean gat(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		MemcacheUtils.logRequest(request);

		IMap<String, HazelcastMemcacheCacheValue> cache = getCache();

		long expiration = request.extras().readUnsignedInt();
		long currentTimeInSeconds = System.currentTimeMillis() / 1000;
		if (expiration <= MAX_EXPIRATION_SEC) {
			expirationInSeconds = expiration;
		} else if (expiration > currentTimeInSeconds) {
			expirationInSeconds = expiration - currentTimeInSeconds;
		} else {
			HazelcastMemcacheCacheValue value = cache.remove(key);
			if(value == null) {
				if(opcode == BinaryMemcacheOpcodes.GAT || opcode == BinaryMemcacheOpcodes.GATK) {
					return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.KEY_ENOENT, "Unable to find Key: " + key).send(ctx);
				} else {
					return MemcacheUtils.returnQuiet(opcode,  opaque).send(ctx);
				}
			} else {
				return returnValue(ctx, value, opcode == BinaryMemcacheOpcodes.GATK || opcode == BinaryMemcacheOpcodes.GETKQ, key);
			}
		}

		HazelcastMemcacheCacheValue value;
		try {
			cache.lock(key);
			value = cache.get(key);

			if (value == null) {
				return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.KEY_ENOENT, "Unable to find Key: " + key).send(ctx);
			}
			if (value.getExpiration() == expiration) {
				cache.set(key, value, expiration, TimeUnit.SECONDS);
			} else {
				HazelcastMemcacheCacheValue newValue = new HazelcastMemcacheCacheValue(value.getValueLength(), value.getFlags(), expiration);
				cache.set(key, newValue, expiration, TimeUnit.SECONDS);
			}
		} finally {
			cache.unlock(key);
		}
		return returnValue(ctx, value, opcode == BinaryMemcacheOpcodes.GATK || opcode == BinaryMemcacheOpcodes.GETKQ, key);
	}

	private boolean returnValue(ChannelHandlerContext ctx, HazelcastMemcacheCacheValue value, boolean includeKey, String key) {
		FullBinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse(null, value.getFlags(), value.getValue());
		response.setStatus(BinaryMemcacheResponseStatus.SUCCESS);
		response.setOpcode(opcode);
		response.setCas(value.getCAS());
		response.setExtrasLength(value.getFlagLength());
		response.setOpaque(opaque);
		if (includeKey) {
			response.setKey(key);
			response.setKeyLength((short) key.length());
			response.setTotalBodyLength(value.getTotalFlagsAndValueLength() + key.length());
		} else {
			response.setTotalBodyLength(value.getTotalFlagsAndValueLength());
		}
		ctx.writeAndFlush(response.retain());
		return false;
	}
}
