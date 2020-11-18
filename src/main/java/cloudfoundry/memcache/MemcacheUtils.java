package cloudfoundry.memcache;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheMessage;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheRequest;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheResponseStatus;
import io.netty.handler.codec.memcache.binary.DefaultFullBinaryMemcacheResponse;
import io.netty.handler.codec.memcache.binary.FullBinaryMemcacheResponse;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MemcacheUtils {

	public static final short INTERNAL_ERROR = (short)0x0084;
	public static final short TEMPORARY_FAILURE = (short)0x0086;

	private MemcacheUtils() {
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(MemcacheUtils.class);

	public static ResponseSender returnFailure(BinaryMemcacheRequest request, short errorCode, boolean forceClose,
			String message) {
		return returnFailure(request.opcode(), request.opaque(), errorCode, forceClose, message);
	}

	public static ResponseSender returnFailure(byte opcode, int opaque, short errorCode, boolean forceClose,
			String message) {
		return ctx -> {
			FullBinaryMemcacheResponse response;
			response = new DefaultFullBinaryMemcacheResponse(null, null,
					Unpooled.wrappedBuffer(message.getBytes(StandardCharsets.US_ASCII)));
			response.setStatus(errorCode);
			response.setOpaque(opaque);
			response.setOpcode(opcode);
			response.setTotalBodyLength(message.length());
			ChannelFuture future = MemcacheUtils.writeAndFlushRaw(ctx, response);
			if (forceClose  || errorCode == INTERNAL_ERROR) {
				return future.addListener(ChannelFutureListener.CLOSE);
			}
			return future.addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
		};
	}

	public static ResponseSender returnSuccess(byte opcode, int opaque, long cas, String message) {
		return ctx -> {
			String realMessage = message == null ? "" : message;
			FullBinaryMemcacheResponse response;

			response = new DefaultFullBinaryMemcacheResponse(null, null,
					Unpooled.wrappedBuffer(realMessage.getBytes(StandardCharsets.US_ASCII)));
			response.setStatus(BinaryMemcacheResponseStatus.SUCCESS);
			response.setOpcode(opcode);
			response.setOpaque(opaque);
			response.setTotalBodyLength(realMessage.length());
			response.setCas(cas);
			return MemcacheUtils.writeAndFlush(ctx, response);
		};
	}

	public static ResponseSender returnQuiet(byte opcode, int opaque) {
		return ctx -> MemcacheUtils.writeAndFlushRaw(ctx, new QuietResponse(opcode, opaque))
				.addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
	}

	private static ChannelFuture writeAndFlushRaw(ChannelHandlerContext ctx, BinaryMemcacheMessage msg) {
		if (ctx.channel().isOpen() && ctx.channel().isActive()) {
			return ctx.channel().writeAndFlush(msg.retain());
		}
		throw new IllegalStateException("Channel was not open or active.  Not able to write msg.");
	}
	
	public static ChannelFuture writeAndFlush(ChannelHandlerContext ctx, BinaryMemcacheMessage msg) {
		if (ctx.channel().isOpen() && ctx.channel().isActive()) {
			return ctx.channel().writeAndFlush(msg.retain()).addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
		}
		throw new IllegalStateException("Channel was not open or active.  Not able to write msg.");
	}


	public static void logRequest(BinaryMemcacheRequest request) {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Opcode: {}", request.opcode());
			LOGGER.debug("Key Length: {}", Short.toUnsignedInt(request.keyLength()));
			LOGGER.debug("Key: {}", request.key());
			LOGGER.debug("CAS: {}", request.cas());
			LOGGER.debug("Magic: {}", request.magic());
			LOGGER.debug("Reserved: {}", request.reserved());
			LOGGER.debug("Opaque: {}", request.opaque());
			LOGGER.debug("Extras Length: {}", Byte.toUnsignedInt(request.extrasLength()));
			LOGGER.debug("Body Length: {}", request.totalBodyLength());
		}
	}

	public static String extractSaslUsername(byte[] auth) {
		if (auth.length == 0) {
			throw new IllegalStateException("Failed to parse SASL password auth body is empty.");
		}
		StringBuilder builder = new StringBuilder();
		int i = 1;
		for (; auth[i] != 0 && i < auth.length; i++) {
			builder.append((char) auth[i]);
		}
		if (i == auth.length) {
			throw new IllegalStateException("Failed to parse SASL username no 0 character found.");
		}
		return builder.toString();
	}

	public static String extractSaslPassword(byte[] auth) {
		if (auth.length == 0) {
			throw new IllegalStateException("Failed to parse SASL password auth body is empty.");
		}
		StringBuilder builder = new StringBuilder();
		int passwordIndex = 1;
		while (auth[passwordIndex] != 0 && passwordIndex < auth.length) {
			passwordIndex++;
		}
		if (passwordIndex == auth.length) {
			throw new IllegalStateException("Failed to parse SASL password no 0 character found.");
		}
		passwordIndex++;
		for (int i = passwordIndex; i < auth.length; i++) {
			builder.append((char) auth[i]);
		}
		return builder.toString();

	}

	private static final long UINT_MASK = 0xFFFFFFFFl;

	public static long hash64(final byte[] data, int start, int length, long seed) {
		final long m = 0xc6a4a7935bd1e995L;
		final int r = 47;

		long h = (seed & UINT_MASK) ^ (length * m);

		int length8 = length >> 3;

		for (int i = start; i < length8; i++) {
			final int i8 = i << 3;

			long k = ((long) data[i8] & 0xff) + (((long) data[i8 + 1] & 0xff) << 8)
					+ (((long) data[i8 + 2] & 0xff) << 16) + (((long) data[i8 + 3] & 0xff) << 24)
					+ (((long) data[i8 + 4] & 0xff) << 32) + (((long) data[i8 + 5] & 0xff) << 40)
					+ (((long) data[i8 + 6] & 0xff) << 48) + (((long) data[i8 + 7] & 0xff) << 56);

			k *= m;
			k ^= k >>> r;
			k *= m;

			h ^= k;
			h *= m;
		}

		switch (length & 7) {
		case 7:
			h ^= (long) (data[(length & ~7) + 6] & 0xff) << 48;

		case 6:
			h ^= (long) (data[(length & ~7) + 5] & 0xff) << 40;

		case 5:
			h ^= (long) (data[(length & ~7) + 4] & 0xff) << 32;

		case 4:
			h ^= (long) (data[(length & ~7) + 3] & 0xff) << 24;

		case 3:
			h ^= (long) (data[(length & ~7) + 2] & 0xff) << 16;

		case 2:
			h ^= (long) (data[(length & ~7) + 1] & 0xff) << 8;

		case 1:
			h ^= (long) (data[length & ~7] & 0xff);
			h *= m;
		}

		h ^= h >>> r;
		h *= m;
		h ^= h >>> r;

		return h;
	}
}
