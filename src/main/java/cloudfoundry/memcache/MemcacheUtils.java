package cloudfoundry.memcache;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.memcache.DefaultLastMemcacheContent;
import io.netty.handler.codec.memcache.LastMemcacheContent;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheRequest;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheResponse;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheResponseStatus;
import io.netty.handler.codec.memcache.binary.DefaultBinaryMemcacheResponse;

import java.io.UnsupportedEncodingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemcacheUtils {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(MemcacheUtils.class);

	public static ResponseSender returnFailure(BinaryMemcacheRequest request, short errorCode, String message) {
		return returnFailure(request.opcode(), request.opaque(), errorCode, message);
	}
	
	public static ResponseSender returnFailure(byte opcode, int opaque, short errorCode, String message) {
		return (ctx) -> {
			BinaryMemcacheResponse response = new DefaultBinaryMemcacheResponse();
			response.setStatus(errorCode);
			response.setOpaque(opaque);
			response.setOpcode(opcode);
			response.setTotalBodyLength(message.length());
			ctx.write(response);
			LastMemcacheContent content;
			try {
				content = new DefaultLastMemcacheContent(Unpooled.wrappedBuffer(message.getBytes("US-ASCII")));
				ctx.writeAndFlush(content);
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException(e);
			}
			return false;
		};
	}
	
	public static ResponseSender returnSuccess(byte opcode, int opaque, long cas, String message) {
		return (ctx) -> {
    		String realMessage = message == null ? "" : message;
    		BinaryMemcacheResponse response = new DefaultBinaryMemcacheResponse();
    		response.setStatus(BinaryMemcacheResponseStatus.SUCCESS);
    		response.setOpcode(opcode);
    		response.setOpaque(opaque);
    		response.setTotalBodyLength(realMessage.length());
    		response.setCas(cas);
    		if(realMessage.length() == 0) {
    			ctx.writeAndFlush(response);
    		} else {
        		ctx.write(response);
        		LastMemcacheContent content;
        		try {
        			content = new DefaultLastMemcacheContent(Unpooled.wrappedBuffer(realMessage.getBytes("US-ASCII")));
        			ctx.writeAndFlush(content);
        		} catch (UnsupportedEncodingException e) {
        			throw new RuntimeException(e);
        		}
    		}
    		return false;
		};
	}

	public static void logRequest(BinaryMemcacheRequest request) {
		if(LOGGER.isDebugEnabled()) {
    		LOGGER.debug("Opcode: "+request.opcode());
    		LOGGER.debug("Key Length: "+request.keyLength());
    		LOGGER.debug("Key: "+request.key());
    		LOGGER.debug("CAS: "+request.cas());
    		LOGGER.debug("Magic: "+request.magic());
    		LOGGER.debug("Reserved: "+request.reserved());
    		LOGGER.debug("Opaque: "+request.opaque());
    		LOGGER.debug("Extras Length: "+request.extrasLength());
    		LOGGER.debug("Body Length: "+request.totalBodyLength());
		}
	}
	
	public static String extractSaslUsername(byte[] auth) {
		StringBuilder builder = new StringBuilder();
		for(int i = 1;auth[i] != 0;i++) {
			builder.append((char)auth[i]);
		}
		return builder.toString();
	}
	
	public static String extractSaslPassword(byte[] auth) {
		StringBuilder builder = new StringBuilder();
		int passwordIndex = 1;
		while(auth[passwordIndex] != 0) {
			passwordIndex++;
		}
		passwordIndex++;
		for(int i = passwordIndex;i < auth.length;i++) {
			builder.append((char)auth[i]);
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
            
            long k =  ((long)data[i8]&0xff) +(((long)data[i8+1]&0xff)<<8)
                    +(((long)data[i8+2]&0xff)<<16) +(((long)data[i8+3]&0xff)<<24)
                    +(((long)data[i8+4]&0xff)<<32) +(((long)data[i8+5]&0xff)<<40)
                    +(((long)data[i8+6]&0xff)<<48) +(((long)data[i8+7]&0xff)<<56);
            
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
