package cloudfoundry.memcache;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.memcache.MemcacheContent;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheRequest;

import java.util.UUID;
import java.util.concurrent.Future;

public interface AuthMsgHandler {
	Future<?> listMechs(ChannelHandlerContext ctx, BinaryMemcacheRequest request);
	Future<?> startAuth(ChannelHandlerContext ctx, BinaryMemcacheRequest request);
	Future<?> startAuth(ChannelHandlerContext ctx, MemcacheContent content);
	boolean isAuthenticated();
	String getUsername();
	UUID getBindGuid();
	String getCacheName();
}
