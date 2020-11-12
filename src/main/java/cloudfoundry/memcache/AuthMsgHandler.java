package cloudfoundry.memcache;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.memcache.MemcacheContent;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheRequest;
import java.util.UUID;

public interface AuthMsgHandler {
	ChannelFuture listMechs(ChannelHandlerContext ctx, BinaryMemcacheRequest request);

	ChannelFuture startAuth(ChannelHandlerContext ctx, BinaryMemcacheRequest request);

	ChannelFuture startAuth(ChannelHandlerContext ctx, MemcacheContent content);

	boolean isAuthenticated();

	String getUsername();

	UUID getBindGuid();

	String getCacheName();
}
