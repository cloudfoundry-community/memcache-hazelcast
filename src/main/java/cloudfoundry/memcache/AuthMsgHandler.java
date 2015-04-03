package cloudfoundry.memcache;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.memcache.MemcacheContent;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheRequest;

import java.util.UUID;

public interface AuthMsgHandler {
	boolean listMechs(ChannelHandlerContext ctx, BinaryMemcacheRequest request);
	boolean startAuth(ChannelHandlerContext ctx, BinaryMemcacheRequest request);
	boolean startAuth(ChannelHandlerContext ctx, MemcacheContent content);
	boolean isAuthenticated();
	String getUsername();
	UUID getAppGuid();
	String getCacheName();
}
