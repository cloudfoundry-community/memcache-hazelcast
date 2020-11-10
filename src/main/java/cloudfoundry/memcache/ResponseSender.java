package cloudfoundry.memcache;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;

public interface ResponseSender {
	ChannelFuture send(ChannelHandlerContext ctx);
}
