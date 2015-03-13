package cloudfoundry.memcache;

import io.netty.channel.ChannelHandlerContext;

public interface ResponseSender {
	boolean send(ChannelHandlerContext ctx);
}
