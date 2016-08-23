package cloudfoundry.memcache;

import java.util.concurrent.Future;

import io.netty.channel.ChannelHandlerContext;

public interface ResponseSender {
	Future<?> send(ChannelHandlerContext ctx);
}
