package cloudfoundry.memcache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheServerCodec;
import io.netty.handler.timeout.IdleStateHandler;

public class MemcacheServer implements AutoCloseable {
	private static final Logger LOGGER = LoggerFactory.getLogger(MemcacheServer.class);

	private final EventLoopGroup bossGroup;
	private final EventLoopGroup workerGroup;

	public MemcacheServer(MemcacheMsgHandlerFactory msgHandlerFactory, int port,
			AuthMsgHandlerFactory authMsgHandlerFactory, int queueSizeLimit, int requestRateLimit,
			MemcacheStats memcacheStats) throws InterruptedException {
		bossGroup = new NioEventLoopGroup(1);
		try {
			workerGroup = new NioEventLoopGroup();
		} catch (Exception e) {
			bossGroup.shutdownGracefully();
			throw e;
		}
		try {
			new ServerBootstrap().group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
					.childHandler(new ChannelInitializer<SocketChannel>() {
						@Override
						protected void initChannel(SocketChannel ch) throws Exception {
							ch.pipeline().addFirst("memcache", new BinaryMemcacheServerCodec());
							ch.pipeline().addLast(new IdleStateHandler(60, 10, 0));
							ch.pipeline().addLast("memcache-handler",
									new MemcacheInboundHandlerAdapter(ch.id().asShortText(), msgHandlerFactory,
											authMsgHandlerFactory.createAuthMsgHandler(msgHandlerFactory),
											queueSizeLimit, requestRateLimit, memcacheStats));
						}
					}).childOption(ChannelOption.TCP_NODELAY, true).childOption(ChannelOption.SO_KEEPALIVE, true)
					.childOption(ChannelOption.AUTO_READ, false).bind(port).sync();
		} catch (Exception e) {
			bossGroup.shutdownGracefully();
			workerGroup.shutdownGracefully();
			throw e;
		}
		LOGGER.info("Memcached server started on port: {}", port);
	}

	@Override
	public void close() {
		LOGGER.info("Shutting down memcache server.");
		LOGGER.info("Shutting down boss thread group.");
		bossGroup.shutdownGracefully().awaitUninterruptibly();
		LOGGER.info("Shutting down worker thread group.");
		workerGroup.shutdownGracefully().awaitUninterruptibly();
	}
}
