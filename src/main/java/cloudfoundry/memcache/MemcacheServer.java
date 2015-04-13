package cloudfoundry.memcache;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheServerCodec;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemcacheServer {
	private static final Logger LOGGER = LoggerFactory.getLogger(MemcacheServer.class);
	
	private final MemcacheMsgHandlerFactory msgHandlerFactory;
	private EventLoopGroup bossGroup;
	private EventLoopGroup workerGroup;
	private boolean started = false;
	private final int port;
	private final AuthMsgHandlerFactory authMsgHandlerFactory;

	public MemcacheServer(MemcacheMsgHandlerFactory msgHandlerFactory, int port, AuthMsgHandlerFactory authMsgHandlerFactory) {
		this.msgHandlerFactory = msgHandlerFactory;
		this.port = port;
		this.authMsgHandlerFactory = authMsgHandlerFactory;
	}

	@PostConstruct
	public void start() {
		bossGroup = new NioEventLoopGroup(1);
		workerGroup = new NioEventLoopGroup(100);

		started = true;

		ServerBootstrap b = new ServerBootstrap();
		b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
				.childHandler(new ChannelInitializer<SocketChannel>() {
					@Override
					protected void initChannel(SocketChannel ch) throws Exception {
						ch.pipeline().addFirst(new BinaryMemcacheServerCodec());
						ch.pipeline().addLast(new MemcacheChannelInboundHandlerAdapter(msgHandlerFactory, authMsgHandlerFactory.createAuthMsgHandler()));
					}
				})
				.childOption(ChannelOption.TCP_NODELAY, true);

		try {
			// Start the server.
			b.bind(port).sync();
		} catch (InterruptedException e) {
			throw new IllegalStateException("Failed to start memcache server on port: "+port, e);
		}
	}

	@PreDestroy
	public void shutdown() {
		LOGGER.info("Shutting down memcache server.");
		if (started) {
			LOGGER.info("Shutting down boss thread group.");
			bossGroup.shutdownGracefully().awaitUninterruptibly();
			LOGGER.info("Shutting down worker thread group.");
			workerGroup.shutdownGracefully().awaitUninterruptibly();
		}
	}
}
