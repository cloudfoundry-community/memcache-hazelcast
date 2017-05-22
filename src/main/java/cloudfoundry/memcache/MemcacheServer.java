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

public class MemcacheServer {
	private static final Logger LOGGER = LoggerFactory.getLogger(MemcacheServer.class);
	
	private EventLoopGroup bossGroup;
	private EventLoopGroup workerGroup;
	private boolean started = false;
	private final int port;
	private final AuthMsgHandlerFactory authMsgHandlerFactory;
	private final MemcacheStats memcacheStats;
	private int queueSize;

	public MemcacheServer(int port, AuthMsgHandlerFactory authMsgHandlerFactory, int queueSize, MemcacheStats memcacheStats) {
		this.port = port;
		this.authMsgHandlerFactory = authMsgHandlerFactory;
		this.queueSize = queueSize;
		this.memcacheStats = memcacheStats;
	}

	public void start(MemcacheMsgHandlerFactory msgHandlerFactory) {
		bossGroup = new NioEventLoopGroup(1);
		workerGroup = new NioEventLoopGroup();

		started = true;

		ServerBootstrap b = new ServerBootstrap();
		b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
				.childHandler(new ChannelInitializer<SocketChannel>() {
					@Override
					protected void initChannel(SocketChannel ch) throws Exception {
						ch.pipeline().addFirst("memcache", new BinaryMemcacheServerCodec());
						ch.pipeline().addLast("memcache-handler", new MemcacheInboundHandlerAdapter(msgHandlerFactory, authMsgHandlerFactory.createAuthMsgHandler(), queueSize, MemcacheServer.this, memcacheStats));
					}
				})
				.childOption(ChannelOption.TCP_NODELAY, true)
				.childOption(ChannelOption.SO_KEEPALIVE, true)
				.childOption(ChannelOption.AUTO_READ, false);

		try {
			b.bind(port).sync();
		} catch (InterruptedException e) {
			throw new RuntimeException("Binding to port interrupted.", e);
		}
		LOGGER.info("Memcached server started on port: "+port);
	}
	
	public void shutdown() {
		if (started) {
			LOGGER.info("Shutting down memcache server.");
			LOGGER.info("Shutting down boss thread group.");
			bossGroup.shutdownGracefully().awaitUninterruptibly();
			LOGGER.info("Shutting down worker thread group.");
			workerGroup.shutdownGracefully().awaitUninterruptibly();
		}
		started = false;
	}
}
