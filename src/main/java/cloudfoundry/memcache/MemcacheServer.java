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
	private volatile boolean running = false;
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
		workerGroup = new NioEventLoopGroup();

		started = true;

		ServerBootstrap b = new ServerBootstrap();
		b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
				.childHandler(new ChannelInitializer<SocketChannel>() {
					@Override
					protected void initChannel(SocketChannel ch) throws Exception {
						ch.pipeline().addFirst("memcache", new BinaryMemcacheServerCodec());
						ch.pipeline().addLast("memcache-handler", new MemcacheInboundHandlerAdapter(msgHandlerFactory, authMsgHandlerFactory.createAuthMsgHandler()));
						ch.pipeline().addBefore("memcache-handler", "memcache-order", new MemcacheOrderingDuplexHandler());
					}
				})
				.childOption(ChannelOption.TCP_NODELAY, true)
				.childOption(ChannelOption.SO_KEEPALIVE, true);

		new Thread(new Runnable() {
			public void run() {
				try {
					LOGGER.info("Waiting for the Memcache Backend to be ready to accept connections.");
					while (!msgHandlerFactory.isReady() && !Thread.interrupted()) {
						LOGGER.warn("Memcache backend not ready.  Waiting 5 sec.");
						Thread.sleep(5000);
					}
					if (msgHandlerFactory.isReady()) {
						b.bind(port).sync();
						LOGGER.info("Memcached server started on port: "+port);
						running = true;
					} else {
						LOGGER.error("Memcache server never got ready.  Terminating process.");
						System.exit(1);
						return;
					}
				} catch (Throwable e) {
					LOGGER.error("Memcache server never got ready.  Terminating process.", e);
					System.exit(1);
				}
			}
		}).start();
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
		running = false;
	}

	public boolean isRunning() {
		return running;
	}
	

}
