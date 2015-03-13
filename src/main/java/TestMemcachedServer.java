import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.memcache.DefaultLastMemcacheContent;
import io.netty.handler.codec.memcache.LastMemcacheContent;
import io.netty.handler.codec.memcache.MemcacheContent;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheMessage;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheOpcodes;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheRequest;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheResponse;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheResponseStatus;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheServerCodec;
import io.netty.handler.codec.memcache.binary.DefaultBinaryMemcacheResponse;
import io.netty.util.ReferenceCountUtil;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TestMemcachedServer {
	static final int PORT = Integer.parseInt(System.getProperty("port", "8007"));

	static Map<String, byte[]> data = new ConcurrentHashMap<>();
	static Map<String, byte[]> extras = new ConcurrentHashMap<>();

	
	public static void main(String[] args) throws Exception {
		// Configure the server.
		EventLoopGroup bossGroup = new NioEventLoopGroup(1);
		EventLoopGroup workerGroup = new NioEventLoopGroup();
		try {
			ServerBootstrap b = new ServerBootstrap();
			b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
					.option(ChannelOption.SO_BACKLOG, 100)
					.option(ChannelOption.SO_KEEPALIVE, true)
					.childHandler(new ChannelInitializer<SocketChannel>() {
						@Override
						protected void initChannel(SocketChannel ch) throws Exception {
							ch.pipeline().addFirst(new BinaryMemcacheServerCodec());
							ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
								byte optcode;
								String key;
								ByteBuf readBytes;
								int index;
								@Override
								public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
									try {
										if (msg instanceof MemcacheContent) {
											MemcacheContent mc = (MemcacheContent) msg;
//											System.out.println(mc.content().toString(Charset.forName("UTF-8")));
											if (optcode == BinaryMemcacheOpcodes.SET) {
												mc.content().readBytes(readBytes, mc.content().readableBytes());
												index += mc.content().readableBytes();
												if (mc instanceof LastMemcacheContent) {
													byte[] dataBytes = new byte[readBytes.readableBytes()];
													readBytes.readBytes(dataBytes);
													data.put(key, dataBytes);
													readBytes.release();
													BinaryMemcacheResponse response = new DefaultBinaryMemcacheResponse();
													response.setStatus(BinaryMemcacheResponseStatus.SUCCESS);
													response.setOpcode(BinaryMemcacheOpcodes.SET);
													response.setCas(1l);
													ctx.writeAndFlush(response);
												}
											} else
											if (optcode == BinaryMemcacheOpcodes.GET) {
												BinaryMemcacheResponse response = new DefaultBinaryMemcacheResponse();
												response.setStatus(BinaryMemcacheResponseStatus.SUCCESS);
												response.setOpcode(BinaryMemcacheOpcodes.GET);
												response.setCas(1l);
												response.setExtrasLength((byte)4);
												ByteBuf buf = ctx.alloc().buffer(4);
												buf.writeBytes(extras.get(key));
												response.setExtras(buf);
												response.setTotalBodyLength(4 + data.get(key).length);
												ctx.write(response);
												ByteBuf bufData = ctx.alloc().buffer(data.get(key).length);
												bufData.writeBytes(data.get(key));
												LastMemcacheContent content = new DefaultLastMemcacheContent(bufData);
												ctx.writeAndFlush(content);
											} else
											if (optcode == BinaryMemcacheOpcodes.GETK) {
												BinaryMemcacheResponse response = new DefaultBinaryMemcacheResponse();
												response.setStatus(BinaryMemcacheResponseStatus.SUCCESS);
												response.setOpcode(BinaryMemcacheOpcodes.GETK);
												response.setCas(1l);
												response.setExtrasLength((byte)4);
												response.setKey(key);
												response.setKeyLength((short)key.length());
												ByteBuf buf = ctx.alloc().buffer(4);
												buf.writeBytes(extras.get(key));
												response.setExtras(buf);
												response.setTotalBodyLength(key.length()+4 + data.get(key).length);
												ctx.write(response);
												ByteBuf bufData = ctx.alloc().buffer(data.get(key).length);
												bufData.writeBytes(data.get(key));
												LastMemcacheContent content = new DefaultLastMemcacheContent(bufData);
												ctx.writeAndFlush(content);

											}
											if (optcode == BinaryMemcacheOpcodes.FLUSH) {
												data.clear();
												BinaryMemcacheResponse response = new DefaultBinaryMemcacheResponse();
												response.setStatus(BinaryMemcacheResponseStatus.SUCCESS);
												response.setOpcode(BinaryMemcacheOpcodes.FLUSH);
												response.setCas(1l);
												response.setTotalBodyLength(0);
												ctx.write(response);
												LastMemcacheContent content = new DefaultLastMemcacheContent(Unpooled.EMPTY_BUFFER);
												ctx.writeAndFlush(content);
											}

										} else if (msg instanceof BinaryMemcacheRequest) {
											BinaryMemcacheMessage mc = (BinaryMemcacheMessage) msg;
											optcode = mc.opcode();
											key = mc.key();
//											System.out.println("Optcode: " + mc.opcode());
//											System.out.println("Key: " + mc.key());
//											System.out.println("Key Length: " + mc.keyLength());
//											System.out.println("Extra Length: " + mc.extrasLength());
											if (mc.opcode() == BinaryMemcacheOpcodes.SET) {
												byte[] flags = new byte[4];
												byte[] exp = new byte[4];
												if (mc.extrasLength() > 0) {
													mc.extras().readBytes(flags);
													mc.extras().readBytes(exp);
												}
												extras.put(mc.key(), flags);
											}
											readBytes = ctx.alloc().buffer(mc.totalBodyLength()-mc.extrasLength()-mc.keyLength());
//											System.out.println("Size: " + mc.totalBodyLength());
										}
									} finally {
										ReferenceCountUtil.release(msg); // (2)
									}

								}

								@Override
								public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
//									System.out.println("Read Completed");
								}

								@Override
								public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
									// Close the connection when an exception is
									// raised.
									cause.printStackTrace();
									ctx.close();
								}
							});

						}
					});

			// Start the server.
			ChannelFuture f = b.bind(PORT).sync();

			// Wait until the server socket is closed.
			f.channel().closeFuture().sync();
		} finally {
			// Shut down all event loops to terminate all threads.
			bossGroup.shutdownGracefully();
			workerGroup.shutdownGracefully();
		}
	}
}
