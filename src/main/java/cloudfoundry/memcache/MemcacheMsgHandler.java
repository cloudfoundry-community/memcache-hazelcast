package cloudfoundry.memcache;

import java.util.concurrent.Future;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.memcache.MemcacheContent;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheRequest;

public interface MemcacheMsgHandler {
	public byte getOpcode();
	public int getOpaque();
	public Future<?> get(ChannelHandlerContext ctx, BinaryMemcacheRequest request);
	public Future<?> getK(ChannelHandlerContext ctx, BinaryMemcacheRequest request);
	public Future<?> set(ChannelHandlerContext ctx, BinaryMemcacheRequest request);
	public Future<?> set(ChannelHandlerContext ctx, MemcacheContent content);
	public Future<?> add(ChannelHandlerContext ctx, BinaryMemcacheRequest request);
	public Future<?> add(ChannelHandlerContext ctx, MemcacheContent content);
	public Future<?> replace(ChannelHandlerContext ctx, BinaryMemcacheRequest request);
	public Future<?> replace(ChannelHandlerContext ctx, MemcacheContent content);
	public Future<?> delete(ChannelHandlerContext ctx, BinaryMemcacheRequest request);
	public Future<?> increment(ChannelHandlerContext ctx, BinaryMemcacheRequest request);
	public Future<?> decrement(ChannelHandlerContext ctx, BinaryMemcacheRequest request);
	public Future<?> quit(ChannelHandlerContext ctx, BinaryMemcacheRequest request);
	public Future<?> flush(ChannelHandlerContext ctx, BinaryMemcacheRequest request);
	public Future<?> noop(ChannelHandlerContext ctx, BinaryMemcacheRequest request);
	public Future<?> version(ChannelHandlerContext ctx, BinaryMemcacheRequest request);
	public Future<?> append(ChannelHandlerContext ctx, BinaryMemcacheRequest request);
	public Future<?> append(ChannelHandlerContext ctx, MemcacheContent content);
	public Future<?> prepend(ChannelHandlerContext ctx, BinaryMemcacheRequest request);
	public Future<?> prepend(ChannelHandlerContext ctx, MemcacheContent content);
	public Future<?> stat(ChannelHandlerContext ctx, BinaryMemcacheRequest request);
	public Future<?> touch(ChannelHandlerContext ctx, BinaryMemcacheRequest request);
	public Future<?> gat(ChannelHandlerContext ctx, BinaryMemcacheRequest request);
}
