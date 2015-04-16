package cloudfoundry.memcache;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.memcache.MemcacheContent;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheRequest;

public interface MemcacheMsgHandler {
	public byte getOpcode();
	public int getOpaque();
	public boolean get(ChannelHandlerContext ctx, BinaryMemcacheRequest request);
	public boolean getK(ChannelHandlerContext ctx, BinaryMemcacheRequest request);
	public boolean set(ChannelHandlerContext ctx, BinaryMemcacheRequest request);
	public boolean set(ChannelHandlerContext ctx, MemcacheContent content);
	public boolean add(ChannelHandlerContext ctx, BinaryMemcacheRequest request);
	public boolean add(ChannelHandlerContext ctx, MemcacheContent content);
	public boolean replace(ChannelHandlerContext ctx, BinaryMemcacheRequest request);
	public boolean replace(ChannelHandlerContext ctx, MemcacheContent content);
	public boolean delete(ChannelHandlerContext ctx, BinaryMemcacheRequest request);
	public boolean increment(ChannelHandlerContext ctx, BinaryMemcacheRequest request);
	public boolean decrement(ChannelHandlerContext ctx, BinaryMemcacheRequest request);
	public boolean quit(ChannelHandlerContext ctx, BinaryMemcacheRequest request);
	public boolean flush(ChannelHandlerContext ctx, BinaryMemcacheRequest request);
	public boolean noop(ChannelHandlerContext ctx, BinaryMemcacheRequest request);
	public boolean version(ChannelHandlerContext ctx, BinaryMemcacheRequest request);
	public boolean append(ChannelHandlerContext ctx, BinaryMemcacheRequest request);
	public boolean append(ChannelHandlerContext ctx, MemcacheContent content);
	public boolean prepend(ChannelHandlerContext ctx, BinaryMemcacheRequest request);
	public boolean prepend(ChannelHandlerContext ctx, MemcacheContent content);
	public boolean stat(ChannelHandlerContext ctx, BinaryMemcacheRequest request);
	public boolean touch(ChannelHandlerContext ctx, BinaryMemcacheRequest request);
	public boolean gat(ChannelHandlerContext ctx, BinaryMemcacheRequest request);
}
