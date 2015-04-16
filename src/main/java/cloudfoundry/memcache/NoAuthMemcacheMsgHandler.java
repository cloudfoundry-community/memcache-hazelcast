package cloudfoundry.memcache;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.memcache.MemcacheContent;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheRequest;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheResponseStatus;

public class NoAuthMemcacheMsgHandler implements MemcacheMsgHandler {

	private final byte opcode;
	private final int opaque;

	public NoAuthMemcacheMsgHandler(BinaryMemcacheRequest request) {
		this.opcode = request.opcode();
		this.opaque = request.opaque();
	}

	@Override
	public int getOpaque() {
		return opaque;
	}
	
	@Override
	public byte getOpcode() {
		return opcode;
	}
	
	@Override
	public boolean get(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.AUTH_ERROR, "Connection must be authenticated to use this command.").send(ctx);
	}

	@Override
	public boolean getK(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.AUTH_ERROR, "Connection must be authenticated to use this command.").send(ctx);
	}

	@Override
	public boolean set(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.AUTH_ERROR, "Connection must be authenticated to use this command.").send(ctx);
	}

	@Override
	public boolean set(ChannelHandlerContext ctx, MemcacheContent content) {
		return false;
	}

	@Override
	public boolean add(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.AUTH_ERROR, "Connection must be authenticated to use this command.").send(ctx);
	}

	@Override
	public boolean add(ChannelHandlerContext ctx, MemcacheContent content) {
		return false;
	}

	@Override
	public boolean replace(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.AUTH_ERROR, "Connection must be authenticated to use this command.").send(ctx);
	}

	@Override
	public boolean replace(ChannelHandlerContext ctx, MemcacheContent content) {
		return false;
	}

	@Override
	public boolean delete(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.AUTH_ERROR, "Connection must be authenticated to use this command.").send(ctx);
	}

	@Override
	public boolean increment(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.AUTH_ERROR, "Connection must be authenticated to use this command.").send(ctx);
	}

	@Override
	public boolean decrement(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.AUTH_ERROR, "Connection must be authenticated to use this command.").send(ctx);
	}

	@Override
	public boolean quit(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.AUTH_ERROR, "Connection must be authenticated to use this command.").send(ctx);
	}

	@Override
	public boolean flush(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.AUTH_ERROR, "Connection must be authenticated to use this command.").send(ctx);
	}

	@Override
	public boolean noop(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.AUTH_ERROR, "Connection must be authenticated to use this command.").send(ctx);
	}

	@Override
	public boolean version(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.AUTH_ERROR, "Connection must be authenticated to use this command.").send(ctx);
	}

	@Override
	public boolean append(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.AUTH_ERROR, "Connection must be authenticated to use this command.").send(ctx);
	}

	@Override
	public boolean append(ChannelHandlerContext ctx, MemcacheContent content) {
		return false;
	}

	@Override
	public boolean prepend(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.AUTH_ERROR, "Connection must be authenticated to use this command.").send(ctx);
	}

	@Override
	public boolean prepend(ChannelHandlerContext ctx, MemcacheContent content) {
		return false;
	}

	@Override
	public boolean stat(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.AUTH_ERROR, "Connection must be authenticated to use this command.").send(ctx);
	}

	@Override
	public boolean touch(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.AUTH_ERROR, "Connection must be authenticated to use this command.").send(ctx);
	}

	@Override
	public boolean gat(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.AUTH_ERROR, "Connection must be authenticated to use this command.").send(ctx);
	}
}
