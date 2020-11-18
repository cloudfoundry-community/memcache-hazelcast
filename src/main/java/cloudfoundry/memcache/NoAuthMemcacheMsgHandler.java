package cloudfoundry.memcache;

import java.util.concurrent.Future;

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
	public Future<?> get(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.AUTH_ERROR, true, "Connection must be authenticated to use this command.").send(ctx);
	}

	@Override
	public Future<?> getK(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.AUTH_ERROR, true, "Connection must be authenticated to use this command.").send(ctx);
	}

	@Override
	public Future<?> set(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.AUTH_ERROR, true, "Connection must be authenticated to use this command.").send(ctx);
	}

	@Override
	public Future<?> set(ChannelHandlerContext ctx, MemcacheContent content) {
		return CompletedFuture.INSTANCE;
	}

	@Override
	public Future<?> add(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.AUTH_ERROR, true, "Connection must be authenticated to use this command.").send(ctx);
	}

	@Override
	public Future<?> add(ChannelHandlerContext ctx, MemcacheContent content) {
		return CompletedFuture.INSTANCE;
	}

	@Override
	public Future<?> replace(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.AUTH_ERROR, true, "Connection must be authenticated to use this command.").send(ctx);
	}

	@Override
	public Future<?> replace(ChannelHandlerContext ctx, MemcacheContent content) {
		return CompletedFuture.INSTANCE;
	}

	@Override
	public Future<?> delete(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.AUTH_ERROR, true, "Connection must be authenticated to use this command.").send(ctx);
	}

	@Override
	public Future<?> increment(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.AUTH_ERROR, true, "Connection must be authenticated to use this command.").send(ctx);
	}

	@Override
	public Future<?> decrement(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.AUTH_ERROR, true, "Connection must be authenticated to use this command.").send(ctx);
	}

	@Override
	public Future<?> quit(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.AUTH_ERROR, true, "Connection must be authenticated to use this command.").send(ctx);
	}

	@Override
	public Future<?> flush(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.AUTH_ERROR, true, "Connection must be authenticated to use this command.").send(ctx);
	}

	@Override
	public Future<?> noop(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.AUTH_ERROR, true, "Connection must be authenticated to use this command.").send(ctx);
	}

	@Override
	public Future<?> version(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.AUTH_ERROR, true, "Connection must be authenticated to use this command.").send(ctx);
	}

	@Override
	public Future<?> append(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.AUTH_ERROR, true, "Connection must be authenticated to use this command.").send(ctx);
	}

	@Override
	public Future<?> append(ChannelHandlerContext ctx, MemcacheContent content) {
		return CompletedFuture.INSTANCE;
	}

	@Override
	public Future<?> prepend(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.AUTH_ERROR, true, "Connection must be authenticated to use this command.").send(ctx);
	}

	@Override
	public Future<?> prepend(ChannelHandlerContext ctx, MemcacheContent content) {
		return CompletedFuture.INSTANCE;
	}

	@Override
	public Future<?> stat(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.AUTH_ERROR, true, "Connection must be authenticated to use this command.").send(ctx);
	}

	@Override
	public Future<?> touch(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.AUTH_ERROR, true, "Connection must be authenticated to use this command.").send(ctx);
	}

	@Override
	public Future<?> gat(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
		return MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.AUTH_ERROR, true, "Connection must be authenticated to use this command.").send(ctx);
	}
}
