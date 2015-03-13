package cloudfoundry.memcache;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.memcache.MemcacheContent;
import io.netty.handler.codec.memcache.MemcacheObject;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheOpcodes;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheRequest;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheResponseStatus;
import io.netty.util.ReferenceCountUtil;

public class MemcacheChannelInboundHandlerAdapter extends ChannelInboundHandlerAdapter {

	private MemcacheMsgHandlerFactory msgHandlerFactory;

	private byte optcode = -1;
	private MemcacheMsgHandler currentMsgHandler;
	private AuthMsgHandler authMsgHandler;
	private boolean authRequired;

	public MemcacheChannelInboundHandlerAdapter(MemcacheMsgHandlerFactory msgHandlerFactory, boolean authRequired) {
		super();
		this.msgHandlerFactory = msgHandlerFactory;
		this.authRequired = authRequired;
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		try {
			if (!(msg instanceof MemcacheObject)) {
				return;
			}
			if (msg instanceof BinaryMemcacheRequest) {
				BinaryMemcacheRequest request = (BinaryMemcacheRequest) msg;
				optcode = request.opcode();
				if(currentMsgHandler == null) {
					if(getAuthMsgHandler().isAuthenticated()) {
						currentMsgHandler = msgHandlerFactory.createMsgHandler(getAuthMsgHandler());
					} else {
						currentMsgHandler = new NoAuthMemcacheMsgHandler();
					}
				}
			} else if(currentMsgHandler == null) {
				return;
			}
			
			BinaryMemcacheRequest request = null;

			switch (optcode) {
			case BinaryMemcacheOpcodes.GET:
			case BinaryMemcacheOpcodes.GETQ:
				if(!currentMsgHandler.get(ctx, (BinaryMemcacheRequest)msg)) {
					clearRequest();
				}
				break;
			case BinaryMemcacheOpcodes.GETK:
			case BinaryMemcacheOpcodes.GETKQ:
				if(!currentMsgHandler.getK(ctx, (BinaryMemcacheRequest)msg)) {
					clearRequest();
				}
				break;
			case BinaryMemcacheOpcodes.SET:
			case BinaryMemcacheOpcodes.SETQ:
				if(!(msg instanceof BinaryMemcacheRequest ? currentMsgHandler.set(ctx, (BinaryMemcacheRequest)msg) : currentMsgHandler.set(ctx, (MemcacheContent)msg))) {
					clearRequest();
				}
				break;
			case BinaryMemcacheOpcodes.ADD:
			case BinaryMemcacheOpcodes.ADDQ:
				if(!(msg instanceof BinaryMemcacheRequest ? currentMsgHandler.add(ctx, (BinaryMemcacheRequest)msg) : currentMsgHandler.add(ctx, (MemcacheContent)msg))) {
					clearRequest();
				}
				break;
			case BinaryMemcacheOpcodes.REPLACE:
			case BinaryMemcacheOpcodes.REPLACEQ:
				if(!(msg instanceof BinaryMemcacheRequest ? currentMsgHandler.replace(ctx, (BinaryMemcacheRequest)msg) : currentMsgHandler.replace(ctx, (MemcacheContent)msg))) {
					clearRequest();
				}
				break;
			case BinaryMemcacheOpcodes.DELETE:
			case BinaryMemcacheOpcodes.DELETEQ:
				if(!currentMsgHandler.delete(ctx, (BinaryMemcacheRequest)msg)) {
					clearRequest();
				}
				break;
			case BinaryMemcacheOpcodes.INCREMENT:
			case BinaryMemcacheOpcodes.INCREMENTQ:
				if(!currentMsgHandler.increment(ctx, (BinaryMemcacheRequest)msg)) {
					clearRequest();
				}
				break;
			case BinaryMemcacheOpcodes.DECREMENT:
			case BinaryMemcacheOpcodes.DECREMENTQ:
				if(!currentMsgHandler.decrement(ctx, (BinaryMemcacheRequest)msg)) {
					clearRequest();
				}
				break;
			case BinaryMemcacheOpcodes.QUIT:
			case BinaryMemcacheOpcodes.QUITQ:
				if(!currentMsgHandler.quit(ctx, (BinaryMemcacheRequest)msg)) {
					clearRequest();
				}
				break;
			case BinaryMemcacheOpcodes.FLUSH:
			case BinaryMemcacheOpcodes.FLUSHQ:
				if(!currentMsgHandler.flush(ctx, (BinaryMemcacheRequest)msg)) {
					clearRequest();
				}
				break;
			case BinaryMemcacheOpcodes.NOOP:
				if(!currentMsgHandler.noop(ctx, (BinaryMemcacheRequest)msg)) {
					clearRequest();
				}
				break;
			case BinaryMemcacheOpcodes.VERSION:
				if(!currentMsgHandler.version(ctx, (BinaryMemcacheRequest)msg)) {
					clearRequest();
				}
				break;
			case BinaryMemcacheOpcodes.APPEND:
				System.out.println("Failed to handle request with optcode: "+optcode);
				break;
			case BinaryMemcacheOpcodes.APPENDQ:
				System.out.println("Failed to handle request with optcode: "+optcode);
				break;
			case BinaryMemcacheOpcodes.PREPEND:
				System.out.println("Failed to handle request with optcode: "+optcode);
				break;
			case BinaryMemcacheOpcodes.PREPENDQ:
				System.out.println("Failed to handle request with optcode: "+optcode);
				break;
			case BinaryMemcacheOpcodes.STAT:
				if(!currentMsgHandler.stat(ctx, (BinaryMemcacheRequest)msg)) {
					clearRequest();
				}
				break;
			case BinaryMemcacheOpcodes.TOUCH:
				System.out.println("Failed to handle request with optcode: "+optcode);
				break;
			case BinaryMemcacheOpcodes.GAT:
				System.out.println("Failed to handle request with optcode: "+optcode);
				break;
			case BinaryMemcacheOpcodes.GATQ:
				System.out.println("Failed to handle request with optcode: "+optcode);
				break;
			case BinaryMemcacheOpcodes.SASL_LIST_MECHS:
				if(!getAuthMsgHandler().listMechs(ctx, (BinaryMemcacheRequest)msg)) {
					clearRequest();
				}
				break;
			case BinaryMemcacheOpcodes.SASL_AUTH:
				boolean result = false;
				if(msg instanceof BinaryMemcacheRequest) {
					result = getAuthMsgHandler().startAuth(ctx, (BinaryMemcacheRequest)msg);
				} else if(msg instanceof MemcacheContent) {
					result = getAuthMsgHandler().startAuth(ctx, (MemcacheContent)msg);
				}
				if(!result) {
					clearRequest();
				}
				break;
			case BinaryMemcacheOpcodes.SASL_STEP:
				MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.AUTH_ERROR, "We don't support any auth mechanisms that require a step.").send(ctx);
				break;
			default:
				System.out.println("Failed to handle request with optcode: "+optcode);
				MemcacheUtils.returnFailure(request, BinaryMemcacheResponseStatus.UNKNOWN_COMMAND, "Unable to handle command: 0x"+Integer.toHexString(optcode)).send(ctx);
			}
		} finally {
			ReferenceCountUtil.release(msg);
		}
	}
	
	private AuthMsgHandler getAuthMsgHandler() {
		if(authMsgHandler == null) {
			if(authRequired) {
				authMsgHandler = new PlainAuthMsgHandler();
			} else {
				authMsgHandler = new StubAuthMsgHandler("StubUser");
			}
		}
		return authMsgHandler;
	}
	
	private void clearRequest() {
		currentMsgHandler = null;
		optcode = -1;
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		// Close the connection when an exception is
		// raised.
		cause.printStackTrace();
		ctx.close();
	}
}
