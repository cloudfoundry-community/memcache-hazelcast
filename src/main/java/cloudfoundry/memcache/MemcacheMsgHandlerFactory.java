package cloudfoundry.memcache;

import io.netty.handler.codec.memcache.binary.BinaryMemcacheRequest;

public interface MemcacheMsgHandlerFactory {
	public MemcacheMsgHandler createMsgHandler(BinaryMemcacheRequest request, AuthMsgHandler authMsgHandler);
	public void deleteCache(String name);
	public boolean isReady();
	public boolean isRunning();
}
