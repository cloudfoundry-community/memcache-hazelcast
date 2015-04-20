package cloudfoundry.memcache;

import io.netty.handler.codec.memcache.binary.BinaryMemcacheRequest;

import java.util.List;

public interface MemcacheMsgHandlerFactory {
	public MemcacheMsgHandler createMsgHandler(BinaryMemcacheRequest request, AuthMsgHandler authMsgHandler);
	public List<String> getCaches();
	public void createCache(String name);
	public void deleteCache(String name);
	public boolean isReady();
}
