package cloudfoundry.memcache;

import java.util.concurrent.ScheduledExecutorService;

import io.netty.handler.codec.memcache.binary.BinaryMemcacheRequest;

public interface MemcacheMsgHandlerFactory {
	public static final String OK_STATUS = "OK";
	public MemcacheMsgHandler createMsgHandler(BinaryMemcacheRequest request, AuthMsgHandler authMsgHandler, String channelId);
	public void deleteCache(String name);
	public ScheduledExecutorService getScheduledExecutorService();
	public String status();
	boolean isCacheValid(String cacheName);
}
