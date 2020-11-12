package cloudfoundry.memcache;

import java.util.concurrent.ScheduledExecutorService;

import io.netty.handler.codec.memcache.binary.BinaryMemcacheRequest;

public class MemcacheMsgHandlerFactoryAuthStub implements MemcacheMsgHandlerFactory {
	private final boolean valid;

	public MemcacheMsgHandlerFactoryAuthStub(boolean valid) {
		this.valid = valid;
	}

	@Override
	public String status() {
		return null;
	}

	@Override
	public boolean isCacheValid(String cacheName) {
		return valid;
	}

	@Override
	public void deleteCache(String name) {
	}

	@Override
	public ScheduledExecutorService getScheduledExecutorService() {
		return null;
	}

	@Override
	public MemcacheMsgHandler createMsgHandler(BinaryMemcacheRequest request, AuthMsgHandler authMsgHandler, String channelId) {
		return null;
	}
}