package cloudfoundry.memcache;

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
	public void shutdownNow() {
	}

	@Override
	public void shutdown() {
	}

	@Override
	public boolean isCacheValid(String cacheName) {
		return valid;
	}

	@Override
	public void deleteCache(String name) {
	}

	@Override
	public MemcacheMsgHandler createMsgHandler(BinaryMemcacheRequest request, AuthMsgHandler authMsgHandler) {
		return null;
	}
}