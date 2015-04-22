package cloudfoundry.memcache;

import io.netty.handler.codec.memcache.binary.BinaryMemcacheRequest;

import com.hazelcast.core.HazelcastInstance;

public interface MemcacheMsgHandlerFactory {
	public MemcacheMsgHandler createMsgHandler(BinaryMemcacheRequest request, AuthMsgHandler authMsgHandler);
	public void deleteCache(String name);
	public boolean isReady();
	public HazelcastInstance getInstance();
}
