package cloudfoundry.memcache;

import java.util.List;

public interface MemcacheMsgHandlerFactory {
	public MemcacheMsgHandler createMsgHandler(AuthMsgHandler authMsgHandler);
	public List<String> getCaches();
	public void createCache(String name);
	public void deleteCache(String name);
}
