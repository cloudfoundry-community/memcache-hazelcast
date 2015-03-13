package cloudfoundry.memcache;

public interface MemcacheMsgHandlerFactory {
	public MemcacheMsgHandler createMsgHandler(AuthMsgHandler authMsgHandler);
}
