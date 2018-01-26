package cloudfoundry.memcache;

public interface AuthMsgHandlerFactory {

	public abstract AuthMsgHandler createAuthMsgHandler(MemcacheMsgHandlerFactory msgHandlerFactory);

}