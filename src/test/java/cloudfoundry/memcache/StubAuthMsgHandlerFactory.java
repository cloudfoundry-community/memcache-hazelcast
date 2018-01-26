package cloudfoundry.memcache;

public class StubAuthMsgHandlerFactory implements AuthMsgHandlerFactory {

	@Override
	public AuthMsgHandler createAuthMsgHandler(MemcacheMsgHandlerFactory factory) {
		return new StubAuthMsgHandler();
	}
}
