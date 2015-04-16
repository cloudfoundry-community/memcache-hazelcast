package cloudfoundry.memcache;

public class StubAuthMsgHandlerFactory implements AuthMsgHandlerFactory {

	@Override
	public AuthMsgHandler createAuthMsgHandler() {
		return new StubAuthMsgHandler();
	}
}
