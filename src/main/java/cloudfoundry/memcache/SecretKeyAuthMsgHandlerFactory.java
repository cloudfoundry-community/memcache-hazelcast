package cloudfoundry.memcache;

public class SecretKeyAuthMsgHandlerFactory implements AuthMsgHandlerFactory {

	@Override
	public AuthMsgHandler createAuthMsgHandler() {
		return new PlainAuthMsgHandler();
	}
}
