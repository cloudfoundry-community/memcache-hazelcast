package cloudfoundry.memcache;

public class SecretKeyAuthMsgHandlerFactory implements AuthMsgHandlerFactory {

	private String key;
	
	public SecretKeyAuthMsgHandlerFactory(String key) {
		if(key == null) {
			throw new IllegalArgumentException("null key");
		}
		this.key = key;
	}

	@Override
	public AuthMsgHandler createAuthMsgHandler() {
		return new SecretKeyAuthMsgHandler(key);
	}
}
