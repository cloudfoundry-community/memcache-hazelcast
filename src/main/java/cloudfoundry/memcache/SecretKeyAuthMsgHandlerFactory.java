package cloudfoundry.memcache;

public class SecretKeyAuthMsgHandlerFactory implements AuthMsgHandlerFactory {

	private final String key;
	private final String testUser;
	private final String testPassword;
	private final String testCacheName;
	private final MemcacheMsgHandlerFactory factory;

	
	public SecretKeyAuthMsgHandlerFactory(MemcacheMsgHandlerFactory factory, String key, String testUser, String testPassword, String testCacheName) {
		if(key == null) {
			throw new IllegalArgumentException("null key");
		}
		this.key = key;
		this.testUser = testUser;
		this.testPassword = testPassword;
		this.testCacheName = testCacheName;
		this.factory = factory;

	}

	@Override
	public AuthMsgHandler createAuthMsgHandler() {
		return new SecretKeyAuthMsgHandler(factory, key, testUser, testPassword, testCacheName);
	}
}
