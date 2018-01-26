package cloudfoundry.memcache;

public class SecretKeyAuthMsgHandlerFactory implements AuthMsgHandlerFactory {

	private final String key;
	private final String testUser;
	private final String testPassword;
	private final String testCacheName;

	
	public SecretKeyAuthMsgHandlerFactory(String key, String testUser, String testPassword, String testCacheName) {
		if(key == null) {
			throw new IllegalArgumentException("null key");
		}
		this.key = key;
		this.testUser = testUser;
		this.testPassword = testPassword;
		this.testCacheName = testCacheName;
	}

	@Override
	public AuthMsgHandler createAuthMsgHandler(MemcacheMsgHandlerFactory factory) {
		return new SecretKeyAuthMsgHandler(factory, key, testUser, testPassword, testCacheName);
	}
}
