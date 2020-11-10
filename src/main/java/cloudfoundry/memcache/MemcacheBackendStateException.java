package cloudfoundry.memcache;

public class MemcacheBackendStateException extends IllegalStateException {

	private static final long serialVersionUID = -490870496025813835L;

	public MemcacheBackendStateException() {
		super();
	}

	public MemcacheBackendStateException(String message, Throwable cause) {
		super(message, cause);
	}

	public MemcacheBackendStateException(String s) {
		super(s);
	}

	public MemcacheBackendStateException(Throwable cause) {
		super(cause);
	}
}
