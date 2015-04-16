package cloudfoundry.memcache;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.ConnectionFactoryBuilder.Protocol;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.auth.AuthDescriptor;
import net.spy.memcached.auth.PlainCallbackHandler;

public class MemcacheClient {

	public static void main(String[] args) throws Exception {
		ConnectionFactoryBuilder binaryConnectionFactory = new ConnectionFactoryBuilder();
		binaryConnectionFactory.setProtocol(Protocol.BINARY);
		binaryConnectionFactory.setAuthDescriptor(new AuthDescriptor(null, new PlainCallbackHandler("small|123", "5u3/Ov+W83dPbtqo9bFELkBtM0+57F6ibuNGp54m3bAXPlK7Pya0OYdC+DI5rtFL")));
		binaryConnectionFactory.setShouldOptimize(false);
		binaryConnectionFactory.setAuthWaitTime(10000000);
		binaryConnectionFactory.setOpTimeout(10000000);

		MemcachedClient c = new net.spy.memcached.MemcachedClient(binaryConnectionFactory.build(), AddrUtil.getAddresses("127.0.0.1:"+54913));
		
		c.set("dude", 0, "bob");
		System.out.println(c.get("dude"));
		c.shutdown();
	}
}
