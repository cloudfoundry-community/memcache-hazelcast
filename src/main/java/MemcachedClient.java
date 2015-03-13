import com.whalin.MemCached.MemCachedClient;
import com.whalin.MemCached.SockIOPool;

public class MemcachedClient {

	public static void main(String[] args) throws Exception {
		 MemCachedClient c = new MemCachedClient(true, true);
		 SockIOPool pool = SockIOPool.getInstance();
		 pool.setServers(new String[] {"127.0.0.1:8007"});
		 pool.initialize();
//		ConnectionFactoryBuilder binaryConnectionFactory = new ConnectionFactoryBuilder();
//		binaryConnectionFactory.setProtocol(Protocol.BINARY);
//		binaryConnectionFactory.setShouldOptimize(false);
//
//		net.spy.memcached.MemcachedClient c = new net.spy.memcached.MemcachedClient(binaryConnectionFactory.build(), AddrUtil.getAddresses("127.0.0.1:8007"));
		try {
			c.set("someKey", "Some Data!");
			System.out.println("Sent Some Data!");
			Object myObject = c.get("someKey");
			System.out.println("Got " + myObject);
		} finally {
//			c.shutdown();
		}
	}

}
