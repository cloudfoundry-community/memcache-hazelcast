package cloudfoundry.memcache.hazelcast;

import java.util.Random;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.ConnectionFactoryBuilder.Protocol;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.auth.AuthDescriptor;
import net.spy.memcached.auth.PlainCallbackHandler;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import cloudfoundry.memcache.MemcacheMsgHandlerFactory;
import cloudfoundry.memcache.MemcacheServer;

import com.hazelcast.config.Config;


public class HazelcastMemcacheSpyTest {

	MemcachedClient c;
	MemcacheServer server;

	@BeforeClass
	public void setup() throws Exception {
		System.getProperties().put("io.netty.leakDetectionLevel", "paranoid");
		MemcacheMsgHandlerFactory factory = new HazelcastMemcacheMsgHandlerFactory(new Config());
		
		
		int localPort = 54913;
/*		try (ServerSocket s = new ServerSocket(0)) {
			localPort = s.getLocalPort();
		}
*/
		System.out.println("Localport: "+localPort);
		server = new MemcacheServer(factory, localPort);
		server.start();

		ConnectionFactoryBuilder binaryConnectionFactory = new ConnectionFactoryBuilder();
		binaryConnectionFactory.setProtocol(Protocol.BINARY);
		binaryConnectionFactory.setAuthDescriptor(new AuthDescriptor(null, new PlainCallbackHandler("someUser", "somePassword")));
		binaryConnectionFactory.setShouldOptimize(false);
		binaryConnectionFactory.setAuthWaitTime(10000000);
		binaryConnectionFactory.setOpTimeout(10000000);

		 c = new net.spy.memcached.MemcachedClient(binaryConnectionFactory.build(), AddrUtil.getAddresses("127.0.0.1:"+localPort));
//		 c = new net.spy.memcached.MemcachedClient(binaryConnectionFactory.build(), AddrUtil.getAddresses("127.0.0.1:11211"));
		 
	}
	
	@AfterClass
	public void after() throws Exception {
    	//Thread.sleep(1000000);
		c.shutdown();
		server.shutdown();
	}

	@Test
	public void getBasic() throws Exception {
		Assert.assertNull(c.get("nothingHere"));
		c.set("someKey", 0, "Some Data!");
		Assert.assertEquals(c.get("someKey"), "Some Data!");
	}

	@Test
	public void setBasic() throws Exception {
		c.set("someKey", 0, "Some Data!").get();
		Assert.assertEquals(c.get("someKey"), "Some Data!");
	}
	
	@Test
	public void setWithExpiration() throws Exception {
		long time = System.currentTimeMillis();
		long expireTime = time+1000;
		c.set("someKey", 1, "Some Data!").get();
		Object value = c.get("someKey");
		Assert.assertNotNull(value);
		while(System.currentTimeMillis() < expireTime+10000 && value != null) {
			value = c.get("someKey");
			System.out.println("Got Value");
			Thread.sleep(250);
		}
		Assert.assertNull(value);
	}

	@Test
	public void setWithTooSoonExpiration() throws Exception {
		long time = System.currentTimeMillis();
		c.set("someKey", (int)((time/1000)-100000), "Some Data!").get();
		Assert.assertNull(c.get("someKey"));
	}

	@Test
	public void setValueTooBig() throws Exception {
		byte[] data = new byte[20004857];
		new Random().nextBytes(data);
		try {
			c.set("BigValue", 0, data).get();
			Assert.fail();
		} catch(Exception e) {
			//success
		}
	}
}
