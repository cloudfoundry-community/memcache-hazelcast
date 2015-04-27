package cloudfoundry.memcache.hazelcast;

import java.net.ServerSocket;
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
import cloudfoundry.memcache.StubAuthMsgHandlerFactory;

import com.hazelcast.config.Config;


public class HazelcastMemcacheSpyTest {

	MemcachedClient c;
	MemcacheServer server;

	@BeforeClass
	public void setup() throws Exception {
		System.getProperties().put("io.netty.leakDetectionLevel", "paranoid");
		MemcacheMsgHandlerFactory factory = new HazelcastMemcacheMsgHandlerFactory(new Config(), 1, 1, 16, 536870912, 20, 10000, 271);
		
		
		int localPort = 54913;
		try (ServerSocket s = new ServerSocket(0)) {
			localPort = s.getLocalPort();
		}

		System.out.println("Localport: "+localPort);
		server = new MemcacheServer(factory, localPort, new StubAuthMsgHandlerFactory());
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
		c.shutdown();
		server.shutdown();
	}

	@Test
	public void getBasic() throws Exception {
		Assert.assertNull(c.get("nothingHereGet"));
		c.set("nothingHereGet", 0, "Some Data!");
		Assert.assertEquals(c.get("nothingHereGet"), "Some Data!");
	}

	@Test
	public void setBasic() throws Exception {
		c.set("someKey", 0, "Some Data Set!").get();
		Assert.assertEquals(c.get("someKey"), "Some Data Set!");
	}
	
	@Test
	public void setWithExpiration() throws Exception {
		c.set("someKey", 1, "Some Data!").get();
		Object value = c.get("someKey");
		Assert.assertNotNull(value);
		Thread.sleep(2000);
		Assert.assertNull(c.get("someKey"));
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

	@Test
	public void touchBasic() throws Exception {
		Assert.assertNull(c.get("nothingHere"));
		c.set("nothingHere", 4, "Some Data!");
		Thread.sleep(3000);
		c.touch("nothingHere", 4);
		Thread.sleep(3000);
		Assert.assertEquals(c.get("nothingHere"), "Some Data!");
		Thread.sleep(3000);
		Assert.assertNull(c.get("nothingHere"));
	}

	@Test
	public void gatBasic() throws Exception {
		Assert.assertNull(c.get("nothingHere2"));
		c.set("nothingHere2", 4, "Some Data!");
		Thread.sleep(3000);
		Assert.assertEquals(c.getAndTouch("nothingHere2", 4).getValue(), "Some Data!");
		Thread.sleep(3000);
		Assert.assertEquals(c.get("nothingHere2"), "Some Data!");
		Thread.sleep(3000);
		Assert.assertNull(c.get("nothingHere2"));
	}

	@Test
	public void incBasic() throws Exception {
		Assert.assertEquals(c.incr("nothingHere2", 1, 1), 1);
		Assert.assertEquals(c.incr("nothingHere2", 1, 1), 2);
	}
}
