package cloudfoundry.memcache.hazelcast;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import cloudfoundry.memcache.MemcacheMsgHandlerFactory;
import cloudfoundry.memcache.MemcacheServer;
import cloudfoundry.memcache.MemcacheStats;
import cloudfoundry.memcache.SecretKeyAuthMsgHandlerFactory;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.ConnectionFactoryBuilder.Protocol;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.auth.AuthDescriptor;
import net.spy.memcached.auth.PlainCallbackHandler;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.ops.StatusCode;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class HazelcastMemcacheSpyTest {

	static MemcachedClient c;
	static HazelcastMemcacheMsgHandlerFactory factory;
	static MemcacheServer server;

	@BeforeClass
	public static void setup() throws Exception {
		System.getProperties().put("io.netty.leakDetectionLevel", "paranoid");
		int localPort = 54913;
		try (ServerSocket s = new ServerSocket(0)) {
			localPort = s.getLocalPort();
		}

		MemcacheHazelcastConfig appConfig = new MemcacheHazelcastConfig();
		appConfig.getHazelcast().getMachines().put("local", Collections.singletonList("127.0.0.1"));
		appConfig.getMemcache().setMaxValueSize(10485760);
		factory = new HazelcastMemcacheMsgHandlerFactory(appConfig);

		System.out.println("Localport: "+localPort);
		server = new MemcacheServer(factory, localPort, new SecretKeyAuthMsgHandlerFactory("key", "test", "test", "test"), 1000, 1000, new MemcacheStats());


		while(!factory.status().equals(MemcacheMsgHandlerFactory.OK_STATUS)) {
			Thread.sleep(1000);
		}

		ConnectionFactoryBuilder binaryConnectionFactory = new ConnectionFactoryBuilder();
		binaryConnectionFactory.setProtocol(Protocol.BINARY);
		binaryConnectionFactory.setAuthDescriptor(new AuthDescriptor(null, new PlainCallbackHandler("test", "test")));
		binaryConnectionFactory.setShouldOptimize(true);
		binaryConnectionFactory.setAuthWaitTime(10000000);
		binaryConnectionFactory.setOpTimeout(10000000);
		
		c = new net.spy.memcached.MemcachedClient(binaryConnectionFactory.build(), AddrUtil.getAddresses("127.0.0.1:"+localPort));
	}
	
	@AfterClass
	public static void after() throws Exception {
		c.shutdown();
		server.close();
		factory.close();
	}

	@Test
	public void getBasic() throws Exception {
		c.delete("nothingHereGet").get();
		Assert.assertNull(c.get("nothingHereGet"));
		c.set("nothingHereGet", 0, "Some Data!").get();
		Assert.assertEquals(c.get("nothingHereGet"), "Some Data!");
	}

	@Test
	public void setBasic() throws Exception {
		c.set("someKey", 0, "Some Data Set!").get();
		Assert.assertEquals(c.get("someKey"), "Some Data Set!");
	}

//	@Test
//	public void backpressuretest() throws Exception {
//		c.set("someKey", 0, "Some Data Set!").get();
//		List<GetFuture<Object>> futures = new ArrayList<>();
//		for(int loop = 0; loop < 10000; loop++) {
//			System.out.println("loop index: "+loop);
//			futures.add(c.asyncGet("someKey"+loop));
//		}
//		
//		for(GetFuture<Object> future : futures) {
//			future.get();
//			System.out.println("Got response");
//		}
//		System.out.println("Test all done!");
//	}

	@Test
	public void ratelimittest() throws Exception {
		c.set("someKey", 0, "Some Data Set!").get();
		long startTime = System.currentTimeMillis();
		for(int loop = 0; loop < 2000; loop++) {
			System.out.println("loop index: "+loop);
			c.get("someKey"+loop);
		}
		long finishTime = System.currentTimeMillis();
		
		if(finishTime - startTime < 10000) {
			Assert.fail("Rate limit test finished faster than 5 seconds.  Rate limiting didn't appear to work.");
		}
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
		c.set("someKey", (int)((time/1000)-1000), "Some Data!").get();
		Assert.assertNull(c.get("someKey"));
	}

	@Test
	public void setValueReallyBig() throws Exception {
		byte[] data = new byte[10485760];
		new Random().nextBytes(data);
		OperationFuture<Boolean> setResult = c.set("BigValue", 0, data);
		assertTrue(setResult.get());
	}

	@Test
	public void setKeyReallyBig() throws Exception {
		String randomAlphabetic = RandomStringUtils.randomAlphabetic(250);
		OperationFuture<Boolean> setResult = c.set(randomAlphabetic, 0, "Groovy Value");
		assertTrue(setResult.get());
		assertEquals("Groovy Value", c.get(randomAlphabetic));
	}

	@Test
	public void setValueTooBig() throws Exception {
		byte[] data = new byte[10485761];
		new Random().nextBytes(data);
		try {
			OperationFuture<Boolean> setResult = c.set("BigValue", 0, data);
			if(setResult.get()) {
				System.out.print("We got a response!");
				assertEquals(StatusCode.ERR_2BIG, setResult.getStatus().getStatusCode());
			}
		} catch(Exception e) {
			//success
		}
	}

	@Test
	public void touchBasic() throws Exception {
		Assert.assertNull(c.get("nothingHere"));
		c.set("nothingHere", 4, "Some Data!").get();
		Thread.sleep(3000);
		c.touch("nothingHere", 4).get();
		Thread.sleep(3000);
		Assert.assertEquals(c.get("nothingHere"), "Some Data!");
		Thread.sleep(3000);
		Assert.assertNull(c.get("nothingHere"));
	}

	@Test
	public void gatBasic() throws Exception {
		c.delete("nothingHere2").get();
		Assert.assertNull(c.get("nothingHere2"));
		c.set("nothingHere2", 4, "Some Data!").get();
		Thread.sleep(3000);
		Assert.assertEquals(c.getAndTouch("nothingHere2", 4).getValue(), "Some Data!");
		Thread.sleep(3000);
		Assert.assertEquals(c.get("nothingHere2"), "Some Data!");
		Thread.sleep(3000);
		Assert.assertNull(c.get("nothingHere2"));
	}

	@Test
	public void incBasic() throws Exception {
		c.delete("nothingHere2").get();
		Assert.assertEquals(c.incr("nothingHere2", 1, 1), 1);
		Assert.assertEquals(c.incr("nothingHere2", 1, 1), 2);
		Assert.assertEquals(c.get("nothingHere2"), "2");
	}

	@Test
	public void statBasic() throws Exception {
		boolean containedAnEntry = false;
		Map<SocketAddress, Map<String, String>> stats = c.getStats();
		for(Map.Entry<SocketAddress, Map<String, String>> addressEntry : stats.entrySet()) {
			System.out.println("Address: "+addressEntry.getKey());
			for(Map.Entry<String, String> stat : addressEntry.getValue().entrySet()) {
				containedAnEntry = true;
				System.out.println(stat.getKey()+":"+stat.getValue());
			}
		}
		Assert.assertTrue(containedAnEntry);
	}
}
