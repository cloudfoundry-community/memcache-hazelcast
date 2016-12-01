package cloudfoundry.memcache.hazelcast;

import static org.junit.Assert.assertEquals;

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
import net.spy.memcached.internal.OperationCompletionListener;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.ops.StatusCode;
import net.spy.memcached.transcoders.SerializingTranscoder;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import cloudfoundry.memcache.MemcacheMsgHandlerFactory;
import cloudfoundry.memcache.MemcacheServer;
import cloudfoundry.memcache.SecretKeyAuthMsgHandler;
import cloudfoundry.memcache.SecretKeyAuthMsgHandlerFactory;
import cloudfoundry.memcache.StubAuthMsgHandlerFactory;

import com.hazelcast.config.Config;


public class HazelcastMemcacheSpyTest {

	MemcachedClient c;
	MemcacheMsgHandlerFactory factory;

	@BeforeClass
	public void setup() throws Exception {
		System.getProperties().put("io.netty.leakDetectionLevel", "paranoid");
		int localPort = 54913;
		try (ServerSocket s = new ServerSocket(0)) {
			localPort = s.getLocalPort();
		}

		System.out.println("Localport: "+localPort);
		MemcacheServer server = new MemcacheServer(localPort, new SecretKeyAuthMsgHandlerFactory("key", "test", "test", "test"), 100);

		MemcacheHazelcastConfig appConfig = new MemcacheHazelcastConfig();
		appConfig.getHazelcast().getMachines().put("local", Collections.singletonList("127.0.0.1"));
		factory = new HazelcastMemcacheMsgHandlerFactory(server, appConfig);

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
	public void after() throws Exception {
		c.shutdown();
		factory.shutdown();
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
	public void setValueTooBig() throws Exception {
		byte[] data = new byte[20004857];
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
