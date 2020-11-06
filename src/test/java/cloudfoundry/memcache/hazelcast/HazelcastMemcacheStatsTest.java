package cloudfoundry.memcache.hazelcast;

import cloudfoundry.memcache.MemcacheMsgHandlerFactory;
import cloudfoundry.memcache.MemcacheServer;
import cloudfoundry.memcache.MemcacheStats;
import cloudfoundry.memcache.SecretKeyAuthMsgHandlerFactory;
import java.net.ServerSocket;
import java.util.Collections;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.ConnectionFactoryBuilder.Protocol;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.auth.AuthDescriptor;
import net.spy.memcached.auth.PlainCallbackHandler;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;


public class HazelcastMemcacheStatsTest {

	static MemcachedClient c;
	static HazelcastMemcacheMsgHandlerFactory factory;
	static MemcacheStats memcacheStats;
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
		memcacheStats = new MemcacheStats();
		server = new MemcacheServer(factory, localPort, new SecretKeyAuthMsgHandlerFactory("key", "test", "test", "test"), 100, 1000, memcacheStats);


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
	public void basic() throws Exception {
		c.set("dude", 0, "value").get();
		Assert.assertEquals(memcacheStats.getHitStats().get("set").longValue(), 1);
		c.get("dude");
		c.get("dude");
		Assert.assertEquals(memcacheStats.getHitStats().get("get").longValue(), 2);
		Assert.assertEquals(memcacheStats.getHitStats().get("sasl_auth").longValue(), 1);
	}
}
