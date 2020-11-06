package cloudfoundry.memcache.hazelcast;

import cloudfoundry.memcache.MemcacheMsgHandlerFactory;
import cloudfoundry.memcache.MemcacheServer;
import cloudfoundry.memcache.MemcacheStats;
import cloudfoundry.memcache.SecretKeyAuthMsgHandlerFactory;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.rubyeye.xmemcached.MemcachedClient;
import net.rubyeye.xmemcached.XMemcachedClientBuilder;
import net.rubyeye.xmemcached.command.BinaryCommandFactory;
import net.rubyeye.xmemcached.utils.AddrUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;


public class HazelcastMemcacheXMemcachedTest {

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
		server = new MemcacheServer(factory, localPort, new SecretKeyAuthMsgHandlerFactory("key", "test", "test", "test"), 100, 1000, new MemcacheStats());

		
		while(!factory.status().equals(MemcacheMsgHandlerFactory.OK_STATUS)) {
			Thread.sleep(1000);
		}

		List<InetSocketAddress> addresses = AddrUtil.getAddresses("localhost:"+localPort);
		XMemcachedClientBuilder builder = new XMemcachedClientBuilder("localhost:"+localPort);
		Map<InetSocketAddress, net.rubyeye.xmemcached.auth.AuthInfo> authInfo = new HashMap<>();
		authInfo.put(addresses.get(0), net.rubyeye.xmemcached.auth.AuthInfo.plain("test", "test"));
		builder.setAuthInfoMap(authInfo);
		builder.setCommandFactory(new BinaryCommandFactory());
		builder.setConnectTimeout(10000000);
		builder.setOpTimeout(100000000);
		builder.setConnectionPoolSize(8);
		c = builder.build();
	}
	
	@AfterClass
	public static void after() throws Exception {
		c.shutdown();
		server.close();
		factory.close();
	}

	@Test
	public void incBasic() throws Exception {
		c.delete("nothingHereGet");
		Assert.assertNull(c.get("nothingHereGet"));
		Assert.assertEquals(c.incr("nothingHereGet", 1, 2), 2);
		Assert.assertEquals(c.incr("nothingHereGet", 1), 3);
	}

}
