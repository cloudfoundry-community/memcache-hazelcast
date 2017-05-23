package cloudfoundry.memcache.hazelcast;

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

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import cloudfoundry.memcache.MemcacheMsgHandlerFactory;
import cloudfoundry.memcache.MemcacheServer;
import cloudfoundry.memcache.MemcacheStats;
import cloudfoundry.memcache.SecretKeyAuthMsgHandlerFactory;
import cloudfoundry.memcache.StubAuthMsgHandlerFactory;

import com.hazelcast.config.Config;


public class HazelcastMemcacheXMemcachedTest {

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
		MemcacheServer server = new MemcacheServer(localPort, new SecretKeyAuthMsgHandlerFactory("key", "test", "test", "test"), 100, 1000, new MemcacheStats());

		MemcacheHazelcastConfig appConfig = new MemcacheHazelcastConfig();
		appConfig.getHazelcast().getMachines().put("local", Collections.singletonList("127.0.0.1"));
		factory = new HazelcastMemcacheMsgHandlerFactory(server, appConfig);
		
		while(!factory.status().equals(MemcacheMsgHandlerFactory.OK_STATUS)) {
			Thread.sleep(1000);
		}

		String[] servers =
			{
//			  "localhost:"+localPort,
			  "localhost:11211",
			};

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
	public void after() throws Exception {
		c.shutdown();
		factory.shutdown();
	}

	@Test
	public void incBasic() throws Exception {
		c.delete("nothingHereGet");
		Assert.assertNull(c.get("nothingHereGet"));
		Assert.assertEquals(c.incr("nothingHereGet", 1, 2), 2);
		Assert.assertEquals(c.incr("nothingHereGet", 1), 3);
	}
}
