package cloudfoundry.memcache.hazelcast;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
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
import cloudfoundry.memcache.StubAuthMsgHandlerFactory;

import com.hazelcast.config.Config;


public class HazelcastMemcacheXMemcachedTest {

	MemcachedClient c;
	MemcacheServer server;

	@BeforeClass
	public void setup() throws Exception {
		System.getProperties().put("io.netty.leakDetectionLevel", "paranoid");
		MemcacheMsgHandlerFactory factory = new HazelcastMemcacheMsgHandlerFactory(new Config(), 1, 1, 16, 536870912, 20, 10000);
		
		
		int localPort = 54913;
		try (ServerSocket s = new ServerSocket(0)) {
			localPort = s.getLocalPort();
		}

		System.out.println("Localport: "+localPort);
		server = new MemcacheServer(factory, localPort, new StubAuthMsgHandlerFactory(), 100);
		server.start();

		String[] servers =
			{
//			  "localhost:"+localPort,
			  "localhost:11211",
			};

		List<InetSocketAddress> addresses = AddrUtil.getAddresses("localhost:"+localPort);
		XMemcachedClientBuilder builder = new XMemcachedClientBuilder("localhost:"+localPort);
		Map<InetSocketAddress, net.rubyeye.xmemcached.auth.AuthInfo> authInfo = new HashMap<>();
		authInfo.put(addresses.get(0), net.rubyeye.xmemcached.auth.AuthInfo.plain("user", "password"));
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
		server.shutdown();
	}

	@Test
	public void incBasic() throws Exception {
		c.delete("nothingHereGet");
		Assert.assertNull(c.get("nothingHereGet"));
		Assert.assertEquals(c.incr("nothingHereGet", 1, 2), 2);
		Assert.assertEquals(c.incr("nothingHereGet", 1), 3);
	}
}
