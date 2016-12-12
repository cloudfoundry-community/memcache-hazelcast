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
import cloudfoundry.memcache.MemcacheStats;
import cloudfoundry.memcache.SecretKeyAuthMsgHandler;
import cloudfoundry.memcache.SecretKeyAuthMsgHandlerFactory;
import cloudfoundry.memcache.StubAuthMsgHandlerFactory;

import com.hazelcast.config.Config;


public class HazelcastMemcacheStatsTest {

	MemcachedClient c;
	MemcacheMsgHandlerFactory factory;
	private MemcacheStats memcacheStats;

	@BeforeClass
	public void setup() throws Exception {
		System.getProperties().put("io.netty.leakDetectionLevel", "paranoid");
		int localPort = 54913;
		try (ServerSocket s = new ServerSocket(0)) {
			localPort = s.getLocalPort();
		}

		System.out.println("Localport: "+localPort);
		memcacheStats = new MemcacheStats();
		MemcacheServer server = new MemcacheServer(localPort, new SecretKeyAuthMsgHandlerFactory("key", "test", "test", "test"), 100, memcacheStats);

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
	public void basic() throws Exception {
		c.set("dude", 0, "value").get();
		Assert.assertEquals(memcacheStats.getHitStats().get("set").longValue(), 1);
		c.get("dude");
		c.get("dude");
		Assert.assertEquals(memcacheStats.getHitStats().get("get").longValue(), 2);
		Assert.assertEquals(memcacheStats.getHitStats().get("sasl_auth").longValue(), 1);
	}
}