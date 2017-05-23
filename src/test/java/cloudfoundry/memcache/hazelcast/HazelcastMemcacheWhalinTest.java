package cloudfoundry.memcache.hazelcast;

import java.net.ServerSocket;
import java.util.Collections;

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
import com.schooner.MemCached.AuthInfo;
import com.schooner.MemCached.SchoonerSockIOPool;
import com.whalin.MemCached.MemCachedClient;


public class HazelcastMemcacheWhalinTest {

	MemCachedClient c;
	MemcacheMsgHandlerFactory factory;
	SchoonerSockIOPool pool;

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
			  "localhost:"+localPort,
			};

		// grab an instance of our connection pool
		pool = SchoonerSockIOPool.getInstance(AuthInfo.plain("test", "test"));

		// set the servers and the weights
		pool.setServers( servers );

		// set some basic pool settings
		// 5 initial, 5 min, and 250 max conns
		// and set the max idle time for a conn
		// to 6 hours
		pool.setInitConn( 5 );
		pool.setMinConn( 5 );
		pool.setMaxConn( 250 );
		pool.setMaxIdle( 1000 * 60 * 60 * 6 );

		// set the sleep for the maint thread
		// it will wake up every x seconds and
		// maintain the pool size
		pool.setMaintSleep( 30 );

		// set some TCP settings
		// disable nagle
		// set the read timeout to 3 secs
		// and don't set a connect timeout
		pool.setNagle( false );
		pool.setSocketTO( 3000 );
		pool.setSocketConnectTO( 0 );
		
		// initialize the connection pool
		pool.initialize();

		//For some reason Whalin insists on going even though it doesn't have a connection.
		Thread.sleep(5000);
		c = new MemCachedClient(true, true);
	}
	
	@AfterClass
	public void after() throws Exception {
		pool.shutDown();
		factory.shutdown();
	}

	@Test
	public void getBasic() throws Exception {
		Assert.assertNull(c.get("nothingHereGet"));
		c.set("nothingHereGet", "Some Data!");
		Assert.assertEquals(c.get("nothingHereGet"), "Some Data!");
	}
}
