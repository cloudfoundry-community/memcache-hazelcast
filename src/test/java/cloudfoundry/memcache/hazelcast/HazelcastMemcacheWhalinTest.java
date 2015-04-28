package cloudfoundry.memcache.hazelcast;

import java.net.ServerSocket;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import cloudfoundry.memcache.MemcacheMsgHandlerFactory;
import cloudfoundry.memcache.MemcacheServer;
import cloudfoundry.memcache.StubAuthMsgHandlerFactory;

import com.hazelcast.config.Config;
import com.schooner.MemCached.AuthInfo;
import com.schooner.MemCached.SchoonerSockIOPool;
import com.whalin.MemCached.MemCachedClient;


public class HazelcastMemcacheWhalinTest {

	MemCachedClient c;
	MemcacheServer server;
	SchoonerSockIOPool pool;

	@BeforeClass
	public void setup() throws Exception {
		System.getProperties().put("io.netty.leakDetectionLevel", "paranoid");
		MemcacheMsgHandlerFactory factory = new HazelcastMemcacheMsgHandlerFactory(new Config(), 1, 1, 16, 536870912, 20, 10000, 271, 3, -1, -1, 5, 5);
		
		
		int localPort = 54913;
		try (ServerSocket s = new ServerSocket(0)) {
			localPort = s.getLocalPort();
		}

		System.out.println("Localport: "+localPort);
		server = new MemcacheServer(factory, localPort, new StubAuthMsgHandlerFactory());
		server.start();

		String[] servers =
			{
			  "localhost:"+localPort,
			};

		// grab an instance of our connection pool
		pool = SchoonerSockIOPool.getInstance(AuthInfo.plain("username", "password"));

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
		
		c = new MemCachedClient(true, true);
	}
	
	@AfterClass
	public void after() throws Exception {
		pool.shutDown();
		server.shutdown();
	}

	@Test
	public void getBasic() throws Exception {
		Assert.assertNull(c.get("nothingHereGet"));
		c.set("nothingHereGet", "Some Data!");
		Assert.assertEquals(c.get("nothingHereGet"), "Some Data!");
	}
}
