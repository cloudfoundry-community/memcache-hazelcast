package cloudfoundry.memcache.hazelcast;

import java.net.ServerSocket;
import java.util.Collections;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.hazelcast.config.Config;

import cloudfoundry.memcache.MemcacheMsgHandlerFactory;
import cloudfoundry.memcache.MemcacheServer;
import cloudfoundry.memcache.MemcacheStats;
import cloudfoundry.memcache.StubAuthMsgHandlerFactory;

public class HazelcastMemcapableTest {

	MemcacheMsgHandlerFactory factory;
	int localPort;

	@BeforeClass
	public void setup() throws Exception {
		System.getProperties().put("io.netty.leakDetectionLevel", "paranoid");

		localPort = 54913;
		try (ServerSocket s = new ServerSocket(0)) {
			localPort = s.getLocalPort();
		}

		System.out.println("Localport: " + localPort);
		MemcacheServer server = new MemcacheServer(localPort, new StubAuthMsgHandlerFactory(), 1000, new MemcacheStats());

		MemcacheHazelcastConfig appConfig = new MemcacheHazelcastConfig();
		appConfig.getHazelcast().getMachines().put("local", Collections.singletonList("127.0.0.1"));
		factory = new HazelcastMemcacheMsgHandlerFactory(server, appConfig);

		while(!factory.status().equals(MemcacheMsgHandlerFactory.OK_STATUS)) {
			Thread.sleep(1000);
		}
	}

	@AfterClass
	public void after() throws Exception {
		factory.shutdown();
	}

	@Test
	public void runMemcapable() throws Exception {
		ProcessBuilder pb = new ProcessBuilder("memcapable", "-h", "127.0.0.1", "-p", Integer.toString(localPort), "-b", "-v", "-t", "1000");
		pb.redirectErrorStream(true);
		pb.inheritIO();
		Process process = pb.start();
		if(process.waitFor() != 0) {
			Assert.fail("Memcapable test failed.  See Log.");
		}
	}
}
