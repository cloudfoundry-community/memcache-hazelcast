package cloudfoundry.memcache.hazelcast;

import cloudfoundry.memcache.MemcacheMsgHandlerFactory;
import cloudfoundry.memcache.MemcacheServer;
import cloudfoundry.memcache.MemcacheStats;
import cloudfoundry.memcache.StubAuthMsgHandlerFactory;
import java.net.ServerSocket;
import java.util.Collections;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class HazelcastMemcapableTest {

	static HazelcastMemcacheMsgHandlerFactory factory;
	static MemcacheServer server;
	static int localPort;

	@BeforeClass
	public static void setup() throws Exception {
		System.getProperties().put("io.netty.leakDetectionLevel", "paranoid");

		localPort = 54913;
		try (ServerSocket s = new ServerSocket(0)) {
			localPort = s.getLocalPort();
		}

		MemcacheHazelcastConfig appConfig = new MemcacheHazelcastConfig();
		appConfig.getHazelcast().getMachines().put("local", Collections.singletonList("127.0.0.1"));
		appConfig.getMemcache().setMaxValueSize(10485760);
		factory = new HazelcastMemcacheMsgHandlerFactory(appConfig);

		System.out.println("Localport: " + localPort);
		server = new MemcacheServer(factory, localPort, new StubAuthMsgHandlerFactory(), 1000, 1000, new MemcacheStats());


		while(!factory.status().equals(MemcacheMsgHandlerFactory.OK_STATUS)) {
			Thread.sleep(1000);
		}
	}

	@AfterClass
	public static void after() throws Exception {
		server.close();
		factory.close();
	}

	@Test
	public void runMemcapable() throws Exception {
		ProcessBuilder pb = new ProcessBuilder("memcapable", "-p", Integer.toString(localPort), "-b", "-v", "-t", "1000");
		pb.redirectErrorStream(true);
		pb.inheritIO();
		Process process = pb.start();
		if(process.waitFor() != 0) {
			Assert.fail("Memcapable test failed.  See Log.");
		}
	}
}
