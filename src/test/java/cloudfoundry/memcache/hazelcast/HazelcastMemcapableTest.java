package cloudfoundry.memcache.hazelcast;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.ServerSocket;

import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import cloudfoundry.memcache.MemcacheMsgHandlerFactory;
import cloudfoundry.memcache.MemcacheServer;
import cloudfoundry.memcache.StubAuthMsgHandlerFactory;

import com.hazelcast.config.Config;

public class HazelcastMemcapableTest {

	MemcacheServer server;
	int localPort;

	@BeforeClass
	public void setup() throws Exception {
		System.getProperties().put("io.netty.leakDetectionLevel", "paranoid");
		MemcacheMsgHandlerFactory factory = new HazelcastMemcacheMsgHandlerFactory(new Config(), 1, 1, 16, 536870912, 20, 10000, 271, 3, -1, -1, 5, 5, 300);

		localPort = 54913;
		try (ServerSocket s = new ServerSocket(0)) {
			localPort = s.getLocalPort();
		}

		System.out.println("Localport: " + localPort);
		server = new MemcacheServer(factory, localPort, new StubAuthMsgHandlerFactory(), 1000);
		server.start();
		Thread.sleep(1000);
	}

	@AfterClass
	public void after() throws Exception {
		server.shutdown();
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
