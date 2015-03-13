package cloudfoundry.memcache;

import org.testng.annotations.Test;

public class MemcacheUtilsTest {
	
	@Test
	public void testUnsignedLong() {
		long value = Long.parseLong("21121212121212121221121212121212");
		System.out.println("Long is: "+value);
	}
}
