package cloudfoundry.memcache;

import org.slf4j.MDC;

public final class LogUtils {
	private static final String CHANNEL_ID_MDC_KEY = "channel";
	
	private LogUtils() {
	}

	public static void setupChannelMdc(String channelId) {
		MDC.put(CHANNEL_ID_MDC_KEY, channelId);
	}
	public static void cleanupChannelMdc() {
		MDC.remove(CHANNEL_ID_MDC_KEY);
	}

}
