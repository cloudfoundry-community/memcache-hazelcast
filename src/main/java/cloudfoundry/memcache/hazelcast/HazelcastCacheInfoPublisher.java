package cloudfoundry.memcache.hazelcast;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.IMap;
import com.hazelcast.monitor.LocalMapStats;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class HazelcastCacheInfoPublisher {
	private final HazelcastMemcacheMsgHandlerFactory hazelcastMsgFactory;

	@Autowired
	public HazelcastCacheInfoPublisher(HazelcastMemcacheMsgHandlerFactory hazelcastMsgFactory) {
		this.hazelcastMsgFactory = hazelcastMsgFactory;
	}

	@Scheduled(fixedRate = 900000, initialDelay = 900000)
	public void publishMetrics() {
		for (DistributedObject object : hazelcastMsgFactory.getInstance().getDistributedObjects()) {
			if (object instanceof IMap) {
				IMap<?, ?> map = (IMap<?, ?>) object;
				LocalMapStats stats = map.getLocalMapStats();
				System.out.println("{\"time\":\"" + DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now())
						+ "\",\"cache\":\"" + map.getName() + "\",\"stats\":" + stats.toJson() + "}");
			}
		}
	}
}