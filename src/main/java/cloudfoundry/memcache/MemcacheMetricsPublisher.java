package cloudfoundry.memcache;

import cf.dropsonde.metron.MetronClient;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class MemcacheMetricsPublisher {

	private final MetronClient metronClient;
	private final MemcacheStats memcacheStats;
	
	
	@Autowired
	public MemcacheMetricsPublisher(MemcacheStats memcacheStats, MetronClient metronClient) {
		super();
		this.metronClient = metronClient;
		this.memcacheStats = memcacheStats;
	}

	@Scheduled(fixedRateString="${hazelcast.metricsPublishInterval}", initialDelayString="${hazelcast.metricsPublishInterval}")
	public void publishMetrics() {
		long totalHits = 0;
		for(Map.Entry<String, LongAdder> hit : memcacheStats.getHitStats().entrySet()) {
			long sum = hit.getValue().sumThenReset();
			if(sum > 0) {
				totalHits += sum;
				metronClient.emitCounterEvent("memcache.hits."+hit.getKey(), sum);
			}
		}
		metronClient.emitCounterEvent("memcache.hits.total", totalHits);
	}
}