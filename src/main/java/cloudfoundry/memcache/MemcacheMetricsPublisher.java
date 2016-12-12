package cloudfoundry.memcache;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import cf.dropsonde.metron.MetronClient;

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
		for(Map.Entry<String, Long> hit : memcacheStats.getHitStats().entrySet()) {
			totalHits += hit.getValue();
			if(hit.getValue() > 0) {
				metronClient.emitValueMetric("memcache.hits."+hit.getKey(), hit.getValue(), "count");
			}
		}
		metronClient.emitValueMetric("memcache.hits.total", totalHits, "count");
	}
}