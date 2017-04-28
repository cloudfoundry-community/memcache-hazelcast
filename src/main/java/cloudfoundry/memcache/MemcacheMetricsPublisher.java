package cloudfoundry.memcache;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

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
		for(Map.Entry<String, AtomicLong> hit : memcacheStats.getHitStats().entrySet()) {
			if(hit.getValue().get() > 0) {
				long sum = hit.getValue().getAndSet(0);
				totalHits += sum;
				metronClient.emitCounterEvent("memcache.hits."+hit.getKey(), sum);
			}
		}
		metronClient.emitCounterEvent("memcache.hits.total", totalHits);
	}
}