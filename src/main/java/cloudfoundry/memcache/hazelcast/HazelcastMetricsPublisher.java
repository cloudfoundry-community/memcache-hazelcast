package cloudfoundry.memcache.hazelcast;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.hazelcast.com.eclipsesource.json.JsonObject;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.memory.DefaultMemoryStats;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.monitor.NearCacheStats;

import cf.dropsonde.metron.MetronClient;

@Component
public class HazelcastMetricsPublisher {

	private final HazelcastMemcacheMsgHandlerFactory hazelcastMsgFactory;
	private final long maxSize;
	private final MetronClient metronClient;
	private volatile Map<String, Long> previousOperationsCounts;

	@Autowired
	public HazelcastMetricsPublisher(HazelcastMemcacheMsgHandlerFactory hazelcastMsgFactory, MetronClient metronClient, MemcacheHazelcastConfig config) {
		super();
		this.hazelcastMsgFactory = hazelcastMsgFactory;
		this.maxSize = config.getHazelcast().getMaxCacheSize();
		this.metronClient = metronClient;
		previousOperationsCounts = new HashMap<>();
	}

	@Scheduled(fixedRateString="${hazelcast.metricsPublishInterval}", initialDelayString="${hazelcast.metricsPublishInterval}")
	public void publishMetrics() {
		MemoryStats memoryStats = new DefaultMemoryStats();
		
		metronClient.emitValueMetric("hazelcast.total_physical_memory", memoryStats.getTotalPhysical(), "bytes");
		metronClient.emitValueMetric("hazelcast.free_physical_memory", memoryStats.getFreePhysical(), "bytes");
		metronClient.emitValueMetric("hazelcast.used_physical_memory", memoryStats.getTotalPhysical()-memoryStats.getFreePhysical(), "bytes");
		metronClient.emitValueMetric("hazelcast.gc_collection_time_minor", memoryStats.getGCStats().getMinorCollectionTime(), "ms");
		metronClient.emitValueMetric("hazelcast.gc_collection_time_major", memoryStats.getGCStats().getMajorCollectionTime(), "ms");
		AggregateStats stats = buildAggregateStats();
		
		metronClient.emitValueMetric("hazelcast.cache_used", stats.getHeapCost(), "bytes");
		metronClient.emitValueMetric("hazelcast.cache_free", maxSize-stats.getHeapCost(), "bytes");
		metronClient.emitValueMetric("hazelcast.cache_max", maxSize, "bytes");
		metronClient.emitValueMetric("hazelcast.total_caches", stats.getTotalCaches(), "count");
		metronClient.emitValueMetric("hazelcast.backup_entry", stats.getBackupEntryCount(), "count");
		metronClient.emitValueMetric("hazelcast.backup_entry_memory_cost", stats.getBackupEntryMemoryCost(), "bytes");
		metronClient.emitValueMetric("hazelcast.event_operation", stats.getEventOperationCount(), "count");
		metronClient.emitValueMetric("hazelcast.get_operation", stats.getGetOperationCount(), "count");
		metronClient.emitValueMetric("hazelcast.hits", stats.getHits(), "count");
		metronClient.emitValueMetric("hazelcast.locked_entry", stats.getLockedEntryCount(), "count");
		metronClient.emitValueMetric("hazelcast.max_get_latency", stats.getMaxGetLatency(), "ms");
		metronClient.emitValueMetric("hazelcast.max_put_latency", stats.getMaxPutLatency(), "ms");
		metronClient.emitValueMetric("hazelcast.max_remove_latency", stats.getMaxRemoveLatency(), "ms");
		metronClient.emitValueMetric("hazelcast.other_operation", stats.getOtherOperationCount(), "count");
		metronClient.emitValueMetric("hazelcast.owned_entry", stats.getOwnedEntryCount(), "count");
		metronClient.emitValueMetric("hazelcast.owned_entry_memory_cost", stats.getOwnedEntryMemoryCost(), "bytes");
		metronClient.emitValueMetric("hazelcast.put_operation", stats.getPutOperationCount(), "count");
		metronClient.emitValueMetric("hazelcast.remove_operation", stats.getRemoveOperationCount(), "count");
		metronClient.emitValueMetric("hazelcast.total_get_latency", stats.getTotalGetLatency(), "ms");
		metronClient.emitValueMetric("hazelcast.total_put_latency", stats.getTotalPutLatency(), "ms");
		metronClient.emitValueMetric("hazelcast.total_remove_latency", stats.getTotalRemoveLatency(), "ms");
		metronClient.emitValueMetric("hazelcast.total", stats.total(), "count");
		metronClient.emitValueMetric("hazelcast.committed_memory_cost", stats.getCommittedMemoryCost(), "bytes");
		metronClient.emitValueMetric("hazelcast.max_operations", stats.getMaxOperationsCount(), "count");
		try {
			metronClient.emitValueMetric("hazelcast.uptime", System.currentTimeMillis()-((Long)hazelcastMsgFactory.getInstance().getReplicatedMap(Stat.STAT_MAP).get(Stat.UPTIME_KEY)).longValue(), "ms");
		} catch(Exception e) {
			metronClient.emitValueMetric("hazelcast.uptime", 0, "ms");
		}
	}
	
	private AggregateStats buildAggregateStats() {
		HazelcastInstance instance = hazelcastMsgFactory.getInstance();
		AggregateStats stats = new AggregateStats(previousOperationsCounts);
		for (DistributedObject object : instance.getDistributedObjects()) {
			if (object instanceof IMap) {
				IMap<?, ?> map = (IMap<?, ?>) object;
				stats.aggregateStats(instance, map);
			}
		}
		previousOperationsCounts = stats.getOperationsCounts();
		return stats;
	}
	
	private static class AggregateStats implements LocalMapStats {
		private long backupEntryCount = 0;
		private long backupEntryMemoryCost = 0;
		private long eventOperationCount = 0;
		private long getOperationCount = 0;
		private long heapCost = 0;
		private long hits = 0;
		private long lockedEntryCount = 0;
		private long maxGetLatency = 0;
		private long maxPutLatency = 0;
		private long maxRemoveLatency = 0;
		private long otherOperationCount = 0;
		private long ownedEntryCount = 0;
		private long ownedEntryMemoryCost = 0;
		private long putOperationCount = 0;
		private long removeOperationCount = 0;
		private long totalGetLatency = 0;
		private long totalPutLatency = 0;
		private long totalRemoveLatency = 0;
		private long total = 0;
		private int totalCaches = 0;
		private long committedMemoryCost = 0;
		private long maxOperationsCount;
		private volatile Map<String, Long> previousOperationsCounts;
		private Map<String, Long> operationsCounts = new HashMap<>();
		
		public AggregateStats(Map<String, Long> previousOperationsCounts) {
			super();
			this.previousOperationsCounts = previousOperationsCounts;
		}

		public void aggregateStats(HazelcastInstance instance, IMap<?, ?> map) {
			LocalMapStats localStats = map.getLocalMapStats();
			backupEntryCount += localStats.getBackupEntryCount();
			backupEntryMemoryCost += localStats.getBackupEntryMemoryCost();
			eventOperationCount += localStats.getEventOperationCount();
			getOperationCount += localStats.getGetOperationCount();
			heapCost += localStats.getHeapCost();
			hits += localStats.getHits();
			lockedEntryCount += localStats.getLockedEntryCount();
			maxGetLatency = localStats.getMaxGetLatency() > maxGetLatency ? localStats.getMaxGetLatency() : maxGetLatency;
			maxPutLatency = localStats.getMaxPutLatency() > maxPutLatency ? localStats.getMaxPutLatency() : maxPutLatency;
			maxRemoveLatency = localStats.getMaxRemoveLatency() > maxRemoveLatency ? localStats.getMaxRemoveLatency() : maxRemoveLatency;
			otherOperationCount += localStats.getOtherOperationCount();
			ownedEntryCount += localStats.getOwnedEntryCount();
			ownedEntryMemoryCost += localStats.getOwnedEntryMemoryCost();
			putOperationCount += localStats.getPutOperationCount();
			removeOperationCount += localStats.getRemoveOperationCount();
			totalGetLatency += localStats.getTotalGetLatency();
			totalPutLatency += localStats.getTotalPutLatency();
			totalRemoveLatency += localStats.getTotalRemoveLatency();
			total += localStats.total();
			totalCaches++;
			committedMemoryCost += instance.getConfig().findMapConfig(map.getName()).getMaxSizeConfig().getSize();
			long totalOperationsCount = getOperationCount+otherOperationCount+putOperationCount+removeOperationCount;
			operationsCounts.put(map.getName(), totalOperationsCount);
			long localMaxOperationsCount = totalOperationsCount;
			if(previousOperationsCounts.containsKey(map.getName())) {
				localMaxOperationsCount = totalOperationsCount-previousOperationsCounts.get(map.getName());
			}
			if(maxOperationsCount < localMaxOperationsCount) {
				maxOperationsCount = localMaxOperationsCount;
			}
		}
		
		@Override
		public long getBackupEntryCount() {
			return backupEntryCount;
		}
		
		@Override
		public long getBackupEntryMemoryCost() {
			return backupEntryMemoryCost;
		}
		
		@Override
		public long getEventOperationCount() {
			return eventOperationCount;
		}
		
		@Override
		public long getGetOperationCount() {
			return getOperationCount;
		}
		
		@Override
		public long getHeapCost() {
			return heapCost;
		}
		
		@Override
		public long getHits() {
			// TODO Auto-generated method stub
			return hits;
		}
		
		@Override
		public long getLockedEntryCount() {
			return lockedEntryCount;
		}
		
		@Override
		public long getMaxGetLatency() {
			return maxGetLatency;
		}
		
		@Override
		public long getMaxPutLatency() {
			return maxPutLatency;
		}
		
		@Override
		public long getMaxRemoveLatency() {
			return maxRemoveLatency;
		}
		
		
		@Override
		public long getOtherOperationCount() {
			return otherOperationCount;
		}
		
		@Override
		public long getOwnedEntryCount() {
			return ownedEntryCount;
		}
		
		@Override
		public long getOwnedEntryMemoryCost() {
			return ownedEntryMemoryCost;
		}
		
		@Override
		public long getPutOperationCount() {
			return putOperationCount;
		}
		
		@Override
		public long getRemoveOperationCount() {
			return removeOperationCount;
		}
		
		@Override
		public long getTotalGetLatency() {
			return totalGetLatency;
		}
		
		@Override
		public long getTotalPutLatency() {
			return totalPutLatency;
		}
		
		@Override
		public long getTotalRemoveLatency() {
			return totalRemoveLatency;
		}
		
		@Override
		public long total() {
			return total;
		}
		
		public int getTotalCaches() {
			return totalCaches;
		}
		
		public long getCommittedMemoryCost() {
			return committedMemoryCost;
		}
		
		public long getMaxOperationsCount() {
			return maxOperationsCount;
		}
		
		public Map<String, Long> getOperationsCounts() {
			return operationsCounts;
		}
		
		@Override
		public long getCreationTime() {
			throw new UnsupportedOperationException();
		}

		@Override
		public long getDirtyEntryCount() {
			throw new UnsupportedOperationException();
		}
		@Override
		public long getLastAccessTime() {
			throw new UnsupportedOperationException();
		}
		
		@Override
		public long getLastUpdateTime() {
			throw new UnsupportedOperationException();
		}
		
		@Override
		public NearCacheStats getNearCacheStats() {
			throw new UnsupportedOperationException();
		}
		
		@Override
		public void fromJson(JsonObject json) {
			throw new UnsupportedOperationException();
		}
		
		@Override
		public JsonObject toJson() {
			throw new UnsupportedOperationException();
		}

		@Override
		public int getBackupCount() {
			throw new UnsupportedOperationException();
		}
	}
}