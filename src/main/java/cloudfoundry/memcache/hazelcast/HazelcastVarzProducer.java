package cloudfoundry.memcache.hazelcast;

import java.util.HashMap;
import java.util.Map;

import cf.component.VarzProducer;

import com.hazelcast.com.eclipsesource.json.JsonObject;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.memory.DefaultMemoryStats;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.monitor.NearCacheStats;

public class HazelcastVarzProducer implements VarzProducer {

	private final HazelcastInstance instance;
	private final long maxSize;

	public HazelcastVarzProducer(HazelcastInstance instance, long maxSize) {
		super();
		this.instance = instance;
		this.maxSize = maxSize;
	}

	@Override
	public Map<String, ?> produceVarz() {
		Map<String, Object> varz = new HashMap<>();
		MemoryStats memoryStats = new DefaultMemoryStats();
		
		varz.put("total_physical_bytes", memoryStats.getTotalPhysical());
		varz.put("free_physical_bytes", memoryStats.getFreePhysical());
		varz.put("used_physical_bytes", memoryStats.getTotalPhysical()-memoryStats.getFreePhysical());
		varz.put("gc_collection_time_minor", memoryStats.getGCStats().getMinorCollectionTime());
		varz.put("gc_collection_time_major", memoryStats.getGCStats().getMajorCollectionTime());
		
		AggregateStats stats = buildAggregateStats();
		
		varz.put("cache_bytes_used", stats.getHeapCost());
		varz.put("cache_bytes_free", maxSize-stats.getHeapCost());
		varz.put("cache_bytes_max", maxSize);
		varz.put("total_caches", stats.getTotalCaches());
		varz.put("hazelcast_backup_entry_count", stats.getBackupEntryCount());
		varz.put("hazelcast_backup_entry_memory_cost", stats.getBackupEntryMemoryCost());
		varz.put("hazelcast_event_operation_count", stats.getEventOperationCount());
		varz.put("hazelcast_get_operation_count", stats.getGetOperationCount());
		varz.put("hazelcast_hits", stats.getHits());
		varz.put("hazelcast_locked_entry_count", stats.getLockedEntryCount());
		varz.put("hazelcast_max_get_latency", stats.getMaxGetLatency());
		varz.put("hazelcast_max_put_latency", stats.getMaxPutLatency());
		varz.put("hazelcast_max_remove_latency", stats.getMaxRemoveLatency());
		varz.put("hazelcast_other_operation_count", stats.getOtherOperationCount());
		varz.put("hazelcast_owned_entry_count", stats.getOwnedEntryCount());
		varz.put("hazelcast_owned_entry_memory_cost", stats.getOwnedEntryMemoryCost());
		varz.put("hazelcast_put_operation_count", stats.getPutOperationCount());
		varz.put("hazelcast_remove_operation_count", stats.getRemoveOperationCount());
		varz.put("hazelcast_total_get_latency", stats.getTotalGetLatency());
		varz.put("hazelcast_total_put_latency", stats.getTotalPutLatency());
		varz.put("hazelcast_total_remove_latency", stats.getTotalRemoveLatency());
		varz.put("hazelcast_total", stats.total());

		return varz;
	}
	
	private AggregateStats buildAggregateStats() {
		AggregateStats stats = new AggregateStats();
		for (DistributedObject object : instance.getDistributedObjects()) {
			if (object instanceof IMap) {
				IMap<?, ?> map = (IMap<?, ?>) object;
				stats.aggregateStats(map.getLocalMapStats());
			}
		}
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
		private int totalCaches;
		
		public void aggregateStats(LocalMapStats localStats) {
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
