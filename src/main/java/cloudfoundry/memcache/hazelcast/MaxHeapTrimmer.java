package cloudfoundry.memcache.hazelcast;

import java.util.Iterator;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class MaxHeapTrimmer implements Runnable {
	private static final Logger LOGGER = LoggerFactory.getLogger(MaxHeapTrimmer.class);

	private final HazelcastInstance instance;
	private final long totalHeap;
	private final int percentToTrim;
	
	public MaxHeapTrimmer(HazelcastInstance instance, long totalHeap, int percentToTrim) {
		super();
		this.instance = instance;
		this.totalHeap = totalHeap;
		this.percentToTrim = percentToTrim;
	}

	public void run() {
		LOGGER.debug("Running MaxHeapTrimmer.");
		try {
			long totalUsed;
			do {
				totalUsed = 0;
				for (DistributedObject object : instance.getDistributedObjects()) {
					if (object instanceof IMap) {
						IMap<?, ?> map = (IMap<?, ?>) object;
						long heapCost = map.getLocalMapStats().getHeapCost();
						totalUsed += heapCost;
					}
				}
				if (totalUsed > totalHeap) {
					LOGGER.error("Cache size '" + totalUsed + "' is greater than total allowed '" + totalHeap + "' trimming '" + percentToTrim
							+ "' percent from all caches.  We need to add more RAM to our cache servers.");
					trimCacheSize();
				}
			} while(totalUsed > totalHeap);
		} catch (Throwable t) {
			LOGGER.error("Unexpected error running max heap trimmer.", t);
		}
	}

	@SuppressWarnings("unchecked")
	private void trimCacheSize() {
		for(DistributedObject object : instance.getDistributedObjects()) {
			if(object instanceof IMap) {
				IMap<Object, ?> map = (IMap<Object, ?>)object;
				long ownedEntryCost = map.getLocalMapStats().getOwnedEntryMemoryCost();
				if(ownedEntryCost <= 0) {
 					continue;
				}
				long bytesToRemove = (ownedEntryCost/(100/percentToTrim));
				TreeSet<LocalMapEntry> evictionCandidates = new TreeSet<>();
				for(Object localKey : map.localKeySet()) {
					EntryView<Object, ?> entry = map.getEntryView(localKey);
					LocalMapEntry localEntry = new LocalMapEntry(localKey, entry.getCreationTime() > entry.getLastAccessTime() ? entry.getCreationTime() : entry.getLastAccessTime(), entry.getCost());
					evictionCandidates.add(localEntry);
				}
				Iterator<LocalMapEntry> localEntryIterator = evictionCandidates.descendingIterator();
				long totalCostEvicted = 0;
				while(localEntryIterator.hasNext() && totalCostEvicted < bytesToRemove) {
					LocalMapEntry entry = localEntryIterator.next();
					totalCostEvicted += entry.size;
					//Don't evict the item if it puts it over the edge.  No need to drain small caches of everything they have.
					if(totalCostEvicted < bytesToRemove) {
						map.evict(entry.key);
					}
				}
			}
		}
	}

	private static class LocalMapEntry implements Comparable<LocalMapEntry> {
		public LocalMapEntry(Object key, long lastAccessed, long size) {
			super();
			this.key = key;
			this.lastAccessed = lastAccessed;
			this.size = size;
		}

		final Object key;
		final long lastAccessed;
		final long size;
		
		@Override
		public int compareTo(LocalMapEntry o) {
			return (int)(o.lastAccessed - lastAccessed);
		}
	}
}
