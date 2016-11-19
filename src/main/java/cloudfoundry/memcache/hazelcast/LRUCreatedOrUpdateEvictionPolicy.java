package cloudfoundry.memcache.hazelcast;

import com.hazelcast.core.EntryView;
import com.hazelcast.map.eviction.MapEvictionPolicy;

/**
 * LRU eviction policy for an {@link com.hazelcast.core.IMap IMap}
 */
public class LRUCreatedOrUpdateEvictionPolicy<K, V> extends MapEvictionPolicy<K, V> {

    /**
     * LRU eviction policy instance.
     */
    public static final LRUCreatedOrUpdateEvictionPolicy<?, ?> INSTANCE = new LRUCreatedOrUpdateEvictionPolicy<>();

    @Override
    public int compare(EntryView<K, V> entryView1, EntryView<K, V> entryView2) {
        long lastAccessTime1 = findLastTimeAccessedCreatedOrUpdated(entryView1);

        long lastAccessTime2 = findLastTimeAccessedCreatedOrUpdated(entryView2);
        return (lastAccessTime1 < lastAccessTime2) ? -1 : ((lastAccessTime1 == lastAccessTime2) ? 0 : 1);
    }

	private long findLastTimeAccessedCreatedOrUpdated(EntryView<K, V> entryView) {
		long lastAccessTime = entryView.getLastAccessTime();
        if(lastAccessTime < entryView.getLastUpdateTime()) {
        	lastAccessTime = entryView.getLastUpdateTime();
        }
		if(lastAccessTime < entryView.getCreationTime()) {
        	lastAccessTime = entryView.getCreationTime();
        }
		return lastAccessTime;
	}
}
