package cloudfoundry.memcache.hazelcast;


public interface Stat {
	public static final String STAT_MAP = "stats";
	public static final String UPTIME_KEY = "uptime";
	String getStat(HazelcastMemcacheMsgHandler handler);
}
