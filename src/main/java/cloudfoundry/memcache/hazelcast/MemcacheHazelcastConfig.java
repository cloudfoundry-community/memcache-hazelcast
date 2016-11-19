package cloudfoundry.memcache.hazelcast;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.hibernate.validator.constraints.NotEmpty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import com.hazelcast.config.EvictionPolicy;

@Component
@ConfigurationProperties
public class MemcacheHazelcastConfig {

	@NotNull @Valid Memcache memcache = new Memcache();
	@NotNull @Valid Hazelcast hazelcast = new Hazelcast();
	@NotNull @Valid Host host = new Host();
	@NotEmpty @Valid Map<String, Plan> plans = new HashMap<>();
	
	public Memcache getMemcache() {
		return memcache;
	}

	public void setMemcache(Memcache memcache) {
		this.memcache = memcache;
	}

	public Hazelcast getHazelcast() {
		return hazelcast;
	}

	public void setHazelcast(Hazelcast hazelcast) {
		this.hazelcast = hazelcast;
	}

	public Host getHost() {
		return host;
	}

	public void setHost(Host host) {
		this.host = host;
	}

	public Map<String, Plan> getPlans() {
		return plans;
	}

	public void setPlans(Map<String, Plan> plans) {
		this.plans = plans;
	}

	public static class Plan {
		@NotNull Integer backup;
		@NotNull Integer asyncBackup;
		@NotNull EvictionPolicy evictionPolicy;
		@NotNull Integer maxIdleSeconds;
		@NotNull Integer maxSizeUsedHeap;
		@Valid NearCache nearCache;
		
		public NearCache getNearCache() {
			return nearCache;
		}
		public void setNearCache(NearCache nearCache) {
			this.nearCache = nearCache;
		}
		public Integer getBackup() {
			return backup;
		}
		public void setBackup(Integer backup) {
			this.backup = backup;
		}
		public Integer getAsyncBackup() {
			return asyncBackup;
		}
		public void setAsyncBackup(Integer asyncBackup) {
			this.asyncBackup = asyncBackup;
		}
		public EvictionPolicy getEvictionPolicy() {
			return evictionPolicy;
		}
		public void setEvictionPolicy(EvictionPolicy evictionPolicy) {
			this.evictionPolicy = evictionPolicy;
		}
		public Integer getMaxIdleSeconds() {
			return maxIdleSeconds;
		}
		public void setMaxIdleSeconds(Integer maxIdleSeconds) {
			this.maxIdleSeconds = maxIdleSeconds;
		}
		public Integer getMaxSizeUsedHeap() {
			return maxSizeUsedHeap;
		}
		public void setMaxSizeUsedHeap(Integer maxSizeUsedHeap) {
			this.maxSizeUsedHeap = maxSizeUsedHeap;
		}
		
		public static class NearCache {
			@NotEmpty String evictionPolicy;
			@NotNull Integer maxSize;
			@NotNull Integer ttlSeconds;
			@NotNull Integer maxIdleSeconds;
			public String getEvictionPolicy() {
				return evictionPolicy;
			}
			public void setEvictionPolicy(String evictionPolicy) {
				this.evictionPolicy = evictionPolicy;
			}
			public Integer getMaxSize() {
				return maxSize;
			}
			public void setMaxSize(Integer maxSize) {
				this.maxSize = maxSize;
			}
			public Integer getTtlSeconds() {
				return ttlSeconds;
			}
			public void setTtlSeconds(Integer ttlSeconds) {
				this.ttlSeconds = ttlSeconds;
			}
			public Integer getMaxIdleSeconds() {
				return maxIdleSeconds;
			}
			public void setMaxIdleSeconds(Integer maxIdleSeconds) {
				this.maxIdleSeconds = maxIdleSeconds;
			}
		}
	}
	
	public static class Host {
		@NotEmpty String username;
		@NotEmpty String password;
		public String getUsername() {
			return username;
		}
		public void setUsername(String username) {
			this.username = username;
		}
		public String getPassword() {
			return password;
		}
		public void setPassword(String password) {
			this.password = password;
		}
	}
	
	public static class Hazelcast {
		@NotNull Integer port;
		@NotNull Integer localMemberSafeTimeout;
		@NotNull Integer minimumClusterMembers;
		@NotNull Integer executorPoolSize;
		@NotNull Boolean enableMemoryTrimmer = Boolean.FALSE;
		@NotNull Long maxCacheSize;
		@NotNull Integer percentToTrim;
		@NotNull Integer trimDelay;
		@NotNull Integer partitionCount;
		@NotNull Integer ioThreadCount;
		@NotNull Integer operationThreadCount;
		@NotNull Integer operationGenericThreadCount;
		@NotNull Integer eventThreadCount;
		@NotNull Integer clientEventThreadCount;
		@NotNull Integer maxNoHeartbeatSeconds;
		@NotNull Integer operationCallTimeout;
		@NotNull Integer receiveBufferSize;
		@NotNull Integer sendBufferSize;
		@NotEmpty Map<String, List<String>> machines = new HashMap<>();

		public Integer getPort() {
			return port;
		}
		public void setPort(Integer port) {
			this.port = port;
		}
		public Integer getLocalMemberSafeTimeout() {
			return localMemberSafeTimeout;
		}
		public void setLocalMemberSafeTimeout(Integer localMemberSafeTimeout) {
			this.localMemberSafeTimeout = localMemberSafeTimeout;
		}
		public Integer getMinimumClusterMembers() {
			return minimumClusterMembers;
		}
		public void setMinimumClusterMembers(Integer minimumClusterMembers) {
			this.minimumClusterMembers = minimumClusterMembers;
		}
		public Integer getExecutorPoolSize() {
			return executorPoolSize;
		}
		public void setExecutorPoolSize(Integer executorPoolSize) {
			this.executorPoolSize = executorPoolSize;
		}
		public Boolean getEnableMemoryTrimmer() {
			return enableMemoryTrimmer;
		}
		public void setEnableMemoryTrimmer(Boolean enableMemoryTrimmer) {
			this.enableMemoryTrimmer = enableMemoryTrimmer;
		}
		public Long getMaxCacheSize() {
			return maxCacheSize;
		}
		public void setMaxCacheSize(Long maxCacheSize) {
			this.maxCacheSize = maxCacheSize;
		}
		public Integer getPercentToTrim() {
			return percentToTrim;
		}
		public void setPercentToTrim(Integer percentToTrim) {
			this.percentToTrim = percentToTrim;
		}
		public Integer getTrimDelay() {
			return trimDelay;
		}
		public void setTrimDelay(Integer trimDelay) {
			this.trimDelay = trimDelay;
		}
		public Integer getPartitionCount() {
			return partitionCount;
		}
		public void setPartitionCount(Integer partitionCount) {
			this.partitionCount = partitionCount;
		}
		public Integer getIoThreadCount() {
			return ioThreadCount;
		}
		public void setIoThreadCount(Integer ioThreadCount) {
			this.ioThreadCount = ioThreadCount;
		}
		public Integer getOperationThreadCount() {
			return operationThreadCount;
		}
		public void setOperationThreadCount(Integer operationThreadCount) {
			this.operationThreadCount = operationThreadCount;
		}
		public Integer getOperationGenericThreadCount() {
			return operationGenericThreadCount;
		}
		public void setOperationGenericThreadCount(Integer operationGenericThreadCount) {
			this.operationGenericThreadCount = operationGenericThreadCount;
		}
		public Integer getEventThreadCount() {
			return eventThreadCount;
		}
		public void setEventThreadCount(Integer eventThreadCount) {
			this.eventThreadCount = eventThreadCount;
		}
		public Integer getClientEventThreadCount() {
			return clientEventThreadCount;
		}
		public void setClientEventThreadCount(Integer clientEventThreadCount) {
			this.clientEventThreadCount = clientEventThreadCount;
		}
		public Integer getMaxNoHeartbeatSeconds() {
			return maxNoHeartbeatSeconds;
		}
		public void setMaxNoHeartbeatSeconds(Integer maxNoHeartbeatSeconds) {
			this.maxNoHeartbeatSeconds = maxNoHeartbeatSeconds;
		}
		public Integer getOperationCallTimeout() {
			return operationCallTimeout;
		}
		public void setOperationCallTimeout(Integer operationCallTimeout) {
			this.operationCallTimeout = operationCallTimeout;
		}
		public Integer getReceiveBufferSize() {
			return receiveBufferSize;
		}
		public void setReceiveBufferSize(Integer receiveBufferSize) {
			this.receiveBufferSize = receiveBufferSize;
		}
		public Integer getSendBufferSize() {
			return sendBufferSize;
		}
		public void setSendBufferSize(Integer sendBufferSize) {
			this.sendBufferSize = sendBufferSize;
		}
		public Map<String, List<String>> getMachines() {
			return machines;
		}
		public void setMachines(Map<String, List<String>> machines) {
			this.machines = machines;
		}
	}
	
	public static class Memcache {
		@NotNull Integer port;
		@NotEmpty String secretKey;
		@NotEmpty String testUser;
		@NotEmpty String testPassword;
		@NotEmpty String testCache;
		@NotNull Integer maxQueueSize;
		
		public Integer getPort() {
			return port;
		}
		public void setPort(Integer port) {
			this.port = port;
		}
		public String getSecretKey() {
			return secretKey;
		}
		public void setSecretKey(String secretKey) {
			this.secretKey = secretKey;
		}
		public String getTestUser() {
			return testUser;
		}
		public void setTestUser(String testUser) {
			this.testUser = testUser;
		}
		public String getTestPassword() {
			return testPassword;
		}
		public void setTestPassword(String testPassword) {
			this.testPassword = testPassword;
		}
		public String getTestCache() {
			return testCache;
		}
		public void setTestCache(String testCache) {
			this.testCache = testCache;
		}
		public Integer getMaxQueueSize() {
			return maxQueueSize;
		}
		public void setMaxQueueSize(Integer maxQueueSize) {
			this.maxQueueSize = maxQueueSize;
		}
	}
}
