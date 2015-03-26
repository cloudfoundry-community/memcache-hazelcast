package cloudfoundry.memcache.hazelcast;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import cf.spring.config.YamlPropertyContextInitializer;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import cloudfoundry.memcache.AuthMsgHandlerFactory;
import cloudfoundry.memcache.MemcacheMsgHandlerFactory;
import cloudfoundry.memcache.MemcacheServer;
import cloudfoundry.memcache.SecretKeyAuthMsgHandlerFactory;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.MaxSizeConfig.MaxSizePolicy;
import com.hazelcast.config.MemberGroupConfig;
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.PartitionGroupConfig;
import com.hazelcast.config.PartitionGroupConfig.MemberGroupType;
import com.hazelcast.config.TcpIpConfig;

@Configuration
public class Main {

	private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
	
	private static final String BACKUP_KEY = "backup";
	private static final String ASYNC_BACKUP_KEY = "async_backup";
	private static final String EVICTION_POLICY_KEY = "eviction_policy";
	private static final String MAX_IDLE_SECONDS_KEY = "max_idle_seconds";
	private static final String MAX_SIZE_USED_HEAP_KEY = "max_size_used_heap";

	@Bean
	HazelcastMemcacheMsgHandlerFactory hazelcastconfig(@Value("#{config['plans']}") Map<String, Map<String, Object>> plans, @Value("#{config['hazelcast']['machines']}") Map<String, List<String>> machines, @Value("#{config['hazelcast']['port']}") Integer port) {
		
		Config config = new Config();
		for(Map.Entry<String, Map<String, Object>> plan : plans.entrySet()) {
			MapConfig mapConfig = new MapConfig(plan.getKey()+"*");
			for(Map.Entry<String, Object> planConfig : plan.getValue().entrySet()) {
				if(BACKUP_KEY.equals(planConfig.getKey())) {
					mapConfig.setBackupCount((Integer)planConfig.getValue());
				} else if(ASYNC_BACKUP_KEY.equals(planConfig.getKey())) {
					mapConfig.setAsyncBackupCount((Integer)planConfig.getValue());
				} else if(EVICTION_POLICY_KEY.equals(planConfig.getKey())) {
					mapConfig.setEvictionPolicy(Enum.valueOf(EvictionPolicy.class, (String)planConfig.getValue()));
				} else if(MAX_IDLE_SECONDS_KEY.equals(planConfig.getKey())) {
					mapConfig.setMaxIdleSeconds((Integer)planConfig.getValue());
				} else if(MAX_SIZE_USED_HEAP_KEY.equals(planConfig.getKey())) {
					mapConfig.setMaxSizeConfig(new MaxSizeConfig((Integer)planConfig.getValue(), MaxSizePolicy.USED_HEAP_SIZE));
				}
			}
			config.addMapConfig(mapConfig);
		}
		NetworkConfig networkConfig = new NetworkConfig().setReuseAddress(true);
		config.setNetworkConfig(networkConfig);
		networkConfig.setPort(port);
		networkConfig.setPortAutoIncrement(false);
		JoinConfig joinConfig = new JoinConfig();
		networkConfig.setJoin(joinConfig);
		joinConfig.setMulticastConfig(new MulticastConfig().setEnabled(false));
		TcpIpConfig tcpIpConfig = new TcpIpConfig().setEnabled(true);
		joinConfig.setTcpIpConfig(tcpIpConfig);
		PartitionGroupConfig partitionGroupConfig = new PartitionGroupConfig();
		config.setPartitionGroupConfig(partitionGroupConfig);
		partitionGroupConfig.setEnabled(true);
		partitionGroupConfig.setGroupType(MemberGroupType.CUSTOM);
		for(Map.Entry<String, List<String>> zone : machines.entrySet()) {
			MemberGroupConfig memberGroupConfig = new MemberGroupConfig();
			for(String machine : zone.getValue()) {
				tcpIpConfig.addMember(machine);
				memberGroupConfig.addInterface(machine);
			}
			partitionGroupConfig.addMemberGroupConfig(memberGroupConfig);
		}
		return new HazelcastMemcacheMsgHandlerFactory(config);
	}

	@Bean
	AuthMsgHandlerFactory authHandlerFactory(@Value("#{config['auth']['secret_key']}") String key) {
		return new SecretKeyAuthMsgHandlerFactory(key);
	}

	@Bean
	MemcacheServer memcacheServer(MemcacheMsgHandlerFactory handlerFactory, AuthMsgHandlerFactory authFactory, @Value("#{config['memcached']['port']}") Integer port) {
		LOGGER.info("Memcached server starting on port: "+port);
		MemcacheServer server = new MemcacheServer(handlerFactory, port, authFactory);
		return server;
	}

	public static void main(String[] args) {
		final SpringApplication springApplication = new SpringApplication(Main.class);
		springApplication.addInitializers(new YamlPropertyContextInitializer(
				"config",
				"config",
				"config.yml"));
		final ApplicationContext applicationContext = springApplication.run(args);

		final LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
		final Level level = Level.toLevel(applicationContext.getEnvironment().getProperty("logging.level"), Level.INFO);
		loggerContext.getLogger("ROOT").setLevel(level);
		LOGGER.info("Memcache server started");
	}

}
