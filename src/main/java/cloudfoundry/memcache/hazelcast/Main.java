package cloudfoundry.memcache.hazelcast;

import cf.dropsonde.spring.boot.EnableMetronClient;
import cloudfoundry.memcache.AuthMsgHandlerFactory;
import cloudfoundry.memcache.MemcacheServer;
import cloudfoundry.memcache.MemcacheStats;
import cloudfoundry.memcache.SecretKeyAuthMsgHandlerFactory;
import cloudfoundry.memcache.web.HttpBasicAuthenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@ComponentScan("cloudfoundry.memcache")
@EnableMetronClient
@EnableScheduling
public class Main {

	private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

	@Bean
	HazelcastMemcacheMsgHandlerFactory hazelcastconfig(MemcacheHazelcastConfig appConfig) {
		return new HazelcastMemcacheMsgHandlerFactory(appConfig);
	}

	@Bean
	AuthMsgHandlerFactory authHandlerFactory(MemcacheHazelcastConfig config) {
		return new SecretKeyAuthMsgHandlerFactory(config.getMemcache().getSecretKey(),
				config.getMemcache().getTestUser(), config.getMemcache().getTestPassword(),
				config.getMemcache().getTestCache());
	}

	@Bean
	HttpBasicAuthenticator basicAuthenticator(MemcacheHazelcastConfig config) {
		return new HttpBasicAuthenticator("", config.getHost().getUsername(), config.getHost().getPassword());
	}

	@Bean
	MemcacheServer memcacheServer(HazelcastMemcacheMsgHandlerFactory hazelcastHandlerFactory,
			AuthMsgHandlerFactory authFactory, MemcacheHazelcastConfig config, MemcacheStats memcacheStats)
			throws Exception {
		return new MemcacheServer(hazelcastHandlerFactory, config.getMemcache().getPort(), authFactory,
				config.getMemcache().getMaxQueueSize(), config.getMemcache().getRequestRateLimit(), memcacheStats);
	}

	public static void main(String[] args) throws Exception {
		SpringApplication.run(Main.class, args);
		LOGGER.info("Memcache server initialized.");
	}
}
