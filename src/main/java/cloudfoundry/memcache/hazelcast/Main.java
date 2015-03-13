package cloudfoundry.memcache.hazelcast;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import cf.spring.config.YamlPropertyContextInitializer;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import cloudfoundry.memcache.MemcacheMsgHandlerFactory;
import cloudfoundry.memcache.MemcacheServer;

import com.hazelcast.config.Config;

/**
 * @author Mike Heath <elcapo@gmail.com>
 */
@Configuration
public class Main {

	private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

	@Bean
	HazelcastMemcacheMsgHandlerFactory hazelcastconfig() {
		return new HazelcastMemcacheMsgHandlerFactory(new Config());
	}
	
	@Bean
	MemcacheServer memcacheServer(MemcacheMsgHandlerFactory handlerFactory) {
		System.out.println("Localport: "+54913);
		MemcacheServer server = new MemcacheServer(handlerFactory, 54913);
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
