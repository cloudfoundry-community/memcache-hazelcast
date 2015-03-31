package cloudfoundry.memcache.web;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.annotation.web.servlet.configuration.EnableWebMvcSecurity;

@Configuration
@EnableWebMvcSecurity
public class WebSecurityConfig extends WebSecurityConfigurerAdapter {
	@Override
	protected void configure(HttpSecurity http) throws Exception {
		http.authorizeRequests()
    		.anyRequest().authenticated().and()
    		.csrf().disable()
    		.httpBasic();
	}

	@Autowired
	public void configureGlobal(AuthenticationManagerBuilder auth, @Value("${host.username}") String username, @Value("${host.password}") String password)
			throws Exception {
		auth.inMemoryAuthentication()
			.withUser(username)
			.password(password)
			.roles("USER");
	}
}
