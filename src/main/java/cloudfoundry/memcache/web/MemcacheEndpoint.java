package cloudfoundry.memcache.web;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import cloudfoundry.memcache.MemcacheMsgHandlerFactory;

@Controller
public class MemcacheEndpoint {

	@Autowired
	private HttpBasicAuthenticator authenticator;

	@Autowired
	private MemcacheMsgHandlerFactory factory;
	
	@RequestMapping(value="/cache/{cacheName}", method=RequestMethod.DELETE, consumes = {"text/plain","application/*"})
	public void deleteCache(HttpServletRequest request, HttpServletResponse response, @PathVariable String cacheName) {
		if(authenticator.authenticate(request, response)) {
			factory.deleteCache(cacheName);
		}
	}
}
