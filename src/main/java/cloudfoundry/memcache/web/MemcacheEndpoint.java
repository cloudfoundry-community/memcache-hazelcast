package cloudfoundry.memcache.web;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import cloudfoundry.memcache.MemcacheMsgHandlerFactory;

@Controller
public class MemcacheEndpoint {

	@Autowired
	private MemcacheMsgHandlerFactory factory;
	
	@RequestMapping(value="/cache", method=RequestMethod.GET, consumes = {"text/plain","application/*"}, produces = {"application/json"})
	public @ResponseBody List<String> listCache() {
		return factory.getCaches();
	}

	@RequestMapping(value="/cache", method=RequestMethod.DELETE, consumes = {"text/plain","application/*"})
	public void deleteCache() {
		factory.deleteCache(null);
	}

	@RequestMapping(value="/cache/{cacheName}", method=RequestMethod.PUT, consumes = {"text/plain","application/*"})
	public void createCache(@PathVariable String cacheName) {
		factory.createCache(cacheName);
	}
}
