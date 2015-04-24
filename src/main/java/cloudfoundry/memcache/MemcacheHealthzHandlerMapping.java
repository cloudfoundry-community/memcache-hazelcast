package cloudfoundry.memcache;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.web.HttpRequestHandler;
import org.springframework.web.servlet.handler.AbstractUrlHandlerMapping;

public class MemcacheHealthzHandlerMapping extends AbstractUrlHandlerMapping {
	public MemcacheHealthzHandlerMapping(MemcacheMsgHandlerFactory factory) {
		setOrder(10);
		registerHandler("/healthz", new HttpRequestHandler() {
			@Override
			public void handleRequest(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws ServletException, IOException {
				httpServletResponse.setContentType("text/plain;charset=utf-8");
				if(factory.isReady()) {
					httpServletResponse.getWriter().write("ok");
				} else {
					httpServletResponse.setStatus(503);
					httpServletResponse.getWriter().write("down");
				}
			}
		});
	}
}
