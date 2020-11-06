package cloudfoundry.memcache;

import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.HttpRequestHandler;
import org.springframework.web.servlet.handler.AbstractUrlHandlerMapping;

@Component
public class MemcacheStatusHandlerMapping extends AbstractUrlHandlerMapping {
	@Autowired
	public MemcacheStatusHandlerMapping(MemcacheMsgHandlerFactory factory) {
		setOrder(10);
		registerHandler("/status", new HttpRequestHandler() {
			@Override
			public void handleRequest(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws ServletException, IOException {
				httpServletResponse.setContentType("text/plain;charset=utf-8");
				httpServletResponse.getWriter().write(factory.status());
			}
		});
	}
}
