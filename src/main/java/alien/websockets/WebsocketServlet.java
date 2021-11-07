/**
 *
 */
package alien.websockets;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

/**
 * @author costing
 * @since Nov 6, 2021
 */
public class WebsocketServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;

	@Override
	protected void service(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
		final var url = request.getRequestURI();
		final var lastToken = url.substring(url.lastIndexOf('/'));
		final var dispatcher = getServletContext().getRequestDispatcher("/websocketImpl" + lastToken);
		final var requestWrapper = new MyRequestWrapper(request);
		dispatcher.forward(requestWrapper, response);
	}
}

class MyRequestWrapper extends HttpServletRequestWrapper {
	public MyRequestWrapper(final HttpServletRequest request) {
		super(request);
	}

	@Override
	public Map<String, String[]> getParameterMap() {
		final var decoratedConnection = new HashMap<>(getRequest().getParameterMap());

		decoratedConnection.put("remoteAddr", new String[] { getRequest().getRemoteAddr(), String.valueOf(getRequest().getRemotePort()) });

		return decoratedConnection;
	}
}