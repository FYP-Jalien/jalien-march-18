/**
 *
 */
package alien.websockets;

import java.util.List;
import java.util.Map;

import javax.servlet.ServletContextEvent;
import javax.websocket.DeploymentException;
import javax.websocket.HandshakeResponse;
import javax.websocket.server.HandshakeRequest;
import javax.websocket.server.ServerContainer;
import javax.websocket.server.ServerEndpointConfig;

import org.apache.tomcat.websocket.server.Constants;
import org.apache.tomcat.websocket.server.WsContextListener;

/**
 * @author vyurchen
 *
 *         Websocket listener must be added manually to the Tomcat context to bootstrap the WsServerContainer correctly
 */
public class WebsocketListener extends WsContextListener {

	private static final String[] copyHeaders = new String[] { "User-Agent" };

	private static final class JAliEnConfigurator extends ServerEndpointConfig.Configurator {
		@Override
		public void modifyHandshake(final ServerEndpointConfig conf, final HandshakeRequest request, final HandshakeResponse response) {
			final Map<String, List<String>> headers = request.getHeaders();

			for (final String header : copyHeaders) {
				final List<String> value = headers.get(header);

				if (value != null && value.size() > 0)
					conf.getUserProperties().put(header, value.iterator().next());
			}
		}
	}

	@Override
	public void contextInitialized(final ServletContextEvent sce) {
		super.contextInitialized(sce);

		final ServerContainer sc = (ServerContainer) sce.getServletContext().getAttribute(Constants.SERVER_CONTAINER_SERVLET_CONTEXT_ATTRIBUTE);

		try {
			sc.addEndpoint(ServerEndpointConfig.Builder.create(WebsocketEndpoint.class, "/websocketImpl/json").configurator(new JAliEnConfigurator()).build());
			sc.addEndpoint(ServerEndpointConfig.Builder.create(WebsocketEndpoint.class, "/websocketImpl/plain").configurator(new JAliEnConfigurator()).build());
		}
		catch (final DeploymentException e) {
			throw new RuntimeException(e);
		}
	}
}