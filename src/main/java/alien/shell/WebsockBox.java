package alien.shell;

import java.util.logging.Level;
import java.util.logging.Logger;
import java.io.Console;
import java.net.URI;
import java.security.SecureRandom;
import java.security.Security;

import org.apache.tomcat.websocket.Constants;
import org.apache.tomcat.websocket.WsWebSocketContainer;
import org.json.simple.JSONObject;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.websocket.ClientEndpointConfig;
import javax.websocket.ContainerProvider;
import javax.websocket.Session;
import javax.websocket.WebSocketContainer;

import alien.config.ConfigUtils;
import alien.user.JAKeyStore;
import alien.websockets.ClientEndPoint;
public class WebsockBox {
	
	static transient final Logger logger = ConfigUtils.getLogger(WebsockBox.class.getCanonicalName());
	
	public boolean connect()
	{
		Session session;
		try {
			// get factory
			final KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509", "SunJSSE");

			logger.log(Level.INFO, "Connecting with client cert: " + ((java.security.cert.X509Certificate) JAKeyStore.getKeyStore().getCertificateChain("User.cert")[0]).getSubjectDN());

			// initialize factory, with clientCert(incl. priv+pub)
			kmf.init(JAKeyStore.getKeyStore(), JAKeyStore.pass);

			java.lang.System.setProperty("jdk.tls.client.protocols", "TLSv1,TLSv1.1,TLSv1.2");
			final SSLContext ssc = SSLContext.getInstance("TLS");

			// initialize SSL with certificate and the trusted CA and pub
			// certs
			ssc.init(kmf.getKeyManagers(), JAKeyStore.trusts, new SecureRandom());
			ClientEndpointConfig config = ClientEndpointConfig.Builder.create().build();
			config.getUserProperties().put(Constants.SSL_CONTEXT_PROPERTY, ssc);
			 
			URI uri = URI.create("wss://alice-jcentral.cern.ch:8097/websocket/json");
			WebSocketContainer container = ContainerProvider.getWebSocketContainer();
			session = container.connectToServer(ClientEndPoint.class, config, uri);

		} catch (final Throwable e) {
			logger.log(Level.SEVERE, "Could not initiate SSL connection to the server.", e);
			// e.printStackTrace();
			System.err.println("Could not initiate SSL connection to the server.");
		}
		return false;
	}

}
