package alien.websockets;
import alien.websockets.EndpointConfig;
import java.io.IOException;


import java.net.URI;
import java.security.SecureRandom;
import java.security.Security;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;

import javax.websocket.ClientEndpointConfig;
import javax.websocket.ContainerProvider;
import javax.websocket.Session;
import javax.websocket.WebSocketContainer;

import org.apache.catalina.Lifecycle;
import org.apache.tomcat.websocket.Constants;
import org.apache.tomcat.websocket.WsWebSocketContainer;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import alien.config.ConfigUtils;
import alien.user.JAKeyStore;

//import alien.websockets.JShellWebsocketClient;

public class WebSocketConnection extends Thread {
 
	
	static transient final Logger logger = ConfigUtils.getLogger(WebSocketConnection.class.getCanonicalName());


	
	private static HashMap<Integer, WebSocketConnection> instance = new HashMap<>(20);
	
	public static String addr;
	public static int port;
	
	public WebSocketConnection(ClientEndpointConfig config) throws IOException
		{
			URI uri = URI.create("wss://"+addr+":"+Integer.toString(port)+"/websocket/json");
			//URI uri = URI.create("wss://alice-jcentral.cern.ch:8097/websocket/json");
			try {
				WebSocketContainer container = ContainerProvider.getWebSocketContainer();
				container.connectToServer(WebSocketEndPoint.class,config, uri);
				
			}
			catch (Throwable t){
					t.printStackTrace(System.err);
			}
		}

	public static WebSocketConnection getInstance(final String address, final int p) throws IOException
	{
		addr = address;
		port = p;
		
		if (instance.get(Integer.valueOf(p)) == null) {
			// connect to the other end
			logger.log(Level.INFO, "Connecting to JCentral on " + address + ":" + p);
			System.out.println("Connecting to JCentral on " + address + ":" + p);

			Security.addProvider(new BouncyCastleProvider());

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
				 instance.put(Integer.valueOf(p), new WebSocketConnection(config));
			} catch (final Throwable e) {
				logger.log(Level.SEVERE, "Could not initiate SSL connection to the server.", e);
				// e.printStackTrace();
				System.err.println("Could not initiate SSL connection to the server.");
			}

		}
		return instance.get(Integer.valueOf(p));

	}
	
}
