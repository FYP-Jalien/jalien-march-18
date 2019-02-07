package alien;
import java.io.IOException;

import alien.shell.WebsockBox;


import java.util.logging.Level;
import java.util.logging.Logger;
import java.net.URI;
import java.security.SecureRandom;
import java.security.Security;

import org.apache.tomcat.websocket.Constants;
import org.apache.tomcat.websocket.WsWebSocketContainer;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.websocket.ClientEndpointConfig;
import javax.websocket.ContainerProvider;
import javax.websocket.Session;
import javax.websocket.WebSocketContainer;

import alien.config.ConfigUtils;
import alien.user.JAKeyStore;
import alien.websockets.ClientEndPoint;
import alien.websockets.WebSocketConnection;
public class WebSockShell {

	public static void main(String[] args) {		
		//WebsockBox wbox = new WebsockBox();
		//wbox.connect();
		try {
			WebSocketConnection.getInstance("alice-jcentral.cern.ch", 8097);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
