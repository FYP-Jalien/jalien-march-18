package alien.websockets;

import java.io.IOException;

import javax.websocket.Endpoint;
import javax.websocket.EndpointConfig;
import javax.websocket.MessageHandler;
import javax.websocket.Session;

public class ClientEndPoint extends Endpoint{

	private Session session;
	
	@Override
	public void onOpen(Session session, EndpointConfig config) {
		// TODO Auto-generated method stub
		this.session = session;
		if (session.isOpen()) {
			System.out.println("Connected to JCentral");
		}
		session.addMessageHandler(new MessageHandler.Whole<String>() {
			@Override
			public void onMessage(String message)
			{
				System.out.println("Got message: " + message);
			}
		});
	}

}
