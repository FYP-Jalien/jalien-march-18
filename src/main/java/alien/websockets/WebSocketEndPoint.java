package alien.websockets;
import javax.websocket.Endpoint;
import javax.websocket.Session;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.OnClose;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;

import javax.json.JsonObject;
import javax.json.JsonString;
import javax.websocket.ClientEndpoint;
import javax.websocket.CloseReason;
import javax.websocket.EncodeException;
import javax.websocket.EndpointConfig;
import javax.websocket.MessageHandler;
import javax.websocket.RemoteEndpoint;


public class WebSocketEndPoint extends Endpoint {
	
	private long _startTime = 0L;
	public long getUptime() {
		return System.currentTimeMillis() - _startTime;
	}
	
	public Session session;
	
	@Override
	public void onOpen(Session session, EndpointConfig config)
	{
		this.session = session;
		System.out.println("Connected to JCentral.");
		RemoteEndpoint.Basic remoteEndpointBasic = session.getBasicRemote();		
		session.addMessageHandler(new CommandMessageHandler(remoteEndpointBasic));
		
		
		try {
			session.getBasicRemote().sendText("ls");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		new Thread() {
			@Override
			public void run() {
				try {
					Thread.sleep(11 * 60 * 1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				if(getUptime()<600000)
				onClose(session, new CloseReason(null, "Connection expired (run for more than 10 minutes)"));				
				}
		}.start();
			
	}
	
	@OnMessage
	public void Message(String msg, Session session) {
		this.session=session;
		try {
			session.getBasicRemote().sendText(msg);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void SendCmd(String Cmd) {
		System.out.println("Trying to send: "+Cmd);
		Message(Cmd,this.session);
	}
			
	@Override
	public void onClose(Session session, CloseReason closereason)
	{
		this.session = session;
		try {
			if (session != null)
				session.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}	
	@Override
	public void onError(Session session, Throwable error)
	{
		
	}
	
	public class CommandMessageHandler implements MessageHandler.Whole<String>{
		
		private final RemoteEndpoint.Basic remoteEndpointBasic;
		
		CommandMessageHandler(RemoteEndpoint.Basic remoteEndpointBasic){
			this.remoteEndpointBasic = remoteEndpointBasic;
		}

		@Override
		public void onMessage(String msg) {
			if(remoteEndpointBasic!=null) {
			System.out.println("Recieved message: "+msg);
			}
		}
	}
}
