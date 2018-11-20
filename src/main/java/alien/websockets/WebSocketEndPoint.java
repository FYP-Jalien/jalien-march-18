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


@ClientEndpoint
public class WebSocketEndPoint extends Endpoint {
	
	public static boolean connected = false;
	public Session session =null;
	
	@OnOpen
	public void onOpen(Session session, EndpointConfig config)
	{
		this.session = session;
		connected = true;
		System.out.println("Connected to JCentral.");
		//RemoteEndpoint.Basic remoteEndpointBasic = session.getBasicRemote();
		//session.addMessageHandler(new EchoMessageHandlerText(remoteEndpointBasic));
		
	}
	
	@SuppressWarnings("unchecked")
	@OnMessage
	public void onMessage(String message) throws Exception
	{

		String cmd = message;
		String[] tmp;
		tmp = cmd.split(" ");
		JSONObject fullCmd = new JSONObject();
		fullCmd.put("command", tmp[0]);
		for (int i=1; i<tmp.length;i++) {
		fullCmd.put("options",tmp[i].toString());
		}
		System.out.println("Command is: "+fullCmd.toString());
		this.session.getBasicRemote().sendObject(fullCmd);

	}
		
	@OnClose
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
	
/*	private static class EchoMessageHandlerText implements MessageHandler.Partial<String> {
		
		private final RemoteEndpoint.Basic remoteEndpointBasic;
		EchoMessageHandlerText(RemoteEndpoint.Basic remoteEndpointBasic) {
			this.remoteEndpointBasic = remoteEndpointBasic;
		}
		
		@Override
		public void onMessage(String message, boolean last) {
			try {
				if (remoteEndpointBasic != null) {
					// Try to parse incoming JSON
					Object pobj;
					JSONObject jsonObject;
					JSONParser parser = new JSONParser();

					try {
						pobj = parser.parse(new StringReader(message));
						jsonObject = (JSONObject) pobj;
					} catch (@SuppressWarnings("unused") ParseException e) {
						remoteEndpointBasic.sendText("Incoming JSON not ok", last);
						return;
					}
//
//					// Split JSONObject into strings
//					final ArrayList<String> fullCmd = new ArrayList<>();
//					fullCmd.add(jsonObject.get("command").toString());
//
//					JSONArray mArray = new JSONArray();
//					if (jsonObject.get("options") != null) {
//						mArray = (JSONArray) jsonObject.get("options");
//
//						for (int i = 0; i < mArray.size(); i++)
//							fullCmd.add(mArray.get(i).toString());
//					}
//					for(String m: fullCmd)
//					{
//						//session.getBasicRemote().sendText(fullCmd.toArray(new String[i]));
//						remoteEndpointBasic.sendText(m,last);
//					}
					
					//Get JSONArray list
					final JSONArray fullCmd = new JSONArray();
					fullCmd.add(jsonObject.get("command"));
					
					JSONArray mArray = new JSONArray();
					if (jsonObject.get("options") != null) {
						mArray = (JSONArray) jsonObject.get("options");

						for (int i = 0; i < mArray.size(); i++)
							fullCmd.add(mArray.get(i));
					}
					//Convert JSONArray to StringArray
					final ArrayList<String> sendCommand = new ArrayList<>();
					for (int i=0; i<fullCmd.size();i++)
					{
						sendCommand.add(fullCmd.get(i).toString());
					}
					//Convert StringArray to String
					StringBuilder builder = new StringBuilder();
					for (String s: sendCommand)
					{
						builder.append(s);
					}
					String finalCmd = builder.toString();
					
					//Send Command
					if(connected == true)
					{
						System.out.println(finalCmd);
						remoteEndpointBasic.sendText(finalCmd, last);
					}
				}

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		public void Send(String message)
		{
			onMessage(message, true);
		}
	}*/
}
