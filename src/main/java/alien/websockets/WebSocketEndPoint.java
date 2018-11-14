package alien.websockets;
import javax.websocket.Endpoint;
import javax.websocket.Session;

import org.apache.catalina.startup.Tomcat;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import alien.api.TomcatServer;
import alien.shell.commands.JAliEnCOMMander;
import alien.shell.commands.UIPrintWriter;
import alien.user.AliEnPrincipal;

import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;

import javax.json.JsonString;
import javax.websocket.CloseReason;
import javax.websocket.EndpointConfig;
import javax.websocket.MessageHandler;
import javax.websocket.OnMessage;
import javax.websocket.RemoteEndpoint;

import alien.shell.commands.JAliEnCOMMander;
import alien.shell.commands.JSONPrintWriter;
import alien.shell.commands.JShPrintWriter;
import alien.shell.commands.UIPrintWriter;
import alien.shell.commands.XMLPrintWriter;
import alien.user.AliEnPrincipal;


public class WebSocketEndPoint extends Endpoint {
	
	public static boolean connected = false;

	
	@Override
	public void onOpen(Session session, EndpointConfig config)
	{

		connected = true;
		System.out.println("Connected to JCentral.");
		RemoteEndpoint.Basic remoteEndpointBasic = session.getBasicRemote();
		session.addMessageHandler(new EchoMessageHandlerText(remoteEndpointBasic));
		

	}
	
	@Override
	public void onClose(Session session, CloseReason closereason)
	{

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
	
	private static class EchoMessageHandlerText implements MessageHandler.Partial<String> {
		
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
/*
					// Split JSONObject into strings
					final ArrayList<String> fullCmd = new ArrayList<>();
					fullCmd.add(jsonObject.get("command").toString());

					JSONArray mArray = new JSONArray();
					if (jsonObject.get("options") != null) {
						mArray = (JSONArray) jsonObject.get("options");

						for (int i = 0; i < mArray.size(); i++)
							fullCmd.add(mArray.get(i).toString());
					}
					for(String m: fullCmd)
					{
						//session.getBasicRemote().sendText(fullCmd.toArray(new String[i]));
						remoteEndpointBasic.sendText(m,last);
					}*/
					
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
	}
}
