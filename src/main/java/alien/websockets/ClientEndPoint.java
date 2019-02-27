package alien.websockets;

import java.io.Console;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

import javax.websocket.CloseReason;
import javax.websocket.Endpoint;
import javax.websocket.EndpointConfig;
import javax.websocket.MessageHandler;
import javax.websocket.OnMessage;
import javax.websocket.RemoteEndpoint;
import javax.websocket.Session;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class ClientEndPoint extends Endpoint{

	private Session session;
	final Object stateObject = new Object();
	public static long _lastActivityTime = 0L;
	private long _startTime = 0L;
	public boolean cmdsent = false;
	public long getUptime() {
		return System.currentTimeMillis() - _startTime;
	}
	
	@Override
	public void onOpen(Session session, EndpointConfig config) {
		// TODO Auto-generated method stub
		this.session = session;
		if (session.isOpen()) {
			System.out.println("Connected to JCentral");
		}
		TextMessageHandler handler = new TextMessageHandler(this.session);
		this.session.addMessageHandler(handler);	
		
		///Send command to server
		new Thread() {
			@Override
			public void run() {
			while(ClientEndPoint.this.session.isOpen()) {
				SendCommand();
			}
			}
		}.start();
		///
		new Thread() {
			@Override
			public void run() {
					synchronized (stateObject) {
						try {
							stateObject.wait(3 * 60 * 60 * 1000L);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}

					if (getUptime() > 172800000) // 2 days
						onClose(session, new CloseReason(null, "Connection expired (run for more than 2 days)"));

					if (System.currentTimeMillis() - _lastActivityTime > 3 * 60 * 60 * 1000) // 3 hours
						onClose(session, new CloseReason(null, "Connection idle for more than 3 hours"));
			}
		}.start();

	}
		
	@OnMessage
	public void SendCommand()
	{
		String message;
		Console c = System.console();
		message = c.readLine();
		try {
			this.session.getBasicRemote().sendText("{\"command\":\""+message+"\"}");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		message=null;
		c=null;
	}
	
	@Override
	public void onClose(final Session session, final CloseReason closeReason) {
		try {
			if (session != null)
				session.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		synchronized (stateObject) {
			stateObject.notifyAll();
		}
	}
	
	public static class TextMessageHandler implements MessageHandler.Whole<String>{

		private final RemoteEndpoint.Basic endpointbasic;
		public TextMessageHandler(Session session) {
			this.endpointbasic = session.getBasicRemote();
		}
		public String textPrefix ="";
		public boolean cmdrecieved = false;
		public ArrayList<String> answer =null;
		///
		public void welcome()
		{
			
		}
		///
		public void Parsing(String message)
		{
			//Parse incoming message
			if(endpointbasic!=null) {
				Object obj;
				JSONObject tmpjsonObj;
				JSONParser pr = new JSONParser();
				String prefix = "";
				ArrayList<String> arr = new ArrayList<>();
				try {
					obj = pr.parse(message);
					tmpjsonObj = (JSONObject)obj;					
					JSONArray mArray = new JSONArray();
					
					if(tmpjsonObj.get("metadata")!=null) {
						prefix = tmpjsonObj.get("metadata").toString();
					}
					
					if (tmpjsonObj.get("results") != null) {
						mArray = (JSONArray) tmpjsonObj.get("results");
						
						for (int i = 0; i < mArray.size(); i++) {
							arr.add(mArray.get(i).toString());
						}
						
					}					
					//Parse "Results"
					Object object;
					JSONObject json;
					JSONArray cmd = new JSONArray();
					answer = new ArrayList<>();
					for (int i=0;i<arr.size();i++) {
						object = pr.parse(arr.get(i));
						json = (JSONObject)object;
						if(json.get("message")!=null) {
						cmd.add(json.get("message"));
						}
					}
					for (int i=0;i<cmd.size();i++) {
						answer.add(cmd.get(i).toString());
					}
					
					//Create prefix
					Object pref;
					JSONObject jprefix;
					pref = pr.parse(prefix);
					jprefix = (JSONObject)pref;
					if (jprefix.get("currentdir")!=null) {
						textPrefix = jprefix.get("currentdir").toString();
					}
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		public void Recieve()
		{	
			for (int i=0;i<answer.size();i++) {
				System.out.println(textPrefix+"> "+answer.get(i));
			}
		}
		
		@Override
		public void onMessage(String message) {
			Parsing(message);
			Recieve();
			_lastActivityTime = System.currentTimeMillis();

		}
		
	}

}
