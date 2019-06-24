package alien.websockets;

import java.io.IOException;
import java.util.ArrayList;

import javax.websocket.CloseReason;
import javax.websocket.Endpoint;
import javax.websocket.EndpointConfig;
import javax.websocket.MessageHandler;
import javax.websocket.RemoteEndpoint;
import javax.websocket.Session;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


//EndPoint for Websocket Client
public class ClientEndPoint extends Endpoint{

	private Session session;
	final Object stateObject = new Object();
	public static long _lastActivityTime = 0L;
	private long _startTime = 0L;
	public long getUptime() {
		return System.currentTimeMillis() - _startTime;
	}
	
	@Override
	public void onOpen(Session session, EndpointConfig config) {
		// TODO Auto-generated method stub
		this.session = session;
		TextMessageHandler handler = new TextMessageHandler(this.session);
		this.session.addMessageHandler(handler);
		if (session.isOpen()) {
			System.out.println("Connected to JCentral");
			try {
				System.out.print("Welcome ");
				this.session.getBasicRemote().sendText("{\"command\":\"whoami\"}");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}


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
		public Session session;
		public String textPrefix;
		public ArrayList<String> answer =null;
		public TextMessageHandler(Session session) {
			this.session = session;
			this.endpointbasic = this.session.getBasicRemote();
		}
		@Override
		public void onMessage(String message) {
			textPrefix =null;
			if (endpointbasic !=null) {

				try {
					
				// Get Prefix
					JSONParser p = new JSONParser();
					String prefix = null;
					Object obj;
					obj = p.parse(message);
					JSONObject tmpjsonObj;
					tmpjsonObj = (JSONObject)obj;
					if(tmpjsonObj.get("metadata")!=null) {
						prefix = tmpjsonObj.get("metadata").toString();
					}
					//
					Object pref;
					JSONObject jprefix;
					pref = p.parse(prefix);
					jprefix = (JSONObject)pref;
					
					if (jprefix.get("currentdir")!=null) {
						textPrefix = jprefix.get("currentdir").toString();
					}
					
					//Get Command
					ArrayList<String> arr = new ArrayList<>();
					JSONArray mArray = new JSONArray();
					
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
						object = p.parse(arr.get(i));
						json = (JSONObject)object;
						if(json.get("message")!=null) {
						cmd.add(json.get("message"));
						}
					}
					for (int i=0;i<cmd.size();i++) {
						answer.add(cmd.get(i).toString());
					}
					//
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			
			//Recieve message
			for (int i=0;i<answer.size();i++) {
				System.out.println(answer.get(i));
			}
			System.out.print(textPrefix+"> ");
			textPrefix = null;
			_lastActivityTime = System.currentTimeMillis();

		}

	}

}
}
