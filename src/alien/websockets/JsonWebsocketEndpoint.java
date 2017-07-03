package alien.websockets;

import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.security.Principal;
import java.util.ArrayList;

import javax.websocket.Endpoint;
import javax.websocket.EndpointConfig;
import javax.websocket.MessageHandler;
import javax.websocket.RemoteEndpoint;
import javax.websocket.Session;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import alien.shell.commands.JAliEnCOMMander;
import alien.shell.commands.JSONPrintWriter;
import alien.shell.commands.JShPrintWriter;
import alien.shell.commands.UIPrintWriter;
import alien.shell.commands.XMLPrintWriter;
import alien.user.AliEnPrincipal;

public class JsonWebsocketEndpoint extends Endpoint {
	private AliEnPrincipal userIdentity; 

    @Override
    public void onOpen(Session session, EndpointConfig endpointConfig) {
        RemoteEndpoint.Basic remoteEndpointBasic = session.getBasicRemote();
        Principal userPrincipal = session.getUserPrincipal();
        userIdentity = (AliEnPrincipal)userPrincipal;
        session.addMessageHandler(new EchoMessageHandlerText(remoteEndpointBasic, userIdentity));
        session.addMessageHandler(new EchoMessageHandlerBinary(remoteEndpointBasic));
    }

    private static class EchoMessageHandlerText
            implements MessageHandler.Partial<String> {

        private final RemoteEndpoint.Basic remoteEndpointBasic;
       		
		private JAliEnCOMMander commander = null;
		private UIPrintWriter out = null;
		private OutputStream os;
		private AliEnPrincipal userIdentity; 
		
		private void waitCommandFinish() {		
			// wait for the previous command to finish
			if (commander == null)
				return;
			while (commander.status.get() == 1)
				try {
					synchronized (commander.status) {
						commander.status.wait(1000);
					}
				} catch (@SuppressWarnings("unused") final InterruptedException ie) {
					// ignore
				}
		}
		
		private void setShellPrintWriter(final OutputStream os, final String shelltype) {
			if (shelltype.equals("jaliensh"))
				out = new JShPrintWriter(os);
			else if (shelltype.equals("json"))
				out = new JSONPrintWriter(os);
			else
				out = new XMLPrintWriter(os);
		}

        EchoMessageHandlerText(RemoteEndpoint.Basic remoteEndpointBasic, AliEnPrincipal userIdentity) {        	
            this.remoteEndpointBasic = remoteEndpointBasic;
            this.userIdentity = userIdentity; 
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
					
					// Init Commander only once during first message
					if (commander == null) {
						try {
							os = remoteEndpointBasic.getSendStream();
						} catch (IOException e) {
							e.printStackTrace();
						}
						
						setShellPrintWriter(os, "json");
												
						commander = new JAliEnCOMMander(userIdentity, null, null, null, out);

						commander.start();
					}
					
					// Set metadata
					out.setMetaInfo("user", commander.getUser().getName());
					out.setMetaInfo("role", commander.getRole());
					out.setMetaInfo("currentdir", commander.getCurrentDir().lfn);
					out.setMetaInfo("site", commander.getSite());
					
					// Split JSONObject into strings 
					final ArrayList<String> fullCmd = new ArrayList<>();
					fullCmd.add(jsonObject.get("command").toString());
									
					JSONArray mArray = new JSONArray();
					if (jsonObject.get("options") != null) {
						mArray = (JSONArray) jsonObject.get("options");
					
						for (int i = 0; i < mArray.size(); i++) 
		                    fullCmd.add(mArray.get(i).toString()); 
					}
					
					// Send the command to executor and send the result back to client via OutputStream
					synchronized (commander) {
						commander.setLine(out, fullCmd.toArray(new String[0]));
						commander.notifyAll();
					}					
					waitCommandFinish();
		        }
	        } catch (IOException e) {
	            // TODO Auto-generated catch block
	            e.printStackTrace();
	        } catch (Exception e) {
	        	e.printStackTrace();
	        }
	    }
	}

    private static class EchoMessageHandlerBinary
            implements MessageHandler.Partial<ByteBuffer> {

        private final RemoteEndpoint.Basic remoteEndpointBasic;

        EchoMessageHandlerBinary(RemoteEndpoint.Basic remoteEndpointBasic) {
            this.remoteEndpointBasic = remoteEndpointBasic;
        }

        @Override
        public void onMessage(ByteBuffer message, boolean last) {
            try {
                if (remoteEndpointBasic != null) {
                    remoteEndpointBasic.sendBinary(message, last);
                }
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}