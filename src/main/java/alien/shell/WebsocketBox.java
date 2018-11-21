package alien.shell;

import alien.websockets.WebSocketConnection;
import alien.websockets.WebSocketEndPoint;
import jline.console.ConsoleReader;
import jline.console.completer.ArgumentCompleter;
import jline.console.completer.StringsCompleter;

import javax.websocket.Session;

import org.json.simple.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;

import javax.websocket.ClientEndpointConfig;

import alien.JSh;
import alien.shell.commands.JShPrintWriter;
import alien.websockets.EndpointConfig;

public class WebsocketBox {

	private OutputStream os;
	
	/*public void SendCommand(final String... commandAndArguments) throws IOException
	{
		final String cmd = commandAndArguments.toString();
		os.write(cmd.getBytes());
		os.flush();
	}*/

	public boolean WebSocketConnect()
	{
		try {

			WebSocketConnection.getInstance("alice-jcentral.cern.ch", 8097);
			return true; 
		}

			catch (Throwable t){
					t.printStackTrace(System.err);
			}
	return false;
	}
	
	public void SendCommand(String message) throws Exception
	{
		System.out.println("Start sending message");
		WebSocketEndPoint wse = new WebSocketEndPoint();

		while(true)
		{
			wse.SendCmd(message);
			Thread.sleep(30000);
		}
	}
	
}
