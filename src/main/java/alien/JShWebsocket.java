package alien;
import java.io.IOException;

import alien.shell.WebsocketBox;

import alien.websockets.WebSocketEndPoint;




public class JShWebsocket {
	
	public static void main(String[] args) throws Exception {
		
		WebsocketBox box = new WebsocketBox();
		box.WebSocketConnect();
		//box.SendCommand("ls");

	}

}
