package alien;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.Console;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;

import alien.config.ConfigUtils;
import alien.config.JAliEnIAm;
import alien.shell.BusyBox;
import alien.shell.ShellColor;
import alien.shell.commands.JAliEnBaseCommand;
import alien.user.UserFactory;
import lazyj.commands.CommandOutput;
import lazyj.commands.SystemCommand;
import lia.util.process.ExternalProcess.ExitStatus;
import utils.ProcessWithTimeout;
import alien.shell.WebsocketBox;


import javax.websocket.ClientEndpointConfig;
import javax.websocket.ContainerProvider;
import javax.websocket.Session;
import javax.websocket.WebSocketContainer;
import java.net.URI;

import alien.websockets.EndpointConfig;
import alien.websockets.JsonWebsocketEndpoint;
import org.apache.tomcat.websocket.WsWebSocketContainer;


public class JShWebsocket {
	
	public static void main(String[] args) {
		
		WebsocketBox box = new WebsocketBox();
		box.WebSocketConnect();
		new Thread()
		{
			
		}.start();
		
	}

}
