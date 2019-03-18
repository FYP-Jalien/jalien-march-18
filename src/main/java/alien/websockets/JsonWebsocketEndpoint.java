package alien.websockets;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.security.Principal;
import java.util.ArrayList;

import javax.websocket.CloseReason;
import javax.websocket.Endpoint;
import javax.websocket.EndpointConfig;
import javax.websocket.MessageHandler;
import javax.websocket.RemoteEndpoint;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpointConfig;

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

/**
 * @author yuw
 *
 *         Implementation of websocket endpoint, that parses JSON commands
 */
public class JsonWebsocketEndpoint extends Endpoint {
	AliEnPrincipal userIdentity = null;

	/**
	 * Commander
	 */
	JAliEnCOMMander commander = null;

	private UIPrintWriter out = null;
	private OutputStream os = null;

	private void setShellPrintWriter(final OutputStream os, final String shelltype) {
		if (shelltype.equals("plain"))
			out = new JShPrintWriter(os);
		else
			if (shelltype.equals("json"))
				out = new JSONPrintWriter(os);
			else
				out = new XMLPrintWriter(os);
	}

	private long _startTime = 0L;

	/**
	 * Time with no activity coming from the client
	 */
	public static long _lastActivityTime = 0L;

	/**
	 * Get websocket connection uptime
	 * 
	 * @return uptime in ms
	 */
	public long getUptime() {
		return System.currentTimeMillis() - _startTime;
	}

	/**
	 * Object to send notifications about the state of connection
	 */
	final Object stateObject = new Object();

	@Override
	public void onOpen(final Session session, final EndpointConfig endpointConfig) {
		final Principal userPrincipal = session.getUserPrincipal();
		userIdentity = (AliEnPrincipal) userPrincipal;

		os = new ByteArrayOutputStream();
		final ServerEndpointConfig serverConfig = (ServerEndpointConfig) endpointConfig;
		if (serverConfig.getPath() == "/websocket/json")
			setShellPrintWriter(os, "json");
		else
			setShellPrintWriter(os, "plain");

		commander = new JAliEnCOMMander(userIdentity, null, null, out);

		session.addMessageHandler(new EchoMessageHandlerText(session, commander, out, os));
		_startTime = System.currentTimeMillis();
		_lastActivityTime = System.currentTimeMillis();

		new Thread() {
			@Override
			public void run() {
				while (!commander.kill) {
					synchronized (stateObject) {
						try {
							stateObject.wait(3 * 60 * 60 * 1000L);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}

					if (getUptime() > 172800000 || commander.getUser().getUserCert()[0].getNotAfter().getTime() > System.currentTimeMillis()) // 2 days
						onClose(session, new CloseReason(null, "Connection expired (run for more than 2 days)"));

					if (System.currentTimeMillis() - _lastActivityTime > 3 * 60 * 60 * 1000) // 3 hours
						onClose(session, new CloseReason(null, "Connection idle for more than 3 hours"));
				}
			}
		}.start();
	}

	@Override
	public void onClose(final Session session, final CloseReason closeReason) {
		commander.kill = true;

		out = null;
		try {
			if (os != null)
				os.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		os = null;
		userIdentity = null;
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

	@Override
	public void onError(final Session session, final Throwable thr) {
		//
	}

	private static class EchoMessageHandlerText implements MessageHandler.Partial<String> {

		private final RemoteEndpoint.Basic remoteEndpointBasic;

		private JAliEnCOMMander commander = null;
		private UIPrintWriter out = null;
		private OutputStream os = null;

		EchoMessageHandlerText(final Session session, final JAliEnCOMMander commander, final UIPrintWriter out, OutputStream os) {
			this.remoteEndpointBasic = session.getBasicRemote();
			this.commander = commander;
			this.out = out;
			this.os = os;
		}

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

		@Override
		public void onMessage(final String message, final boolean last) {
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
						synchronized (remoteEndpointBasic) {
							remoteEndpointBasic.sendText("Incoming JSON not ok", true);
						}
						return;
					}

					// Split JSONObject into strings
					final ArrayList<String> fullCmd = new ArrayList<>();
					fullCmd.add(jsonObject.get("command").toString());

					JSONArray mArray = new JSONArray();
					if (jsonObject.get("options") != null) {
						mArray = (JSONArray) jsonObject.get("options");

						for (int i = 0; i < mArray.size(); i++)
							fullCmd.add(mArray.get(i).toString());
					}

					if (!commander.isAlive()) {
						final JAliEnCOMMander comm = new JAliEnCOMMander(commander.getUser(), commander.getCurrentDir(), commander.getSite(), out);
						commander = comm;

						commander.start();
					}

					// Send the command to executor and send the result back to
					// client via OutputStream
					synchronized (commander) {
						commander.status.set(1);
						commander.setLine(out, fullCmd.toArray(new String[0]));
						commander.notifyAll();
					}
					waitCommandFinish();

					// Send back the result to the client
					synchronized (remoteEndpointBasic) {
						final ByteArrayOutputStream baos = (ByteArrayOutputStream) os;
						remoteEndpointBasic.sendText(baos.toString(), true);
						baos.reset();
					}
					_lastActivityTime = System.currentTimeMillis();
				}
			} catch (final IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (final Exception e) {
				e.printStackTrace();
			}
		}
	}
}
