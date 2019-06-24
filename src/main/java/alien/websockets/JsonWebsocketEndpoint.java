package alien.websockets;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

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

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
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
	static transient final Logger logger = ConfigUtils.getLogger(JsonWebsocketEndpoint.class.getCanonicalName());

	static transient final Monitor monitor = MonitorFactory.getMonitor(JsonWebsocketEndpoint.class.getCanonicalName());

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

	static final DelayQueue<SessionContext> sessionQueue = new DelayQueue<>();

	private static final class SessionContext implements Delayed {
		final Session session;
		final JsonWebsocketEndpoint endpoint;

		final long startTime = System.currentTimeMillis();
		long lastActivityTime = System.currentTimeMillis();

		final long absoluteRunningDeadline;

		public SessionContext(final JsonWebsocketEndpoint endpoint, final Session session, final long userCertExpiring) {
			this.endpoint = endpoint;
			this.session = session;

			absoluteRunningDeadline = Math.min(startTime + 2 * 24 * 60 * 60 * 1000L, userCertExpiring);
		}

		@Override
		public int compareTo(final Delayed other) {
			final long delta = getRunningDeadline() - ((SessionContext) other).getRunningDeadline();

			if (delta < 0)
				return -1;

			if (delta > 0)
				return 1;

			return 0;
		}

		final long getRunningDeadline() {
			return Math.min(absoluteRunningDeadline, lastActivityTime + 3 * 60 * 60 * 1000L);
		}

		@Override
		public long getDelay(TimeUnit unit) {
			final long delay = getRunningDeadline() - System.currentTimeMillis();

			return unit.convert(delay, TimeUnit.MILLISECONDS);
		}

		public void touch() {
			this.lastActivityTime = System.currentTimeMillis();
		}
	}

	static final Thread sessionCheckingThread = new Thread() {
		@Override
		public void run() {
			while (true) {
				try {
					SessionContext context = sessionQueue.take();

					if (context != null) {
						if (context.getRunningDeadline() <= System.currentTimeMillis()) {
							logger.log(Level.FINE, "Closing one idle / too long running session");
							context.endpoint.onClose(context.session, new CloseReason(null, "Session timed out"));

							monitor.incrementCounter("timedout_sessions");
						}
						else {
							logger.log(Level.SEVERE, "Session should still be kept in fact, deadline = " + context.getRunningDeadline() + " while now = " + System.currentTimeMillis());
							sessionQueue.add(context);
						}
					}
				}
				catch (@SuppressWarnings("unused") InterruptedException e) {
					// was told to exit
					return;
				}
			}
		}
	};

	static {
		sessionCheckingThread.setName("JsonWebsocketEndpoint.timeoutChecker");
		sessionCheckingThread.setDaemon(true);
		sessionCheckingThread.start();

		monitor.addMonitoring("sessions", (names, values) -> {
			names.add("active_sessions");
			values.add(Double.valueOf(sessionQueue.size()));
		});
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

		final SessionContext context = new SessionContext(this, session, commander.getUser().getUserCert()[0].getNotAfter().getTime());

		session.addMessageHandler(new EchoMessageHandlerText(context, commander, out, os));

		sessionQueue.add(context);

		monitor.incrementCounter("new_sessions");
	}

	@Override
	public void onClose(final Session session, final CloseReason closeReason) {
		monitor.incrementCounter("closed_sessions");

		commander.kill = true;

		out = null;
		try {
			if (os != null)
				os.close();
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		os = null;
		userIdentity = null;

		try {
			if (session != null) {
				final Iterator<SessionContext> it = sessionQueue.iterator();

				while (it.hasNext()) {
					final SessionContext sc = it.next();

					if (sc.session.equals(session))
						it.remove();
				}

				session.close();
			}
		}
		catch (IOException e) {
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

		private final SessionContext context;

		EchoMessageHandlerText(final SessionContext context, final JAliEnCOMMander commander, final UIPrintWriter out, OutputStream os) {
			this.context = context;
			this.remoteEndpointBasic = context.session.getBasicRemote();
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
				}
				catch (@SuppressWarnings("unused") final InterruptedException ie) {
					// ignore
				}
		}

		@Override
		public void onMessage(final String message, final boolean last) {
			monitor.incrementCounter("commands");

			try {
				if (remoteEndpointBasic != null) {
					// Try to parse incoming JSON
					Object pobj;
					JSONObject jsonObject;
					JSONParser parser = new JSONParser();

					try {
						pobj = parser.parse(new StringReader(message));
						jsonObject = (JSONObject) pobj;
					}
					catch (@SuppressWarnings("unused") ParseException e) {
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

					context.touch();
				}
			}
			catch (final IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			catch (final Exception e) {
				e.printStackTrace();
			}
		}
	}
}
