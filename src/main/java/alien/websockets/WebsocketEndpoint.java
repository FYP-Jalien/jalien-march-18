package alien.websockets;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.websocket.CloseReason;
import javax.websocket.CloseReason.CloseCodes;
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
import alien.monitoring.CacheMonitor;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import alien.shell.commands.JAliEnCOMMander;
import alien.shell.commands.JSONPrintWriter;
import alien.shell.commands.JShPrintWriter;
import alien.shell.commands.UIPrintWriter;
import alien.shell.commands.XMLPrintWriter;
import alien.user.AliEnPrincipal;
import lazyj.Utils;
import lazyj.cache.ExpirationCache;

/**
 * @author vyurchen
 *
 *         Implementation of websocket endpoint, that supports plain text and JSON clients
 */
public class WebsocketEndpoint extends Endpoint {
	private static final Logger logger = ConfigUtils.getLogger(WebsocketEndpoint.class.getCanonicalName());

	private static final Monitor monitor = MonitorFactory.getMonitor(WebsocketEndpoint.class.getCanonicalName());

	private static final CacheMonitor ipv6Connections;

	static {
		if (monitor != null)
			ipv6Connections = monitor.getCacheMonitor("ipv6_connections");
		else
			ipv6Connections = null;
	}

	private AliEnPrincipal userIdentity = null;

	/**
	 * Commander
	 */
	JAliEnCOMMander commander = null;

	private UIPrintWriter out = null;
	private OutputStream os = null;

	private void setShellPrintWriter(final OutputStream os, final String shelltype) {
		if (shelltype.equals("plain"))
			out = new JShPrintWriter(os);
		else if (shelltype.equals("json"))
			out = new JSONPrintWriter(os);
		else
			out = new XMLPrintWriter(os);
	}

	private static final DelayQueue<SessionContext> sessionQueue = new DelayQueue<>();

	private static final class SessionContext implements Delayed {
		final Session session;
		final WebsocketEndpoint endpoint;

		final long startTime = System.currentTimeMillis();
		long lastActivityTime = System.currentTimeMillis();

		final long absoluteRunningDeadline;

		public SessionContext(final WebsocketEndpoint endpoint, final Session session, final long userCertExpiring) {
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

		@Override
		public boolean equals(Object obj) {
			return super.equals(obj);
		}

		@Override
		public int hashCode() {
			return endpoint.hashCode() + session.hashCode();
		}

		final long getRunningDeadline() {
			return Math.min(absoluteRunningDeadline, lastActivityTime + 15 * 60 * 1000L);
		}

		@Override
		public long getDelay(final TimeUnit unit) {
			final long delay = getRunningDeadline() - System.currentTimeMillis();

			return unit.convert(delay, TimeUnit.MILLISECONDS);
		}

		public void touch() {
			this.lastActivityTime = System.currentTimeMillis();
		}
	}

	private static final Thread sessionCheckingThread = new Thread() {
		@Override
		public void run() {
			while (true) {
				try {
					final SessionContext context = sessionQueue.take();

					if (context != null) {
						if (context.getRunningDeadline() <= System.currentTimeMillis()) {
							logger.log(Level.FINE, "Closing one idle / too long running session");
							context.endpoint.onClose(context.session, new CloseReason(CloseCodes.TRY_AGAIN_LATER, "Session timed out"));

							monitor.incrementCounter("timedout_sessions");
						}
						else {
							logger.log(Level.SEVERE, "Session should still be kept in fact, deadline = " + context.getRunningDeadline() + " while now = " + System.currentTimeMillis());
							sessionQueue.add(context);
						}
					}
				}
				catch (@SuppressWarnings("unused") final InterruptedException e) {
					// was told to exit
					Thread.currentThread().interrupt(); // restore interrupt
					return;
				}
			}
		}
	};

	static {
		disableAccessWarnings();

		sessionCheckingThread.setName("JsonWebsocketEndpoint.timeoutChecker");
		sessionCheckingThread.setDaemon(true);
		sessionCheckingThread.start();

		if (monitor != null)
			monitor.addMonitoring("sessions", (names, values) -> {
				names.add("active_sessions");
				values.add(Double.valueOf(sessionQueue.size()));
			});
	}

	/**
	 * @return information about the known active connections
	 */
	public static Collection<WebSocketInfo> getActiveConnections() {
		final List<WebSocketInfo> ret = new ArrayList<>(sessionQueue.size());

		for (SessionContext ctx : sessionQueue) {
			final WebsocketEndpoint endpoint = ctx.endpoint;

			if (endpoint != null) {
				final JAliEnCOMMander cmd = endpoint.commander;

				if (cmd != null) {
					final AliEnPrincipal user = cmd.getUser();

					ret.add(new WebSocketInfo(user.getDefaultUser(), user.getRemoteEndpoint(), user.getRemotePort(), ctx.startTime, ctx.lastActivityTime));
				}
			}
		}

		return ret;
	}

	@Override
	public void onOpen(final Session session, final EndpointConfig endpointConfig) {
		final Principal userPrincipal = session.getUserPrincipal();
		userIdentity = (AliEnPrincipal) userPrincipal;

		os = new ByteArrayOutputStream();
		final ServerEndpointConfig serverConfig = (ServerEndpointConfig) endpointConfig;
		if (serverConfig.getPath().equals("/websocket/json"))
			setShellPrintWriter(os, "json");
		else
			setShellPrintWriter(os, "plain");

		final InetSocketAddress remoteIPandPort = getRemoteIP(session);

		final String remoteHostAddress;

		if (remoteIPandPort != null) {
			final InetAddress remoteIP = remoteIPandPort.getAddress();

			if (ipv6Connections != null) {
				if (remoteIP instanceof Inet6Address)
					ipv6Connections.incrementHits();
				else
					ipv6Connections.incrementMisses();
			}

			userIdentity.setRemoteEndpoint(remoteIP);
			userIdentity.setRemotePort(remoteIPandPort.getPort());

			remoteHostAddress = remoteIP.getHostAddress();
		}
		else
			remoteHostAddress = null;

		commander = new JAliEnCOMMander(userIdentity, null, getSite(remoteHostAddress), null, serverConfig.getUserProperties());

		final SessionContext context = new SessionContext(this, session, commander.getUser().getUserCert()[0].getNotAfter().getTime());

		session.addMessageHandler(new WSMessageHandler(context, commander, out, os));

		// Safety net, let the API also close idle connections, at a much later time than our explicit operation
		// The default socket killer doesn't close the underlying socket, leading to server sockets piling up
		session.setMaxIdleTimeout(6 * 60 * 60 * 1000L);

		sessionQueue.add(context);

		monitor.incrementCounter("new_sessions");
	}

	private static ExpirationCache<String, String> closeSiteCache = new ExpirationCache<>(2000);

	/**
	 * Get the client's closest site
	 *
	 * @param ip IP address of the client
	 * @return the name of the closest site
	 */
	private static String getSite(final String ip) {
		if (ip == null) {
			logger.log(Level.SEVERE, "Client IP address is unknown");
			return null;
		}

		final String cacheValue = closeSiteCache.get(ip);

		if (cacheValue != null)
			return cacheValue;

		try {
			final String site = Utils.download("http://alimonitor.cern.ch/services/getClosestSite.jsp?ip=" + ip, null);

			if (logger.isLoggable(Level.FINE))
				logger.log(Level.FINE, "Client IP address " + ip + " mapped to " + site);

			if (site != null) {
				final String newValue = site.trim();

				closeSiteCache.put(ip, newValue, 1000L * 60 * 15);

				return newValue;
			}
		}
		catch (final IOException ioe) {
			logger.log(Level.SEVERE, "Cannot get the closest site information for " + ip, ioe);
		}

		return null;
	}

	/**
	 * Get the IP address of the client using reflection of the socket object
	 *
	 * @param session websocket session which contains the socket
	 * @return IP address and port
	 */
	private static InetSocketAddress getRemoteIP(final Session session) {
		try {
			Object obj = session.getAsyncRemote();

			for (final String fieldName : new String[] { "base", "socketWrapper", "socket", "sc", "remoteAddress" }) {
				obj = getField(obj, fieldName);

				if (obj == null)
					return null;
			}

			return (InetSocketAddress) obj;
		}
		catch (final Exception e) {
			logger.log(Level.SEVERE, "Cannot extract the remote IP address from a session", e);
		}

		return null;
	}

	private static Object getField(final Object obj, final String fieldName) {
		Class<?> objClass = obj.getClass();

		for (; objClass != Object.class; objClass = objClass.getSuperclass()) {
			try {
				Field field;
				field = objClass.getDeclaredField(fieldName);
				field.setAccessible(true);
				return field.get(obj);
			}
			catch (@SuppressWarnings("unused") final Exception e) {
				// ignore
			}
		}

		return null;
	}

	/**
	 * Disable the reflection access warning produced by getRemoteIP(Session) accessing field <code>sun.nio.ch.SocketChannelImpl.remoteAddress</code>
	 */
	private static void disableAccessWarnings() {
		try {
			final Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
			final Field field = unsafeClass.getDeclaredField("theUnsafe");
			field.setAccessible(true);
			final Object unsafe = field.get(null);

			final Method putObjectVolatile = unsafeClass.getDeclaredMethod("putObjectVolatile", Object.class, long.class, Object.class);
			final Method staticFieldOffset = unsafeClass.getDeclaredMethod("staticFieldOffset", Field.class);

			final Class<?> loggerClass = Class.forName("jdk.internal.module.IllegalAccessLogger");
			final Field loggerField = loggerClass.getDeclaredField("logger");
			final Long offset = (Long) staticFieldOffset.invoke(unsafe, loggerField);
			putObjectVolatile.invoke(unsafe, loggerClass, offset, null);
		}
		catch (final Exception e) {
			logger.log(Level.FINE, "Could not disable warnings regarding access to sun.nio.ch.SocketChannelImpl.remoteAddress", e);
		}
	}

	@Override
	public void onClose(final Session session, final CloseReason closeReason) {
		monitor.incrementCounter("closed_sessions");

		if (logger.isLoggable(Level.FINE))
			logger.log(Level.FINE, "Closing session of commander ID " + commander.commanderId + ", reason is " + closeReason + ", session = " + session + ", was opened: " + session.isOpen());

		commander.shutdown();

		out = null;
		try {
			if (os != null)
				os.close();
		}
		catch (final IOException e) {
			logger.log(Level.SEVERE, "Exception closing session output stream", e);
		}

		os = null;
		userIdentity = null;

		try {
			int removedCount = 0;

			if (session != null) {
				final Iterator<SessionContext> it = sessionQueue.iterator();

				while (it.hasNext()) {
					final SessionContext sc = it.next();

					if (sc.session.equals(session)) {
						it.remove();
						removedCount++;
					}
				}

				((org.apache.tomcat.websocket.WsSession) session).doClose(closeReason, closeReason, true);
			}

			if (logger.isLoggable(Level.FINE))
				logger.log(Level.FINE, "Removed " + removedCount + " queued entries for commander ID " + commander.commanderId + ", reason is " + closeReason + ", session = " + session
						+ (session != null ? ", now is opened: " + session.isOpen() : ""));
		}
		catch (final Exception e) {
			logger.log(Level.SEVERE, "Exception closing session", e);
		}
	}

	@Override
	public void onError(final Session session, final Throwable thr) {
		//
	}

	private static class WSMessageHandler implements MessageHandler.Partial<String> {
		private final RemoteEndpoint.Basic remoteEndpointBasic;

		private JAliEnCOMMander commander = null;
		private UIPrintWriter out = null;
		private OutputStream os = null;

		private final SessionContext context;

		WSMessageHandler(final SessionContext context, final JAliEnCOMMander commander, final UIPrintWriter out, final OutputStream os) {
			this.context = context;
			this.remoteEndpointBasic = context.session.getBasicRemote();
			this.commander = commander;
			this.out = out;
			this.os = os;
		}

		@Override
		public void onMessage(final String message, final boolean last) {
			monitor.incrementCounter("commands");

			if (remoteEndpointBasic == null)
				return;

			context.touch();

			ArrayList<String> fullCmd;

			// Parse incoming command
			try {
				if (this.out.getClass().getCanonicalName().equals("alien.shell.commands.JSONPrintWriter")) {
					fullCmd = parseJSON(message);
				}
				else if (this.out.getClass().getCanonicalName().equals("alien.shell.commands.JShPrintWriter")) {
					fullCmd = parsePlainText(message);
				}
				else {
					// this is XMLPrintWriter or some other type of writer
					logger.log(Level.SEVERE, "Tried to use unsopported writer " + this.out.getClass().getCanonicalName() + " in the websocket endpoint");
					return;
				}
			}
			catch (final IOException e) {
				// Failed to send back the reply
				e.printStackTrace();
				logger.log(Level.SEVERE, "Websocket failed to send back the reply: " + e.getMessage());
				return;
			}
			catch (@SuppressWarnings("unused") final IllegalArgumentException e) {
				// Illegal command. Details given by parse method
				return;
			}

			try (Timing t = new Timing(monitor, "execution_time")) {
				// Send the command to executor and send the result back to
				// client via OutputStream
				commander.setLine(out, fullCmd.toArray(new String[0]));

				// Wait and return the result back to the client
				waitForResult();
			}

			context.touch();
		}

		/**
		 * Parse incoming JSON command
		 *
		 * @param message a string in JSON format
		 * @return command and it's arguments as an array
		 */
		private ArrayList<String> parseJSON(final String message) throws IOException, IllegalArgumentException {
			final ArrayList<String> fullCmd = new ArrayList<>();
			Object pobj;
			JSONObject jsonObject;
			final JSONParser parser = new JSONParser();

			try {
				pobj = parser.parse(new StringReader(message));
				jsonObject = (JSONObject) pobj;
			}
			catch (@SuppressWarnings("unused") final ParseException e) {
				synchronized (remoteEndpointBasic) {
					remoteEndpointBasic.sendText("{\"metadata\":{\"exitcode\":\"-1\",\"error\":\"Incoming JSON not ok\"},\"results\":[]}", true);
				}
				throw new IllegalArgumentException();
			}

			// Filter out cp commands
			if (jsonObject.get("command").toString().equals("cp")) {
				synchronized (remoteEndpointBasic) {
					remoteEndpointBasic.sendText(
							"{\"metadata\":{\"exitcode\":\"-1\",\"error\":\"'cp' grid command is not implemented. Please use native client's Cp() method\"},\"results\":[]}", true);
				}
				throw new IllegalArgumentException();
			}

			// Split JSONObject into strings
			fullCmd.add(jsonObject.get("command").toString());

			if (jsonObject.get("options") != null) {
				final JSONArray mArray = (JSONArray) jsonObject.get("options");

				for (final Object element : mArray)
					fullCmd.add(element.toString());
			}

			return fullCmd;
		}

		/**
		 * Parse incoming plain text command
		 *
		 * @param message whitespace-separated string that contains a command and args
		 * @return command and it's arguments as an array
		 */
		private ArrayList<String> parsePlainText(final String message) throws IOException, IllegalArgumentException {
			final ArrayList<String> fullCmd = new ArrayList<>();

			final StringTokenizer st = new StringTokenizer(message, " ");

			while (st.hasMoreTokens())
				fullCmd.add(st.nextToken());

			// Filter out cp commands
			if (fullCmd.get(0).equals("cp")) {
				synchronized (remoteEndpointBasic) {
					remoteEndpointBasic.sendText("'cp' grid command is not implemented. Please use native client's Cp() method", true);
				}
				throw new IllegalArgumentException();
			}

			return fullCmd;
		}

		/**
		 * Wait for the current command to finish and return the result to the remote client.
		 * Creates a new thread if the command takes more than 1 second to be executed to unblock websocket
		 * endpoint and let it respond to ping
		 */
		private void waitForResult() {
			if (commander == null)
				return;

			if (!waitForCommand(true, 1)) {
				// If a command takes too long to be executed, start a new thread
				final ExecutorService commandService = Executors.newSingleThreadExecutor();

				monitor.incrementCounter("long_command", 1);

				commandService.execute(() -> {
					waitForCommand(false, 1);
					returnResult();
				});
			}
			else {
				returnResult();
				monitor.incrementCounter("short_command", 1);
			}
		}

		/**
		 * Check the command status to tell if it is done
		 *
		 * @param oneShot set to <code>true</code> if you want to check the status once and exit
		 * @param seconds the interval of polling for the command status
		 * @return <code>true</code> if the command is done
		 */
		private boolean waitForCommand(final boolean oneShot, final int seconds) {
			final long lStart = System.currentTimeMillis();

			try {
				while (commander.status.get() == 1) {
					if (!oneShot)
						Thread.currentThread().setName("WS: waiting for commander " + commander.commanderId + " for " + (System.currentTimeMillis() - lStart) / 1000 + "s");

					synchronized (commander.status) {
						try {
							commander.status.wait(seconds * 1000);
						}
						catch (@SuppressWarnings("unused") final InterruptedException e) {
							commander.shutdown();
							break;
						}
					}

					if (oneShot)
						break;
				}
			}
			finally {
				if (!oneShot)
					Thread.currentThread().setName("WS: commander " + commander.commanderId + " completed after " + (System.currentTimeMillis() - lStart) + "ms");
			}

			return commander.status.get() == 0;
		}

		/**
		 * Send the result string to the remote client
		 */
		private void returnResult() {
			synchronized (remoteEndpointBasic) {
				final ByteArrayOutputStream baos = (ByteArrayOutputStream) os;
				try {
					remoteEndpointBasic.sendText(baos.toString(), true);
				}
				catch (final IOException e) {
					e.printStackTrace();
				}
				baos.reset();
			}
		}
	}
}
