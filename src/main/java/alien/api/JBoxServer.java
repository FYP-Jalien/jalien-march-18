package alien.api;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import alien.config.ConfigUtils;
import alien.se.SEUtils;
import alien.shell.commands.JAliEnCOMMander;
import alien.shell.commands.JShPrintWriter;
import alien.shell.commands.UIPrintWriter;
import alien.shell.commands.XMLPrintWriter;

/**
 * Simple UI server to be used by ROOT and command line
 *
 * @author costing
 */
public class JBoxServer extends Thread {

	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(JBoxServer.class.getCanonicalName());

	// this triggers to ask for the user home LFN before doing anything else
	private static boolean preemptJCentralConnection = true;

	/**
	 *
	 */
	public static final String passACK = "OKPASSACK";

	/**
	 * static{ new }
	 */
	public static final String passNOACK = "NOPASSACK";

	private final int port;

	private final ServerSocket ssocket;

	private UIConnection connection;

	/**
	 * The password
	 */
	private static String password;

	/**
	 * Number of currently established client connections to this instance
	 */
	final static AtomicInteger connectedClients = new AtomicInteger(0);

	/**
	 * Timestamp of the last operation (connect / disconnect / command)
	 */
	static long lastOperation = System.currentTimeMillis();

	/**
	 * On any activity update the {@link #lastOperation} field
	 */
	static void notifyActivity() {
		lastOperation = System.currentTimeMillis();
	}

	private static final class ShutdownThread extends Thread {
		public ShutdownThread() {
			setName("Idle server watcher");
		}

		@Override
		public void run() {
			while (true) {
				if (connectedClients.get() == 0 && (System.currentTimeMillis() - lastOperation) > 1000 * 60 * 60 * 24 && server != null)
					server.shutdown();

				try {
					Thread.sleep(1000 * 60);
				}
				catch (@SuppressWarnings("unused") final InterruptedException ie) {
					// it's interrupted when it should exit
					break;
				}
			}
		}
	}

	private static ShutdownThread shutdownThread = null;

	private static synchronized void preempt() {
		if (preemptJCentralConnection) {
			final PreemptJCentralConnection preempt = new PreemptJCentralConnection();
			preempt.start();
			preemptJCentralConnection = false;
		}
	}

	private static synchronized void startShutdownThread() {
		if (shutdownThread == null) {
			shutdownThread = new ShutdownThread();

			shutdownThread.start();
		}
	}

	/**
	 * Start the server on a given port
	 *
	 * @param listeningPort
	 * @throws IOException
	 */
	private JBoxServer(final int listeningPort) throws Exception {

		final InetAddress localhost = InetAddress.getByName("127.0.0.1");

		ssocket = new ServerSocket(listeningPort, 10, localhost);
		this.port = ssocket.getLocalPort();
		setPassword(UUID.randomUUID().toString());

		startShutdownThread();
	}

	/**
	 * ramp up the ssl to JCentral
	 *
	 * @author gron
	 */
	static final class PreemptJCentralConnection extends Thread {
		@Override
		public void run() {
			if (SEUtils.getSE(0) == null) {
				logger.log(Level.SEVERE, "JBoxServer: Error running request, potential connection error.");
				System.err.println("JBoxServer: Error running request, potential connection error.");
			}
		}
	}

	/**
	 * One UI connection
	 *
	 * @author costing
	 */
	private class UIConnection extends Thread {

		private final Socket s;

		private final InputStream is;

		private final OutputStream os;

		private JAliEnCOMMander commander = null;

		/**
		 * One UI connection identified by the socket
		 *
		 * @param s
		 * @throws IOException
		 */
		public UIConnection(final Socket s) throws IOException {
			this.s = s;

			is = s.getInputStream();
			os = s.getOutputStream();

			setName("UIConnection: " + s.getInetAddress());
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

		private UIPrintWriter out = null;

		private void setShellPrintWriter(final OutputStream os, final String shelltype) {
			if ("jaliensh".equals(shelltype))
				out = new JShPrintWriter(os);
			else
				out = new XMLPrintWriter(os);
		}

		@Override
		public void run() {
			connectedClients.incrementAndGet();

			notifyActivity();

			BufferedReader br = null;

			try {
				// String sCmdDebug = "";
				String sCmdValue = "";

				String sLine = "";

				// Get the DOM Builder Factory
				final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
				// Get the DOM Builder
				final DocumentBuilder builder = factory.newDocumentBuilder();
				// Load and Parse the XML document
				// document contains the complete XML as a Tree.
				br = new BufferedReader(new InputStreamReader(is));
				String sCommand = "";

				while ((sLine = br.readLine()) != null)
					if (sLine.startsWith("<document>"))
						sCommand = sLine;
					else if (sLine.endsWith("</document>")) {
						sCommand += sLine;
						final ArrayList<String> cmdOptions = new ArrayList<>();
						final ArrayList<String> fullCmd = new ArrayList<>();
						try {

							// <document>
							// <ls>
							// <o>-l</o>
							// <o>-a</o>
							// <o>/alice/cern.ch/user/t/ttothova</o>
							// </ls>
							// </document>
							logger.info("XML =\"" + sCommand + "\"");
							final Document document = builder.parse(new InputSource(new StringReader(sCommand)));

							final NodeList commandNodeList = document.getElementsByTagName("command");

							if (commandNodeList != null && commandNodeList.getLength() == 1) {
								final Node commandNode = commandNodeList.item(0);
								sCmdValue = commandNode.getTextContent();
								fullCmd.add(sCmdValue);
								logger.info("Received command " + sCmdValue);

								final NodeList optionsNodeList = document.getElementsByTagName("o");

								final int nodeCount = optionsNodeList.getLength();

								for (int i = 0; i < nodeCount; i++) {
									final Node optionNode = optionsNodeList.item(i);
									cmdOptions.add(optionNode.getTextContent());
									fullCmd.add(optionNode.getTextContent());
									logger.info("Command options = " + optionNode.getTextContent());
								}

								if ("password".equals(sCmdValue)) {

									if (cmdOptions.get(0).equals(password)) {
										os.write(passACK.getBytes());
										os.flush();
									}
									else {
										os.write(passNOACK.getBytes());
										os.flush();
										return;
									}
								}
								else {
									logger.log(Level.INFO, "JSh connected.");

									if (commander == null)
										commander = new JAliEnCOMMander();

									notifyActivity();

									if ("SIGINT".equals(sLine)) {
										logger.log(Level.INFO, "Received [SIGINT] from JSh.");

										try {
											commander.shutdown();
										}
										catch (@SuppressWarnings("unused") final Throwable t) {
											// ignore
										}
										finally {
											System.out.println("SIGINT reset commander");

											// kill the active command and start a new instance
											final JAliEnCOMMander comm = new JAliEnCOMMander(commander.getUser(), commander.getCurrentDir(), commander.getSite(), out);
											commander = comm;

											commander.flush();
										}
									}
									else if ("shutdown".equals(sLine))
										shutdown();
									else {
										waitCommandFinish();

										synchronized (commander) {
											if ("setshell".equals(sCmdValue) && cmdOptions.size() > 0) {
												setShellPrintWriter(os, cmdOptions.get(0));
												logger.log(Level.INFO, "Set explicit print writer: " + cmdOptions.get(0));

												os.write((JShPrintWriter.streamend + "\n").getBytes());
												os.flush();
												continue;
											}

											if (out == null)
												out = new XMLPrintWriter(os);

											commander.setLine(out, fullCmd.toArray(new String[0]));
										}
									}
									os.flush();
								}
							}
							else
								logger.severe("Received more than one command");
							// some error, there was more than one command
							// attached to the document
						}
						catch (final Exception e) {
							logger.log(Level.SEVERE, "Parse error", e);
						}
					}
					else
						sCommand += "\n" + sLine;

			}
			catch (final Throwable e) {
				logger.log(Level.INFO, "Error running the commander.", e);
			}
			finally {
				if (commander != null)
					commander.shutdown();

				if (br != null)
					try {
						br.close();
					}
					catch (@SuppressWarnings("unused") final IOException ioe) {
						// ignore
					}

				connectedClients.decrementAndGet();

				notifyActivity();

				try {
					s.shutdownOutput();
				}
				catch (@SuppressWarnings("unused") final Exception e) {
					// nothing particular
				}
				try {
					s.shutdownInput();
				}
				catch (@SuppressWarnings("unused") final Exception e) {
					// ignore
				}
				try {
					s.close();
				}
				catch (@SuppressWarnings("unused") final Exception e) {
					// ignore
				}
			}
		}

	}

	private boolean alive = true;

	/**
	 * Kill the JBox instance
	 */
	void shutdown() {

		logger.log(Level.FINE, "Received [shutdown] from JSh.");

		alive = false;

		try {
			ssocket.close();
		}
		catch (@SuppressWarnings("unused") final IOException e) {
			// ignore, we're dead anyway
		}

		if (shutdownThread != null)
			shutdownThread.interrupt();

		logger.log(Level.INFO, "JBox: We die gracefully...Bye!");
		System.exit(0);

	}

	@Override
	public void run() {
		while (alive)
			try {
				@SuppressWarnings("resource")
				final Socket s = ssocket.accept();

				connection = new UIConnection(s);

				connection.start();
			}
			catch (final Exception e) {
				if (alive)
					logger.log(Level.WARNING, "Cannot use socket: ", e.getMessage());
			}
	}

	/**
	 * Singleton
	 */
	static JBoxServer server = null;

	/**
	 * Start once the UIServer
	 */
	public static synchronized void startJBoxServer() {

		logger.log(Level.INFO, "JBox starting ...");

		if (server != null)
			return;

		preempt();

		// Get port range from config
		final boolean portAny = ConfigUtils.getConfig().getb("port.range.any", true);

		final int randomStartingPort = ThreadLocalRandom.current().nextInt(1000);

		final int portMin = ConfigUtils.getConfig().geti("port.range.start", 10100 + randomStartingPort);
		final int portMax = ConfigUtils.getConfig().geti("port.range.end", portAny ? portMin + 1 : 200);

		for (int port = portMin; port < portMax; port++)
			try {
				server = new JBoxServer(portAny ? 0 : port);
				server.start();

				logger.log(Level.INFO, "JBox listening on port " + server.port);
				System.out.println("JBox is listening on port " + server.port);

				break;
			}
			catch (final Exception ioe) {
				// we don't need the already in use info on the port, maybe
				// there's another user on the machine...
				logger.log(Level.FINE, "JBox: Could not listen on port " + String.valueOf(portAny ? 0 : port), ioe);
			}
	}

	/**
	 *
	 * Load necessary keys and start JBoxServer
	 */
	public static void startJBoxService() {
		logger.log(Level.INFO, "Starting JBox");
		System.err.println(passACK);
		JBoxServer.startJBoxServer();
	}

	/**
	 *
	 * Get the port used by JBoxServer
	 *
	 * @return the TCP port this server is listening on. Can be negative to signal that the server is actually not listening on any port (yet?)
	 *
	 */
	public static int getPort() {
		return server != null ? server.port : -1;
	}

	/**
	 * @return the random password set for this session, to share with the local clients
	 */
	public static String getPassword() {
		return password != null ? password : "";
	}

	private static void setPassword(final String password2set) {
		password = password2set;
	}
}
