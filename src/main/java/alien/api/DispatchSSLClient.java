package alien.api;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import org.bouncycastle.jce.provider.BouncyCastleProvider;

import alien.config.ConfigUtils;
import alien.user.JAKeyStore;

/**
 * @author costing
 *
 */
public class DispatchSSLClient {
	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(DispatchSSLClient.class.getCanonicalName());

	private static final int defaultPort = 8098;
	private static final String defaultHost = "alice-jcentral.cern.ch";
	private static String serviceName = "apiService";

	private static String addr = null;
	private static int port = 0;

	private final Socket connection;

	private final ObjectInputStream ois;
	private final ObjectOutputStream oos;

	private final OutputStream os;

	/**
	 * E.g. the CE proxy should act as a fowarding bridge between JA and central services
	 *
	 * @param servName
	 *            name of the config parameter for the host:port settings
	 */
	public static void overWriteServiceAndForward(final String servName) {
		// TODO: we could drop the serviceName overwrite, once we assume to run
		// not on one single host everything
		serviceName = servName;
	}

	/**
	 * @param connection
	 * @throws IOException
	 */
	protected DispatchSSLClient(final Socket connection) throws IOException {
		this.connection = connection;

		connection.setTcpNoDelay(true);
		connection.setTrafficClass(0x10);
		connection.setSoLinger(false, 0);

		// client expect an answer promptly, the 15 minute default value should be large enough to accommodate even heavy requests
		connection.setSoTimeout(ConfigUtils.getConfig().geti("alien.api.DispatchSSLClient.readTimeout_seconds", 900) * 1000);

		this.ois = new ObjectInputStream(connection.getInputStream());

		this.os = connection.getOutputStream();

		this.oos = new ObjectOutputStream(this.os);
		this.oos.flush();
	}

	@Override
	public String toString() {
		return this.connection.getInetAddress().toString();
	}

	private static DispatchSSLClient instance = null;

	private static void connectTo(final List<InetAddress> allAddresses, final int targetPort, final SSLSocketFactory factory, final Object callback, final AtomicInteger connectionState) {
		// connect timeout in config should be given in seconds
		final int connectTimeout = ConfigUtils.getConfig().geti("alien.api.DispatchSSLClient.ConnectTimeout", 10) * 1000;

		DispatchSSLClient ret = null;

		for (final InetAddress endpointToTry : allAddresses) {
			try {
				if (connectionState.get() != 0) {
					logger.log(Level.FINEST, "The thread trying to connect to " + allAddresses + " was told to exit, currently having reached " + endpointToTry);

					return;
				}

				@SuppressWarnings("resource")
				// the socket will be passed along to the SSL one below, and to the connection cache
				final Socket s = new Socket();
				s.connect(new InetSocketAddress(endpointToTry, targetPort), connectTimeout);

				@SuppressWarnings("resource")
				// this object is kept in the map, cannot be closed here
				final SSLSocket client = (SSLSocket) factory.createSocket(s, endpointToTry.getHostAddress(), targetPort, true);

				// 10s to negociate SSL, if it takes more than this to connect just try another endpoint
				client.setSoTimeout(10 * 1000);

				// print info
				printSocketInfo(client, Level.FINE);

				client.startHandshake();

				final Certificate[] peerCerts = client.getSession().getPeerCertificates();

				if (peerCerts != null) {
					if (logger.isLoggable(Level.INFO)) {
						logger.log(Level.INFO, "Printing peer's information:");

						for (final Certificate peerCert : peerCerts) {
							final X509Certificate xCert = (X509Certificate) peerCert;

							logger.log(Level.INFO, "Peer's Certificate Information:\n" + Level.INFO, "- Subject: " + xCert.getSubjectDN().getName() + "\n" + xCert.getIssuerDN().getName() + "\n"
									+ Level.INFO + "- Start Time: " + xCert.getNotBefore().toString() + "\n" + Level.INFO + "- End Time: " + xCert.getNotAfter().toString());
						}
					}

					if (connectionState.compareAndSet(0, 1)) {
						final DispatchSSLClient sc = new DispatchSSLClient(client);
						System.out.println("Connection to JCentral (" + endpointToTry.getHostAddress() + ":" + targetPort + ") established.");

						ret = sc;
					}
					else {
						// somebody beat us and connected faster on another socket, close this slow one
						client.close();
					}

					break;
				}
				logger.log(Level.FINE, "We didn't get any peer/service cert from " + endpointToTry.getHostAddress() + ":" + targetPort);
			}
			catch (final ConnectException e) {
				logger.log(Level.FINE, "Could not connect to JCentral instance on " + endpointToTry.getHostAddress() + ":" + targetPort, e);
			}
			catch (final Throwable e) {
				logger.log(Level.FINE, "Could not initiate SSL connection to the server on " + endpointToTry.getHostAddress() + ":" + targetPort, e);
			}
		}

		synchronized (callback) {
			if (ret != null) {
				if (instance == null) {
					instance = ret;
					callback.notifyAll();
				}
				else {
					ret.close();
				}
			}
		}
	}

	/**
	 * @return instance
	 * @throws IOException
	 */
	private static DispatchSSLClient getInstance() throws IOException {
		if (instance == null) {
			initializeSocketInfo();

			// connect to the other end
			logger.log(Level.INFO, "Connecting to JCentral on " + addr + ":" + port);
			System.out.println("Connecting to JCentral on " + addr + ":" + port);

			Security.addProvider(new BouncyCastleProvider());

			final List<InetAddress> ipv4 = new ArrayList<>();
			final List<InetAddress> ipv6 = new ArrayList<>();

			try {
				final InetAddress[] resolvedAddresses = InetAddress.getAllByName(addr);

				if (resolvedAddresses == null || resolvedAddresses.length == 0) {
					logger.log(Level.SEVERE, "Empty address list for this hostname: " + addr);
					System.err.println("Empty address list for this hostname: " + addr);
					return null;
				}

				for (final InetAddress logAddress : resolvedAddresses) {
					if (logAddress instanceof Inet6Address)
						ipv6.add(logAddress);
					else
						ipv4.add(logAddress);
				}
			}
			catch (final IOException ex) {
				logger.log(Level.SEVERE, "Could not resolve IP address of central services (" + addr + ":" + port + ")", ex);
				System.err.println("Could not resolve IP address of central services (" + addr + ":" + port + "): " + ex.getMessage());
				return null;
			}

			Collections.shuffle(ipv6);
			Collections.shuffle(ipv4);

			final List<InetAddress> mainProtocol;
			final List<InetAddress> fallbackProtocol;

			if (ConfigUtils.getConfig().getb("alien.api.DispatchSSLClient.PreferIPv6", true)) {
				if (ipv6.size() > 0) {
					mainProtocol = ipv6;
					fallbackProtocol = ipv4;
				}
				else {
					mainProtocol = ipv4;
					fallbackProtocol = null;
				}
			}
			else if (ConfigUtils.getConfig().getb("alien.api.DispatchSSLClient.PreferIPv4", false)) {
				if (ipv4.size() > 0) {
					mainProtocol = ipv4;
					fallbackProtocol = ipv6;
				}
				else {
					mainProtocol = ipv6;
					fallbackProtocol = null;
				}
			}
			else {
				mainProtocol = new ArrayList<>();

				// inspired by rfc8305, interleave the protocols, giving a slight preference to IPv6
				final Iterator<InetAddress> it6 = ipv6.iterator();
				final Iterator<InetAddress> it4 = ipv4.iterator();

				while (it6.hasNext() || it4.hasNext()) {
					if (it6.hasNext())
						mainProtocol.add(it6.next());

					if (it4.hasNext())
						mainProtocol.add(it4.next());
				}

				fallbackProtocol = null;
			}

			if (logger.isLoggable(Level.FINER))
				logger.log(Level.FINER, "Will try to connect to the central services in the following order: " + mainProtocol + " then " + fallbackProtocol);

			final SSLSocketFactory f;
			try {
				// get factory
				final KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509", "SunJSSE");

				logger.log(Level.INFO, "Connecting with client cert: " + ((java.security.cert.X509Certificate) JAKeyStore.getKeyStore().getCertificateChain("User.cert")[0]).getSubjectDN());
				// initialize factory, with clientCert(incl. priv+pub)
				kmf.init(JAKeyStore.getKeyStore(), JAKeyStore.pass);

				java.lang.System.setProperty("jdk.tls.client.protocols", "TLSv1.2,TLSv1.3");
				final SSLContext ssc = SSLContext.getInstance("TLS");

				// initialize SSL with certificate and the trusted CA and pub certs
				ssc.init(kmf.getKeyManagers(), JAKeyStore.trusts, new SecureRandom());

				f = ssc.getSocketFactory();
			}
			catch (final Throwable t) {
				logger.log(Level.SEVERE, "Could not load the client certificate", t);
				System.err.println("Could not load the client certificate: " + t.getMessage());
				return null;
			}

			final Object callbackObject = new Object();

			final AtomicInteger connectionState = new AtomicInteger(0);

			final Thread tMain = new Thread(() -> connectTo(mainProtocol, port, f, callbackObject, connectionState));
			tMain.start();

			try {
				synchronized (callbackObject) {
					if (instance == null)
						callbackObject.wait(ConfigUtils.getConfig().geti("alien.api.DispatchSSLClient.happyEyeballsTimeout", 1000));
				}
			}
			catch (final InterruptedException ie) {
				throw new IOException("Connection was interrupted", ie);
			}

			// was either notified or timed out, let's see what is the last state
			if (instance != null)
				return instance;

			final Thread tFallback;

			// no connection could be established on the main protocol so far, let's try the fallback protocol, if available
			if (fallbackProtocol != null && fallbackProtocol.size() > 0) {
				logger.log(Level.FINE, "Could not establish a connection on the preferred protocol so far, will add the fallback solution to the mix");

				tFallback = new Thread(() -> connectTo(fallbackProtocol, port, f, callbackObject, connectionState));
				tFallback.start();
			}
			else
				tFallback = null;

			// wait up to a minute for a connection to be established
			for (int i = 0; i < ConfigUtils.getConfig().geti("alien.api.DispatchSSLClient.connectionTimeoutSteps", 60); i++) {
				try {
					synchronized (callbackObject) {
						if (instance == null)
							callbackObject.wait(ConfigUtils.getConfig().geti("alien.api.DispatchSSLClient.connectionTimeoutResolution", 1000));
					}
				}
				catch (final InterruptedException ie) {
					throw new IOException("Connection was interrupted", ie);
				}

				if (instance != null) {
					logger.log(Level.FINE, "Connection worked at step " + i);

					break;
				}
			}

			connectionState.set(2); // tell the threads to exit, if they haven't done so already

			if (tMain.isAlive())
				tMain.interrupt();

			if (tFallback != null && tFallback.isAlive())
				tFallback.interrupt();
		}

		return instance;
	}

	@SuppressWarnings("unused")
	private void close() {
		if (ois != null)
			try {
				ois.close();
			}
			catch (final IOException ioe) {
				// ignore
			}

		if (oos != null)
			try {
				oos.close();
			}
			catch (final IOException ioe) {
				// ignore
			}

		if (connection != null)
			try {
				connection.close();
			}
			catch (final IOException ioe) {
				// ignore
			}
	}

	/**
	 * Total amount of time (in milliseconds) spent in writing objects to the socket.
	 */
	private static long lSerialization = 0;

	private static synchronized void initializeSocketInfo() {
		addr = ConfigUtils.getConfig().gets(serviceName).trim();

		if (addr.length() == 0) {
			addr = defaultHost;
			port = defaultPort;
		}
		else {
			final String address = addr;
			final int idx = address.indexOf(':');

			if (idx >= 0)
				try {
					port = Integer.parseInt(address.substring(idx + 1));
					addr = address.substring(0, idx);
				}
				catch (@SuppressWarnings("unused") final Exception e) {
					addr = defaultHost;
					port = defaultPort;
				}
			else
				port = defaultPort;
		}
	}

	/**
	 * @param r
	 * @return the reply, or <code>null</code> in case of connectivity problems
	 * @throws ServerException
	 */
	public static synchronized <T extends Request> T dispatchRequest(final T r) throws ServerException {
		try {
			return dispatchARequest(r);
		}
		catch (@SuppressWarnings("unused") final IOException e) {
			// Now let's try, if we can reconnect
			if (instance != null) {
				instance.close();
				instance = null;
			}

			try {
				return dispatchARequest(r);
			}
			catch (final IOException e1) {
				// This time we give up
				logger.log(Level.SEVERE, "Error running request, potential connection error.", e1);
				return null;
			}
		}
	}

	/**
	 * @param r
	 * @return the processed request, if successful
	 * @throws IOException
	 *             in case of connectivity problems
	 * @throws ServerException
	 *             if the server didn't like the request content
	 */
	public static synchronized <T extends Request> T dispatchARequest(final T r) throws IOException, ServerException {
		final DispatchSSLClient c = getInstance();

		if (c == null)
			throw new IOException("Connection is null");

		final long lStart = System.currentTimeMillis();

		c.oos.writeUnshared(r);

		c.oos.flush();

		lSerialization += System.currentTimeMillis() - lStart;

		Object o;
		try {
			o = c.ois.readObject();
		}
		catch (final ClassNotFoundException e) {
			throw new IOException(e);
		}

		if (o == null) {
			logger.log(Level.WARNING, "Read a null object as reply to this request: " + r);
			return null;
		}

		if (logger.isLoggable(Level.FINE)) {
			logger.log(Level.FINE, "Got back an object of type " + o.getClass().getCanonicalName() + " : " + o);

			if (logger.isLoggable(Level.FINEST))
				logger.log(Level.FINEST, "Call stack is: ", new Throwable());
		}

		@SuppressWarnings("unchecked")
		final T reply = (T) o;

		reply.setPartnerAddress(c.connection.getInetAddress());

		final ServerException ex = reply.getException();

		if (ex != null)
			throw ex;

		return reply;
	}

	/**
	 * @return total time in milliseconds spent in serializing objects
	 */
	public static long getSerializationTime() {
		return lSerialization;
	}

	private static void printSocketInfo(final SSLSocket s, final Level level) {
		if (logger.isLoggable(level)) {
			logger.log(level, "Remote address: " + s.getInetAddress().toString() + ":" + s.getPort());
			logger.log(level, "   Local socket address = " + s.getLocalSocketAddress().toString());

			logger.log(level, "   Cipher suite = " + s.getSession().getCipherSuite());
			logger.log(level, "   Protocol = " + s.getSession().getProtocol());
		}
	}

}
