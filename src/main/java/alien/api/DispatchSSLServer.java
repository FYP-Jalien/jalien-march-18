package alien.api;

import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.security.KeyStoreException;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;

import org.bouncycastle.jce.provider.BouncyCastleProvider;

import alien.catalogue.access.AuthorizationFactory;
import alien.config.ConfigUtils;
import alien.log.RequestEvent;
import alien.monitoring.CacheMonitor;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import alien.shell.ErrNo;
import alien.user.AliEnPrincipal;
import alien.user.JAKeyStore;
import alien.user.UserFactory;
import lazyj.Format;
import utils.CachedThreadPool;

/**
 * @author costing
 *
 */
public class DispatchSSLServer extends Thread {
	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(DispatchSSLServer.class.getCanonicalName());

	/**
	 * Service monitoring
	 */
	static final Monitor monitor = MonitorFactory.getMonitor(DispatchSSLServer.class.getCanonicalName());

	/**
	 * The entire connection
	 */
	private final Socket connection;

	/**
	 * Getting requests by this stream
	 */
	private ObjectInputStream ois;

	/**
	 * Writing replies here
	 */
	private ObjectOutputStream oos;

	private OutputStream os;

	private long lastActivity = System.currentTimeMillis();

	private X509Certificate partnerCerts[] = null;

	private static final int defaultPort = 8098;
	private static String serviceName = "apiService";

	private static boolean forwardRequest = false;

	private static AtomicInteger activeSessions = new AtomicInteger();

	private static ConcurrentHashMap<DispatchSSLServer, DispatchSSLServer> sessionMap = new ConcurrentHashMap<>();

	private static final CachedThreadPool acceptorPool = new CachedThreadPool(ConfigUtils.getConfig().geti("alien.api.DispatchSSLServer.maxAcceptorThreads", 16), 10, TimeUnit.SECONDS,
			(r) -> new Thread(r, "SSLAcceptor"));

	private static CacheMonitor ipv6Connections = null;

	private static InetAddress actualServerAddress = null;
	private static int actualServerPort = -1;

	static {
		if (monitor != null) {
			monitor.addMonitoring("activeSessions", (names, values) -> {
				names.add("activeSessions");
				values.add(Double.valueOf(activeSessions.get()));

				names.add("sessionMapSize");
				values.add(Double.valueOf(sessionMap.size()));

				names.add("acceptorPoolSize");
				values.add(Double.valueOf(acceptorPool.getPoolSize()));

				names.add("acceptorPoolQueueLength");
				values.add(Double.valueOf(acceptorPool.getQueue().size()));
			});

			ipv6Connections = monitor.getCacheMonitor("ipv6_connections");
		}
	}

	/**
	 * E.g. the CE proxy should act as a forwarding bridge between JA and central services
	 *
	 * @param servName
	 *            name of the config parameter for the host:port settings
	 */
	public static void overWriteServiceAndForward(final String servName) {
		// TODO: we could drop the serviceName overwrite, once we assume to run
		// not on one single host everything
		serviceName = servName;
		forwardRequest = true;
	}

	/**
	 * @param connection
	 */
	public DispatchSSLServer(final Socket connection) {
		this.connection = connection;

		setName(connection.getInetAddress().toString());
		setDaemon(true);
	}

	@Override
	public void run() {
		try {
			try {
				connection.setTcpNoDelay(true);
				connection.setTrafficClass(0x10);

				// clients that did not send any command for a long time (default 15 minutes) are disconnected
				connection.setSoTimeout(ConfigUtils.getConfig().geti("alien.api.DispatchSSLServer.idleTimeout_seconds", 900) * 1000);

				this.os = connection.getOutputStream();

				this.oos = new ObjectOutputStream(this.os);
				this.oos.flush();
				this.os.flush();

				this.ois = new ObjectInputStream(connection.getInputStream());
			}
			catch (final IOException e) {
				logger.log(Level.SEVERE, "Exception initializing the SSL socket", e);
				return;
			}

			final AliEnPrincipal remoteIdentity = UserFactory.getByCertificate(partnerCerts);

			if (remoteIdentity == null) {
				logger.log(Level.WARNING, "Could not get the identity of this certificate chain: " + Arrays.toString(partnerCerts));
				return;
			}

			remoteIdentity.setRemoteEndpoint((InetSocketAddress) connection.getRemoteSocketAddress());

			try (RequestEvent event = new RequestEvent(getAccessLog())) {
				event.command = "login";
				event.identity = remoteIdentity;

				event.arguments = new ArrayList<>();

				for (final X509Certificate cert : partnerCerts)
					event.arguments.add(cert.getSubjectX500Principal().getName() + " (expires " + cert.getNotAfter() + ")");
			}
			catch (@SuppressWarnings("unused") final IOException ioe) {
				// ignore any exception in writing out the event
			}

			double lLasted = 0;

			int requestCount = 0;

			activeSessions.incrementAndGet();

			try {
				while (true) {
					lastActivity = System.currentTimeMillis();

					final Object o = ois.readObject();

					lastActivity = System.currentTimeMillis();

					if (o != null)
						if (o instanceof Request) {
							Request r = (Request) o;

							r.setPartnerIdentity(remoteIdentity);

							r.setPartnerCertificate(partnerCerts);

							final double requestProcessingDuration;

							try (RequestEvent event = new RequestEvent(getAccessLog())) {
								event.clientAddress = remoteIdentity.getRemoteEndpoint();
								event.clientPort = remoteIdentity.getRemotePort();
								event.command = r.getClass().getSimpleName();
								event.clientID = r.getVMUUID();
								event.requestId = r.getRequestID();
								event.arguments = r.getArguments();

								try {
									r = Dispatcher.execute(r, forwardRequest);
									event.exitCode = 0;
								}
								catch (final Exception e) {
									logger.log(Level.WARNING, "Returning an exception to the client", e);

									r.setException(new ServerException(e.getMessage(), e));

									event.exception = e;
									event.exitCode = ErrNo.EBADE.getErrorCode();
									event.errorMessage = "Exception executing request";
								}

								event.identity = r.getEffectiveRequester();

								requestProcessingDuration = event.timing.getMillis();
							}

							lLasted += requestProcessingDuration;

							final double serializationTime;

							try (Timing timing = new Timing()) {
								// System.err.println("When returning the object, ex is "+r.getException());

								oos.writeObject(r);

								oos.flush();

								serializationTime = timing.getMillis();
							}

							lSerialization += serializationTime;

							if (logger.isLoggable(Level.FINE))
								logger.log(Level.FINE, "Got request from " + r.getRequesterIdentity() + " : " + r.getClass().getCanonicalName());

							if (monitor != null) {
								monitor.addMeasurement("request_processing", requestProcessingDuration);
								monitor.addMeasurement("serialization", serializationTime);
							}

							requestCount++;
						}
						else
							logger.log(Level.WARNING, "I don't know what to do with an object of type " + o.getClass().getCanonicalName());
				}
			}
			catch (

			@SuppressWarnings("unused") final EOFException e) {
				if (logger.isLoggable(Level.INFO))
					logger.log(Level.INFO,
							"Client " + getName() + " disconnected after sending " + requestCount + " requests that took in total " + Format.toInterval((long) lLasted) + " to process and "
									+ Format.toInterval((long) lSerialization) + " to serialize");
			}
			catch (final Throwable e) {
				logger.log(Level.WARNING, "Main thread for " + getName() + " threw an error after sending " + requestCount + " requests that took in total " + Format.toInterval((long) lLasted)
						+ " to process and " + Format.toInterval((long) lSerialization) + " to serialize", e);
			}
			finally {
				activeSessions.decrementAndGet();

				try {
					connection.setSoTimeout(15 * 1000);
				}
				catch (@SuppressWarnings("unused") final IOException ioe) {
					// ignore
				}

				if (ois != null)
					try {
						ois.close();
					}
					catch (@SuppressWarnings("unused") final IOException ioe) {
						// ignore
					}

				if (oos != null)
					try {
						oos.close();
					}
					catch (@SuppressWarnings("unused") final IOException ioe) {
						// ignore
					}

				try {
					connection.close();
				}
				catch (@SuppressWarnings("unused") final IOException ioe) {
					// ignore
				}
			}
		}
		finally {
			sessionMap.remove(this);
		}
	}

	private static File accessLogFile = null;

	private static OutputStream accessLogStream = null;

	static synchronized OutputStream getAccessLog() {
		if (accessLogFile != null && !accessLogFile.exists()) {
			if (accessLogStream != null) {
				try {
					accessLogStream.close();
				}
				catch (@SuppressWarnings("unused") final IOException ioe) {
					// ignore
				}

				accessLogStream = null;
			}

			accessLogFile = null;
		}

		if (accessLogStream == null) {
			final String accessLogFileName = ConfigUtils.getConfig().gets("alien.api.DispatchSSLServer.access_log");

			if (accessLogFileName.length() > 0) {
				try {
					accessLogFile = new File(accessLogFileName);

					accessLogStream = new FileOutputStream(accessLogFile, true);
				}
				catch (final IOException ioe) {
					logger.log(Level.WARNING, "Cannot write to access log " + accessLogFileName + ", will write to stderr instead", ioe);
					accessLogFile = null;
				}
			}
			else {
				System.err.println("Define alien.api.DispatchSSLServer.access_log to a writable location, otherwise logs will go to stderr");
			}

			if (accessLogStream == null)
				accessLogStream = System.err;

			try (RequestEvent event = new RequestEvent(accessLogStream)) {
				event.command = "boot";
				event.identity = AuthorizationFactory.getDefaultUser();
				event.clientAddress = actualServerAddress;
				event.clientPort = actualServerPort;
				event.clientID = Request.getVMID();

				event.arguments = new ArrayList<>();

				if (JAKeyStore.getKeyStore().getCertificateChain("User.cert") != null)
					for (final Certificate cert : JAKeyStore.getKeyStore().getCertificateChain("User.cert")) {
						final X509Certificate x509cert = (java.security.cert.X509Certificate) cert;
						event.arguments.add(x509cert.getSubjectX500Principal().getName() + " (expires " + x509cert.getNotAfter() + ")");
					}
				else {
					event.exitCode = ErrNo.ENOMSG.getErrorCode();
					event.errorMessage = "Local identity doesn't have a certificate chain associated";
				}
			}
			catch (@SuppressWarnings("unused") IOException | KeyStoreException e) {
				// ignore exception in logging the startup message
			}
		}

		return accessLogStream;
	}

	private static boolean isHostCertValid() {
		try {
			((java.security.cert.X509Certificate) JAKeyStore.getKeyStore().getCertificateChain("User.cert")[0]).checkValidity();
		}
		catch (@SuppressWarnings("unused") final CertificateException | KeyStoreException e) {
			return false;
		}

		return true;
	}

	/**
	 * @throws IOException
	 */
	@SuppressWarnings("resource")
	public static void runService() throws IOException {

		int port = defaultPort;

		String address = ConfigUtils.getConfig().gets(serviceName).trim();

		if (address.length() != 0) {
			final int idx = address.indexOf(':');

			if (idx >= 0)
				try {
					port = Integer.parseInt(address.substring(idx + 1).trim());
					address = address.substring(0, idx).trim();
				}
				catch (@SuppressWarnings("unused") final Exception e) {
					port = defaultPort;
				}
		}

		if ("*".equals(address))
			address = "";

		final IdleConnectionsKiller idleKiller = new IdleConnectionsKiller();
		idleKiller.start();

		SSLServerSocket server = null;

		try {
			Security.addProvider(new BouncyCastleProvider());

			final KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509", "SunJSSE");

			kmf.init(JAKeyStore.getKeyStore(), JAKeyStore.pass);

			if (!isHostCertValid()) {
				logger.log(Level.SEVERE, "Host certificate is not valid!");
				return;
			}

			logger.log(Level.INFO, "Running JCentral with host cert: " + ((java.security.cert.X509Certificate) JAKeyStore.getKeyStore().getCertificateChain("User.cert")[0]).getSubjectDN());

			java.lang.System.setProperty("jdk.tls.client.protocols", "TLSv1.2,TLSv1.3");

			final SSLContext sc = SSLContext.getInstance("TLS");

			final TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
			tmf.init(JAKeyStore.getKeyStore());

			// TODO: implement custom TrustManager[] checkClientTrusted() to be able to accept clients with proxy certs
			// Hint: https://stackoverflow.com/questions/6011348/how-do-i-accept-a-self-signed-certificate-with-a-java-using-sslsocket

			sc.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());

			final SSLServerSocketFactory ssf = sc.getServerSocketFactory();

			if (address.length() > 0)
				server = (SSLServerSocket) ssf.createServerSocket(port, 0, InetAddress.getByName(address));
			else
				server = (SSLServerSocket) ssf.createServerSocket(port);

			server.setNeedClientAuth(true);

			server.setUseClientMode(false);

			actualServerAddress = server.getInetAddress();
			actualServerPort = server.getLocalPort();

			System.out.println("JCentral listening on " + server.getLocalSocketAddress());

			logger.log(Level.INFO, "JCentral listening on  " + server.getLocalSocketAddress());

			while (true) {
				if (!isHostCertValid()) {
					logger.log(Level.SEVERE, "Host certificate is not valid any more, please renew it and restart the service");
					return;
				}

				final SSLSocket c;
				try {
					// this object is passed to another thread to deal with the
					// communication
					c = (SSLSocket) server.accept();
				}
				catch (final IOException ioe) {
					logger.log(Level.WARNING, "Exception accepting a client", ioe);

					if (monitor != null)
						monitor.incrementCounter("exception_handling_client");

					continue;
				}

				try {
					acceptorPool.submit(() -> handleOneSSLSocket(c, true));
				}
				catch (final Throwable t) {
					try {
						c.close();
					}
					catch (final Throwable t2) {
						logger.log(Level.SEVERE, "Cannot close the socket as cleanup", t2);
					}

					logger.log(Level.SEVERE, "Problem accepting a client", t);

					if (monitor != null)
						monitor.incrementCounter("exception_handling_client");
				}
			}
		}
		catch (

		final Throwable e) {
			logger.log(Level.SEVERE, "Could not initiate SSL Server Socket on " + address + ":" + port, e);
		}
	}

	private static void handleOneSSLSocket(final SSLSocket c, final boolean needClientAuth) {
		try {
			// clients have 10s to negotiate the SSL
			c.setSoTimeout(10 * 1000);
		}
		catch (final SocketException e1) {
			logger.log(Level.WARNING, "Cannot setSoTimeout for the initial communication", e1);
		}

		if (!c.getSession().isValid()) {
			logger.log(Level.WARNING, "Invalid SSL connection from " + c.getRemoteSocketAddress());

			if (monitor != null)
				monitor.incrementCounter("invalid_ssl_connection");
		}

		X509Certificate[] peerCertChain = null;

		if (needClientAuth) {
			if (logger.isLoggable(Level.FINE))
				logger.log(Level.FINE, "Printing client information:");

			Certificate[] peerCerts;
			try {
				peerCerts = c.getSession().getPeerCertificates();
			}
			catch (final SSLPeerUnverifiedException e) {
				logger.log(Level.WARNING, "Client certificate cannot be validated", e);

				if (monitor != null)
					monitor.incrementCounter("invalid_ssl_connection");

				return;
			}

			if (peerCerts != null) {
				peerCertChain = new X509Certificate[peerCerts.length];

				for (int i = 0; i < peerCerts.length; i++) {
					if (peerCerts[i] instanceof X509Certificate) {
						final X509Certificate xCert = (X509Certificate) peerCerts[i];
						if (logger.isLoggable(Level.FINE))
							logger.log(Level.FINE, getClientInfo(xCert));

						peerCertChain[i] = xCert;
					}
					else {
						logger.log(Level.WARNING, "Peer certificate is not an X509 instance but instead a " + peerCerts[i].getType());
					}
				}
			}
			else
				logger.log(Level.WARNING, "Failed to get peer certificates");
		}

		final DispatchSSLServer serv = new DispatchSSLServer(c);
		if (needClientAuth)
			serv.partnerCerts = peerCertChain;

		serv.start();

		sessionMap.put(serv, serv);

		if (monitor != null) {
			monitor.incrementCounter("accepted_connections");

			if (c.getInetAddress() instanceof Inet6Address)
				ipv6Connections.incrementHits();
			else
				ipv6Connections.incrementMisses();
		}
	}

	/**
	 * Total amount of time (in milliseconds) spent in writing objects to the socket.
	 */
	private static double lSerialization = 0;

	/**
	 * Print client info on SSL partner
	 */
	private static String getClientInfo(final X509Certificate cert) {
		return "Peer Certificate Information:\n" + "- Subject: " + cert.getSubjectDN().getName() + "- Issuer: \n" + cert.getIssuerDN().getName() + "- Version: \n" + cert.getVersion()
				+ "- Start Time: \n" + cert.getNotBefore().toString() + "\n" + "- End Time: " + cert.getNotAfter().toString() + "\n" + "- Signature Algorithm: " + cert.getSigAlgName() + "\n"
				+ "- Serial Number: " + cert.getSerialNumber();
	}

	private static final class IdleConnectionsKiller extends Thread {
		IdleConnectionsKiller() {
			setName("DispatchSSLServer.IdleConnectionsKiller");
			setDaemon(true);
		}

		@Override
		public void run() {
			while (true) {
				final long threshold = System.currentTimeMillis() - ConfigUtils.getConfig().geti("alien.api.DispatchSSLServer.idleTimeout_seconds", 900);

				for (final DispatchSSLServer srv : sessionMap.keySet()) {
					if (srv.lastActivity < threshold) {
						try {
							srv.connection.close();
						}
						catch (final Exception e) {
							logger.log(Level.WARNING, "Exception closing idle connection's socket", e);
						}

						srv.interrupt();

						monitor.incrementCounter("idle_connections_closed");
					}
				}

				try {
					sleep(1000 * 30);
				}
				catch (final InterruptedException e) {
					logger.log(Level.WARNING, "This thread should not have been interrupted", e);
				}
			}
		}
	}

	/**
	 * Print some info on the SSL Socket
	 */
	// private static String printServerSocketInfo(SSLServerSocket s) {
	// return "Server socket class: " + s.getClass()
	// + "\n Socket address = " + s.getInetAddress().toString()
	// + "\n Socket port = " + s.getLocalPort()
	// + "\n Need client authentication = " + s.getNeedClientAuth()
	// + "\n Want client authentication = " + s.getWantClientAuth()
	// + "\n Use client mode = " + s.getUseClientMode();
	// }

}