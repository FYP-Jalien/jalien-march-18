package alien.api;

import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.security.KeyStoreException;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.TrustManagerFactory;

import org.baswell.niossl.NioSslLogger;
import org.baswell.niossl.SSLServerSocketChannel;
import org.baswell.niossl.SSLSocketChannel;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

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
import utils.CachedThreadPool;

/**
 * @author costing
 *
 */
public class DispatchSSLServerNIO implements Runnable {
	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(DispatchSSLServerNIO.class.getCanonicalName());

	/**
	 * Service monitoring
	 */
	static final Monitor monitor = MonitorFactory.getMonitor(DispatchSSLServerNIO.class.getCanonicalName());

	/**
	 * Connections that need looking at
	 */
	private static BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>();

	private static final AtomicInteger threadNo = new AtomicInteger();

	private static final AtomicInteger sslThreadNo = new AtomicInteger();

	/**
	 * Thread pool handling messages
	 */
	private static final int executorMaxSize = ConfigUtils.getConfig().geti(DispatchSSLServerNIO.class.getCanonicalName() + ".executorMaxSize", 100);

	private static final int sslExecutorMaxSize = ConfigUtils.getConfig().geti(DispatchSSLServerNIO.class.getCanonicalName() + ".sslExecutorMaxSize", 16);

	private static ThreadPoolExecutor executor = new CachedThreadPool(executorMaxSize, 1, TimeUnit.MINUTES, (r) -> new Thread(r, "DispatchSSLServerNIO - " + threadNo.incrementAndGet()));

	private static ThreadPoolExecutor sslExecutor = new CachedThreadPool(sslExecutorMaxSize, 1, TimeUnit.MINUTES, (r) -> new Thread(r, "SSLNegociator- " + sslThreadNo.incrementAndGet()));

	private static final int defaultPort = 8098;
	private static String serviceName = "apiService";

	private static boolean forwardRequest = false;

	private static CacheMonitor ipv6Connections = null;

	private static Selector serverSelector;

	private static SSLServerSocketChannel serverSocketChannel;

	private static final SSLContext sslContext = getSSLContext();

	private static ConcurrentHashMap<SelectionKey, DispatchSSLServerNIO> sessionMap = new ConcurrentHashMap<>();

	static {
		if (monitor != null) {
			monitor.addMonitoring("activeSessions", (names, values) -> {
				names.add("activeSessions");
				values.add(Double.valueOf(sessionMap.size()));

				names.add("executorPoolSize");
				values.add(Double.valueOf(executor.getPoolSize()));

				names.add("executorActiveCount");
				values.add(Double.valueOf(executor.getActiveCount()));

				names.add("sslExecutorPoolSize");
				values.add(Double.valueOf(sslExecutor.getPoolSize()));

				names.add("sslExecutorActiveCount");
				values.add(Double.valueOf(sslExecutor.getActiveCount()));

				names.add("eQueueSize");
				values.add(Double.valueOf(taskQueue.size()));
			});

			ipv6Connections = monitor.getCacheMonitor("ipv6_connections");
		}
	}

	// --------------------------- Per connection information

	/**
	 * The entire connection
	 */
	private final SSLSocketChannel channel;

	/**
	 * Getting requests by this stream
	 */
	private ObjectInputStream ois;

	/**
	 * Writing replies here
	 */
	private ObjectOutputStream oos;

	private OutputStream os;

	private X509Certificate partnerCerts[] = null;

	private SelectionKey key;

	private final AtomicBoolean isActive = new AtomicBoolean(false);

	private final AtomicBoolean isKilled = new AtomicBoolean(false);

	private AliEnPrincipal remoteIdentity = null;

	private PipedOutputStream pos = null;
	private PipedInputStream pis = null;

	private long lastActive = System.currentTimeMillis();

	private class ByteBufferOutputStream extends OutputStream {
		private ByteBuffer buffer = ByteBuffer.allocate(1024);

		@Override
		public void write(int arg0) throws IOException {
			if (buffer.remaining() <= 0)
				flush();

			buffer.put((byte) (arg0 & 0xFF));
		}

		@Override
		public void flush() throws IOException {
			buffer.flip();

			channel.write(buffer);

			// channel.getSSLEngineBuffer().flushNetworkOutbound();

			buffer.clear();
		}
	}

	/**
	 * @param channel
	 * @throws Exception
	 */
	@SuppressWarnings("resource")
	public DispatchSSLServerNIO(final SSLSocketChannel channel) throws Exception {
		this.channel = channel;

		channel.configureBlocking(false);

		key = channel.getWrappedSocketChannel().register(serverSelector, SelectionKey.OP_READ);

		serverSelector.wakeup();

		monitor.incrementCounter("accepted_connections");
	}

	private boolean lateInitialization() {
		if (remoteIdentity != null)
			return true;

		final HandshakeStatus hs = channel.getSSLEngineBuffer().getEngine().getHandshakeStatus();

		if (hs != HandshakeStatus.FINISHED && hs != HandshakeStatus.NOT_HANDSHAKING) {
			// Handshake not yet completed
			return false;
		}

		try {
			final InetSocketAddress addr = (InetSocketAddress) channel.getRemoteAddress();

			// TODO get the partner certificates
			partnerCerts = (X509Certificate[]) channel.getSSLEngineBuffer().getEngine().getSession().getPeerCertificates();

			if (logger.isLoggable(Level.FINE))
				logger.log(Level.FINE, getClientInfo(partnerCerts[0]));

			remoteIdentity = UserFactory.getByCertificate(partnerCerts);

			if (remoteIdentity == null)
				throw new IOException("Could not get the identity of this certificate chain: " + Arrays.toString(partnerCerts));

			remoteIdentity.setRemoteEndpoint(addr);

			if (remoteIdentity.getRemoteEndpoint() instanceof Inet6Address)
				ipv6Connections.incrementHits();
			else
				ipv6Connections.incrementMisses();

			try (RequestEvent event = new RequestEvent(DispatchSSLServer.getAccessLog())) {
				event.command = "login";
				event.identity = remoteIdentity;
				event.clientID = Request.getVMID();

				if (logger.isLoggable(Level.FINE))
					logger.log(Level.FINE, "Identity address and port: " + event.identity.getRemoteEndpoint() + ":" + event.identity.getRemotePort());

				event.arguments = new ArrayList<>();

				for (final X509Certificate cert : partnerCerts)
					event.arguments.add(cert.getSubjectX500Principal().getName() + " (expires " + cert.getNotAfter() + ")");
			}
			catch (@SuppressWarnings("unused") final IOException ioe) {
				// ignore any exception in writing out the event
			}

			os = new ByteBufferOutputStream();

			oos = new ObjectOutputStream(os);

			oos.flush();

			pos = new PipedOutputStream();
			pis = new PipedInputStream(pos, 1024 * 1024);
		}
		catch (final IOException ioe) {
			logger.log(Level.WARNING, "Cannot initialize", ioe);
		}

		return remoteIdentity != null;
	}

	private void notifyData() {
		// System.err.println("Handling data notification");

		if (isKilled.get())
			return;

		// boolean anyDataToProcess = false;

		// int size = channel.getSSLEngineBuffer().getEngine().getSession().getApplicationBufferSize();

		int size = 32 * 1024;

		// System.err.println("Allocating a buffer of " + size);

		ByteBuffer smallBuffer = ByteBuffer.allocate(size);

		while (!isKilled.get()) {
			smallBuffer.clear();
			final int num;

			try {
				num = channel.read(smallBuffer);
			}
			catch (final IOException cce) {
				logger.log(Level.WARNING, "Channel was closed, cleaning up", cce);
				cleanup();
				return;
			}

			// check if we can create the streams and have the identity now
			if (num >= 0)
				lateInitialization();

			if (num == -1) {
				cleanup();
				// break;
			}
			else if (num == 0) {
				break;
			}
			else {
				newDataBlock(Arrays.copyOfRange(smallBuffer.array(), 0, smallBuffer.position()));
			}
		}
	}

	private void newDataBlock(final byte[] buffer) {
		try {
			pos.write(buffer);
			// System.err.println("Buffer size is " + pis.available());

			if (isActive.compareAndSet(false, true)) {
				// System.err.println("Starting background reader");
				executor.submit(this);
			}
			else
				pos.flush();
		}
		catch (final IOException ioe) {
			// buffer overflow, the sent object was too large
			logger.log(Level.WARNING, "Exception handling a new data block, closing connection", ioe);
			cleanup();
		}
	}

	/**
	 *
	 */
	private void cleanup() {
		isKilled.set(true);

		DispatchSSLServerNIO removed = sessionMap.remove(key);

		if (removed == null) {
			logger.log(Level.FINE, "no object was removed from map for " + key);
		}

		key.interestOps(0);
		key.cancel();

		close(channel);
		close(pos);
		close(pis);
		close(ois);
	}

	private double executeOneRequest(final Object o) throws Exception {
		double ret = -1;

		if (o instanceof Request) {
			Request r = (Request) o;

			r.setPartnerIdentity(remoteIdentity);

			r.setPartnerCertificate(partnerCerts);

			final double requestProcessingDuration;

			try (RequestEvent event = new RequestEvent(DispatchSSLServer.getAccessLog())) {
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

			ret = requestProcessingDuration;

			final double serializationTime;

			try (Timing timing = new Timing()) {
				// System.err.println("When returning the object, ex is "+r.getException());

				oos.writeUnshared(r);

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
		}
		else
			logger.log(Level.WARNING, "I don't know what to do with an object of type " + o.getClass().getCanonicalName());

		return ret;
	}

	@Override
	public void run() {
		try {
			if (ois == null)
				try {
					ois = new ObjectInputStream(pis);
				}
				catch (IOException e) {
					System.err.println("Cannot create the ois: " + e.getMessage());
					return;
				}

			try {
				while (pis.available() > 0) {
					final Object o = ois.readObject();

					if (o != null) {
						try {
							final double duration = executeOneRequest(o);

							if (duration >= 0) {
								// TODO: handle a successfully executed object
							}
						}
						catch (final Exception e) {
							logger.log(Level.WARNING, "Exception running a command", e);
							cleanup();
						}
					}
				}
			}
			catch (ClassNotFoundException e) {
				logger.log(Level.SEVERE, "Cannot deserialize a request", e);
			}
			catch (IOException e) {
				logger.log(Level.SEVERE, "IOException running a request", e);
			}
		}
		finally {
			isActive.set(false);

			lastActive = System.currentTimeMillis();
		}
	}

	private static void close(final Closeable c) {
		try {
			if (c != null)
				c.close();
		}
		catch (@SuppressWarnings("unused") final IOException ioe) {
			// ignore
		}
	}

	void flush() throws IOException {
		oos.flush();
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

	private static void handleNewConnection(final SSLSocketChannel sc) {
		try {
			final DispatchSSLServerNIO connectionWrapper = new DispatchSSLServerNIO(sc);

			sessionMap.put(connectionWrapper.key, connectionWrapper);

			// initialize the object stream on the other side
			// connectionWrapper.flush();
		}
		catch (final Throwable e) {
			logger.log(Level.SEVERE, "Error handling new socket", e);
		}
	}

	private static Thread acceptorThread = new Thread() {
		@Override
		public void run() {
			while (true) {
				try {
					@SuppressWarnings("resource")
					final SSLSocketChannel sc = serverSocketChannel.acceptOverSSL();

					executor.submit(() -> handleNewConnection(sc));
				}
				catch (final IOException ioe) {
					System.err.println("Exception handling a new socket: " + ioe.getMessage());
				}
			}
		}
	};

	private static Thread selectorThread = new Thread() {
		@Override
		public void run() {
			while (true) {
				try {
					serverSelector.select();

					var selectedKeys = serverSelector.selectedKeys();

					var it = selectedKeys.iterator();

					while (it.hasNext()) {
						final SelectionKey key = it.next();

						if (key == null || !key.isValid()) {
							logger.log(Level.INFO, "Detected closed key, disposing of it");

							if (key != null) {
								final DispatchSSLServerNIO obj = sessionMap.remove(key);

								if (obj != null) {
									try {
										obj.cleanup();
									}
									catch (final Throwable t) {
										logger.log(Level.WARNING, "Cleaning up an invalid key encountered a problem", t);
									}
								}
							}

							it.remove();
							continue;
						}

						if (key.isReadable()) {
							final DispatchSSLServerNIO obj = sessionMap.get(key);

							if (obj != null) {
								// System.err.println("Key found in session map: " + key);
								// System.err.println("Map content: " + sessionMap);
								// disable future events until the dispatcher has digested the data
								// key.interestOps(0);
								// System.err.println("Notifying of new data");
								try {
									obj.notifyData();
								}
								catch (final Throwable t1) {
									try {
										logger.log(Level.WARNING, "Throwable notifying data", t1);
										obj.cleanup();
									}
									catch (final Throwable t) {
										logger.log(Level.WARNING, "Cleaning up after an error encountered a problem", t);
									}
								}
							}
							else {
								System.err.println("Didn't find a corresponding session");
							}
						}

						it.remove();
					}
				}
				catch (final IOException ioe) {
					System.err.println("Server selector threw an exception : " + ioe.getMessage());
					ioe.printStackTrace();
				}
			}
		}
	};

	private static SSLContext getSSLContext() {
		try {
			java.lang.System.setProperty("jdk.tls.client.protocols", "TLSv1.2,TLSv1.3");

			Security.addProvider(new BouncyCastleProvider());

			final KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509", "SunJSSE");

			kmf.init(JAKeyStore.getKeyStore(), JAKeyStore.pass);

			final SSLContext sc = SSLContext.getInstance("TLS");

			final TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
			tmf.init(JAKeyStore.getKeyStore());

			sc.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());

			return sc;
		}
		catch (final Exception e) {
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * @throws IOException
	 */
	@SuppressWarnings("resource")
	public static void runService() throws IOException {
		if (!isHostCertValid()) {
			logger.log(Level.SEVERE, "Host certificate is not valid!");
			return;
		}

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

		if (address.equals("*"))
			address = "";

		serverSelector = Selector.open();

		ServerSocketChannel plainSocketChannel = ServerSocketChannel.open();
		plainSocketChannel.socket().bind(new InetSocketAddress(port));

		final NioSslLogger nioLogger = null; // disabled logging

		serverSocketChannel = new SSLServerSocketChannel(plainSocketChannel, sslContext, sslExecutor, nioLogger);
		serverSocketChannel.needClientAuthentication = true;

		try {
			logger.log(Level.INFO, "Running JCentral with host cert: " + ((java.security.cert.X509Certificate) JAKeyStore.getKeyStore().getCertificateChain("User.cert")[0]).getSubjectDN());

			System.out.println("JCentral listening on " + serverSocketChannel.getLocalAddress());

			logger.log(Level.INFO, "JCentral listening on  " + serverSocketChannel.getLocalAddress());

			acceptorThread.start();

			selectorThread.start();

			while (true) {
				if (!isHostCertValid()) {
					logger.log(Level.SEVERE, "Host certificate is not valid any more, please renew it and restart the service");
					return;
				}

				// serverSelector.wakeup();

				Thread.sleep(60000);

				final long maxIdleTime = ConfigUtils.getConfig().geti("alien.api.DispatchSSLServer.idleTimeout_seconds", 900) * 1000L;

				final long referenceTime = System.currentTimeMillis() - maxIdleTime;

				for (DispatchSSLServerNIO instance : sessionMap.values()) {
					if (instance.lastActive < referenceTime && !instance.isActive.get()) {
						try {
							if (instance.remoteIdentity != null)
								logger.log(Level.WARNING, "Closing idle connection: " + instance.remoteIdentity.getName() + "@" + instance.remoteIdentity.getRemoteEndpoint());
							else
								logger.log(Level.WARNING, "Closing idle connection for a `null` identity");

							instance.cleanup();
						}
						catch (Throwable t) {
							logger.log(Level.WARNING, "Exception closing an idle connection", t);
						}
					}
				}
			}

		}
		catch (final Throwable e) {
			logger.log(Level.SEVERE, "Could not initiate SSL Server Socket on " + address + ":" + port, e);
		}
	}

	/**
	 * Total amount of time (in milliseconds) spent in writing objects to the socket.
	 */
	private static double lSerialization = 0;

	/**
	 * @return total time in milliseconds spent in serializing objects
	 */
	public static double getSerializationTime() {
		return lSerialization;
	}

	/**
	 * Print client info on SSL partner
	 */
	private static String getClientInfo(final X509Certificate cert) {
		return "Peer Certificate Information:\n" + "- Subject: " + cert.getSubjectDN().getName() + "- Issuer: \n" + cert.getIssuerDN().getName() + "- Version: \n" + cert.getVersion()
				+ "- Start Time: \n" + cert.getNotBefore().toString() + "\n" + "- End Time: " + cert.getNotAfter().toString() + "\n" + "- Signature Algorithm: " + cert.getSigAlgName() + "\n"
				+ "- Serial Number: " + cert.getSerialNumber();
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