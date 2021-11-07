package alien.api;

import java.io.File;
import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.catalina.Context;
import org.apache.catalina.LifecycleState;
import org.apache.catalina.Wrapper;
import org.apache.catalina.authenticator.SSLAuthenticator;
import org.apache.catalina.connector.Connector;
import org.apache.catalina.servlets.DefaultServlet;
import org.apache.catalina.startup.Tomcat;
import org.apache.catalina.util.ServerInfo;
import org.apache.coyote.ProtocolHandler;
import org.apache.coyote.http11.Http11NioProtocol;
import org.apache.tomcat.util.descriptor.web.LoginConfig;
import org.apache.tomcat.util.descriptor.web.SecurityCollection;
import org.apache.tomcat.util.descriptor.web.SecurityConstraint;
import org.apache.tomcat.util.net.SSLHostConfig;
import org.apache.tomcat.util.net.SSLHostConfigCertificate;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.user.JAKeyStore;
import alien.user.LdapCertificateRealm;
import alien.user.UserFactory;
import alien.websockets.WebsocketListener;
import alien.websockets.WebsocketServlet;

/**
 * @author vyurchen
 *
 */
public class TomcatServer {

	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(JBoxServer.class.getCanonicalName());

	/**
	 * Activity monitoring
	 */
	private static final Monitor monitor = MonitorFactory.getMonitor(TomcatServer.class.getCanonicalName());

	/**
	 * Web server instance
	 */
	final Tomcat tomcat;

	private final int websocketPort;

	/**
	 * The expiration date of the certificate, used as server cert in tomcat
	 */
	private static long expirationTime;

	/**
	 * Start the Tomcat server on a given port
	 *
	 * @param tomcatPort
	 */
	private TomcatServer(final int tomcatPort, final String bindAddress) throws Exception {
		this.websocketPort = tomcatPort;

		tomcat = new Tomcat();
		tomcat.setBaseDir(System.getProperty("java.io.tmpdir"));
		tomcat.setPort(tomcatPort);
		tomcat.getService().removeConnector(tomcat.getConnector()); // remove default connector
		tomcat.getService().addConnector(createSslConnector(tomcatPort, bindAddress));

		// Add an empty Tomcat context
		final Context ctx = tomcat.addContext("", null);

		// Configure websocket context listener
		ctx.addApplicationListener(WebsocketListener.class.getName());
		Tomcat.addServlet(ctx, "default", new DefaultServlet());
		ctx.addServletMappingDecoded("/", "default");

		final Wrapper wrapper = Tomcat.addServlet(ctx, "WebsocketServlet", WebsocketServlet.class.getName());
		wrapper.addMapping("/websocket/*");

		// Set security constraints in order to use AlienUserPrincipal later
		final SecurityCollection securityCollection = new SecurityCollection();
		securityCollection.addPattern("/*");
		final SecurityConstraint securityConstraint = new SecurityConstraint();
		securityConstraint.addCollection(securityCollection);
		securityConstraint.setAuthConstraint(true);
		securityConstraint.setUserConstraint("CONFIDENTIAL");
		securityConstraint.addAuthRole("users");
		ctx.addConstraint(securityConstraint);

		final LoginConfig loginConfig = new LoginConfig();
		loginConfig.setAuthMethod("CLIENT-CERT");
		loginConfig.setRealmName(LdapCertificateRealm.class.getCanonicalName());
		ctx.setLoginConfig(loginConfig);
		final LdapCertificateRealm ldapRealm = new LdapCertificateRealm();
		ctx.setRealm(ldapRealm);
		ctx.getPipeline().addValve(new SSLAuthenticator());

		tomcat.start();
		if (tomcat.getService().findConnectors()[0].getState() == LifecycleState.FAILED)
			throw new BindException();

		final Executor executor = tomcat.getConnector().getProtocolHandler().getExecutor();

		if (executor instanceof ThreadPoolExecutor) {
			final ThreadPoolExecutor tpe = (ThreadPoolExecutor) executor;

			monitor.addMonitoring("server_status", (names, values) -> {
				names.add("active_threads");
				values.add(Double.valueOf(tpe.getActiveCount()));

				names.add("max_threads");
				values.add(Double.valueOf(tpe.getMaximumPoolSize()));

				names.add("tomcat_version");
				values.add(ServerInfo.getServerNumber());
			});
		}
		else if (executor instanceof org.apache.tomcat.util.threads.ThreadPoolExecutor) {
			final org.apache.tomcat.util.threads.ThreadPoolExecutor tpe = (org.apache.tomcat.util.threads.ThreadPoolExecutor) executor;

			monitor.addMonitoring("server_status", (names, values) -> {
				names.add("active_threads");
				values.add(Double.valueOf(tpe.getActiveCount()));

				names.add("max_threads");
				values.add(Double.valueOf(tpe.getMaximumPoolSize()));

				names.add("tomcat_version");
				values.add(ServerInfo.getServerNumber());
			});
		}
		else
			logger.log(Level.SEVERE, "Cannot monitor Tomcat executor of type " + executor.getClass().getCanonicalName());

		// Let Tomcat run in another thread so it will keep on waiting forever
		new Thread() {
			@Override
			public void run() {
				tomcat.getServer().await();
			}
		}.start();
	}

	/**
	 * Create SSL connector for the Tomcat server
	 *
	 * @param tomcatPort
	 */
	private static Connector createSslConnector(final int tomcatPort, final String bindAddress) {
		final String keystorePass = new String(JAKeyStore.pass);

		final String dirName = System.getProperty("java.io.tmpdir") + File.separator;
		final String keystoreName = dirName + "keystore.jks_" + UserFactory.getUserID();
		final String truststoreName = dirName + "truststore.jks_" + UserFactory.getUserID();

		if (ConfigUtils.isCentralService()) {
			JAKeyStore.saveKeyStore(JAKeyStore.getKeyStore(), keystoreName, JAKeyStore.pass);
			expirationTime = JAKeyStore.getExpirationTime(JAKeyStore.getKeyStore());
		}
		else {
			JAKeyStore.saveKeyStore(JAKeyStore.tokenCert, keystoreName, JAKeyStore.pass);
			expirationTime = JAKeyStore.getExpirationTime(JAKeyStore.tokenCert);
		}
		JAKeyStore.saveKeyStore(JAKeyStore.trustStore, truststoreName, JAKeyStore.pass);

		final Connector connector = new Connector("org.apache.coyote.http11.Http11Nio2Protocol");

		connector.setProperty("address", ConfigUtils.getConfig().gets("alien.api.TomcatServer.bindAddress", bindAddress));

		connector.setPort(tomcatPort);
		connector.setSecure(true);
		connector.setScheme("https");
		connector.setProperty("SSLEnabled", "true");
		connector.setProperty("maxThreads", "200");
		connector.setProperty("maxConnections", "50000");
		connector.setProperty("connectionTimeout", "20000");
		connector.setProperty("compression", "on");
		connector.setProperty("compressionMinSize", "1");
		connector.setProperty("useSendFile", "false");
		connector.setProperty("defaultSSLHostConfigName", "localhost");

		final SSLHostConfig hostconfig = new SSLHostConfig();
		hostconfig.setHostName("localhost");
		hostconfig.setCertificateVerification("true");
		hostconfig.setTruststoreFile(truststoreName);
		hostconfig.setTruststorePassword(keystorePass);
		hostconfig.setTruststoreType("JKS");
		hostconfig.setSslProtocol("TLS");
		hostconfig.setProtocols("TLSv1.2");

		final SSLHostConfigCertificate cert = hostconfig.getCertificates(true).iterator().next();
		cert.setCertificateKeyAlias("User.cert");
		cert.setCertificateKeystoreFile(keystoreName);
		cert.setCertificateKeystorePassword(keystorePass);
		cert.setCertificateKeystoreType("JKS");

		connector.addSslHostConfig(hostconfig);
		return connector;
	}

	/**
	 * Reload the key and trust stores
	 */
	private static void reload(final Connector connector) {
		final ProtocolHandler protocolHandler = connector.getProtocolHandler();
		if (protocolHandler instanceof Http11NioProtocol) {
			final Http11NioProtocol http11NioProtocol = (Http11NioProtocol) protocolHandler;
			http11NioProtocol.reloadSslHostConfigs();
		}
	}

	/**
	 * Singleton
	 */
	static TomcatServer tomcatServer = null;

	/**
	 * Start Tomcat Server
	 */
	public static synchronized void startTomcatServer() {

		if (tomcatServer != null)
			return;

		logger.log(Level.INFO, "Tomcat starting ...");

		if (ConfigUtils.isCentralService()) {
			// Try to launch Tomcat on default port
			final int port = ConfigUtils.getConfig().geti("alien.api.TomcatServer.csPort", 8097);

			try (ServerSocket ssocket = new ServerSocket(port)) // Fast check if port is available
			{
				ssocket.close();
				// Actually start Tomcat
				tomcatServer = new TomcatServer(port, "*");

				logger.log(Level.INFO, "Tomcat listening on " + getListeningAddressAndPort());
				System.out.println("Tomcat is listening on " + getListeningAddressAndPort());
			}
			catch (final Exception ioe) {
				// Central services listen on a fixed server port, they should not bind on a different one as a fallback
				logger.log(Level.SEVERE, "Tomcat: Could not listen on CS port " + port, ioe);
			}
		}
		else {
			// Set dynamic port range for Tomcat server

			final boolean portAny = ConfigUtils.getConfig().getb("port.range.any", true);

			final int randomStartingPort = ThreadLocalRandom.current().nextInt(1000);

			final int portMin = ConfigUtils.getConfig().geti("port.range.start", 13150 + randomStartingPort);
			final int portMax = ConfigUtils.getConfig().geti("port.range.end", portMin + (portAny ? 1 : 200));

			// Try another ports in range
			for (int port = portMin; port < portMax; port++)
				try (ServerSocket ssocket = new ServerSocket(portAny ? 0 : port, 1, InetAddress.getByName("localhost"))) // Fast check if port is available
				{
					final int portToBindTo = ssocket.getLocalPort();

					ssocket.close();
					// Actually start Tomcat
					tomcatServer = new TomcatServer(portToBindTo, "localhost");

					logger.log(Level.INFO, "Tomcat listening on port " + getListeningAddressAndPort());
					System.out.println("Tomcat is listening on " + getListeningAddressAndPort());
					break;
				}
				catch (final Exception ioe) {
					// Try next one
					logger.log(Level.FINE, "Tomcat: Could not listen on port " + port, ioe);
				}
		}
	}

	/**
	 *
	 * Get the port used by tomcatServer
	 *
	 * @return the TCP port this server is listening on. Can be negative to signal that the server is actually not listening on any port (yet?)
	 *
	 */
	public static int getPort() {
		return tomcatServer != null ? tomcatServer.websocketPort : -1;
	}

	/**
	 * @return the address:port where Tomcat is listening, or <code>null</code> if the server is not started
	 */
	public static String getListeningAddressAndPort() {
		if (tomcatServer != null) {
			final Object addressAttribute = tomcatServer.tomcat.getConnector().getProperty("address");

			String address = addressAttribute != null ? addressAttribute.toString() : "*";

			address += ":" + tomcatServer.tomcat.getConnector().getPort();

			return address;
		}

		return null;
	}

	/**
	 * Check tomcat's server certificate every 12 hour. Load fresh token if current one is going to expire
	 */
	public static void startConnectorReloader() {
		new Thread() {
			@Override
			public void run() {
				final String dirName = System.getProperty("java.io.tmpdir") + File.separator;
				final String keystoreName = dirName + "keystore.jks_" + UserFactory.getUserID();
				final String truststoreName = dirName + "truststore.jks_" + UserFactory.getUserID();

				try {
					while (true) {
						sleep(12 * 60 * 60 * 1000);
						if (JAKeyStore.expireSoon(expirationTime)) {
							if (ConfigUtils.isCentralService()) {
								JAKeyStore.saveKeyStore(JAKeyStore.getKeyStore(), keystoreName, JAKeyStore.pass);
								expirationTime = JAKeyStore.getExpirationTime(JAKeyStore.getKeyStore());
							}
							else {
								JAKeyStore.saveKeyStore(JAKeyStore.tokenCert, keystoreName, JAKeyStore.pass);
								expirationTime = JAKeyStore.getExpirationTime(JAKeyStore.tokenCert);
							}
							JAKeyStore.saveKeyStore(JAKeyStore.trustStore, truststoreName, JAKeyStore.pass);

							reload(tomcatServer.tomcat.getConnector());
						}
					}
				}
				catch (final InterruptedException e) {
					e.printStackTrace();
				}
			}
		}.start();
	}
}
