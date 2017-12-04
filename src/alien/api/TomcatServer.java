package alien.api;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.security.KeyStoreException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.catalina.Context;
import org.apache.catalina.LifecycleState;
import org.apache.catalina.Service;
import org.apache.catalina.connector.Connector;
import org.apache.catalina.startup.Tomcat;
import org.apache.tomcat.util.descriptor.web.LoginConfig;
import org.apache.tomcat.util.descriptor.web.SecurityCollection;
import org.apache.tomcat.util.descriptor.web.SecurityConstraint;
import org.apache.tomcat.util.scan.StandardJarScanner;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import alien.catalogue.access.AuthorizationFactory;
import alien.config.ConfigUtils;
import alien.shell.commands.JAliEnCOMMander;
import alien.shell.commands.JSONPrintWriter;
import alien.shell.commands.UIPrintWriter;
import alien.user.AliEnPrincipal;
import alien.user.JAKeyStore;
import alien.user.LdapCertificateRealm;
import alien.user.UserFactory;
import alien.user.UsersHelper;
import lazyj.commands.CommandOutput;
import lazyj.commands.SystemCommand;

/**
 * @author yuw
 *
 */
public class TomcatServer {

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(JBoxServer.class.getCanonicalName());

	Tomcat tomcat;

	private final int websocketPort;

	/**
	 * Start the Tomcat server on a given port
	 *
	 * @param tomcatPort
	 */
	private TomcatServer(int tomcatPort, int iDebug) throws Exception {
		this.websocketPort = tomcatPort;

		tomcat = new Tomcat();
		Service service = tomcat.getService();
		tomcat.getService().removeConnector(tomcat.getConnector()); // remove default connector
		service.addConnector(createSslConnector(tomcatPort));
		tomcat.getEngine().setRealm(new LdapCertificateRealm());

		// Configure websocket webapplication
		String webappDirLocation = "src/alien/websockets";
		Context ctx = tomcat.addWebapp("", new File(webappDirLocation).getAbsolutePath());

		// Set security constraints in order to use AlienUserPrincipal later
		SecurityCollection securityCollection = new SecurityCollection();
		securityCollection.addPattern("/*");
		SecurityConstraint securityConstraint = new SecurityConstraint();
		securityConstraint.addCollection(securityCollection);
		securityConstraint.setUserConstraint("CONFIDENTIAL");

		LoginConfig loginConfig = new LoginConfig();
		loginConfig.setAuthMethod("CLIENT-CERT");
		loginConfig.setRealmName("alien.user.LdapCertificateRealm");
		ctx.setLoginConfig(loginConfig);

		securityConstraint.addAuthRole("users");
		ctx.addSecurityRole("users");
		ctx.addConstraint(securityConstraint);

		// Tell Jar Scanner not to look inside jar manifests
		// otherwise it will produce useless warnings
		StandardJarScanner jarScanner = (StandardJarScanner) ctx.getJarScanner();
		jarScanner.setScanManifest(false);

		tomcat.start();
		if (tomcat.getService().findConnectors()[0].getState() == LifecycleState.FAILED)
			throw new BindException();

		final AliEnPrincipal alUser = AuthorizationFactory.getDefaultUser();

		if (alUser == null || alUser.getName() == null)
			throw new Exception("Could not get your username. FATAL!");

		final String sHomeUser = UsersHelper.getHomeDir(alUser.getName());
		if (!writeTokenFile(tomcat.getHost().getName(), websocketPort, alUser.getName(), sHomeUser, iDebug)) {
			tomcat.stop();
			throw new Exception("Could not write the token file! No application can connect to JBox");
		}

		if (!writeEnvFile(tomcat.getHost().getName(), websocketPort, alUser.getName())) {
			tomcat.stop();
			throw new Exception("Could not write the env file! JSh/JRoot will not be able to connect to JBox");
		}

		// Let Tomcat run in another thread so it will keep on waiting forever
		new Thread() {
			@Override
			public void run() {
				tomcat.getServer().await();
			}
		}.start();
	}

	/**
	 * Create connector for the Tomcat server
	 *
	 * @param tomcatPort
	 * @throws Exception
	 */
	private static Connector createSslConnector(int tomcatPort) throws Exception {
		String keystorePass = new String(JAKeyStore.pass);
		if (ConfigUtils.isCentralService())
			JAKeyStore.saveKeyStore(JAKeyStore.getKeyStore(), "keystore.jks", JAKeyStore.pass);
		else
			JAKeyStore.saveKeyStore(JAKeyStore.tokenCert, "keystore.jks", JAKeyStore.pass);

		Connector connector = new Connector("org.apache.coyote.http11.Http11NioProtocol");

		connector.setPort(tomcatPort);
		connector.setSecure(true);
		connector.setScheme("https");
		connector.setAttribute("keyAlias", "User.cert");
		connector.setAttribute("keystorePass", keystorePass);
		connector.setAttribute("keystoreType", "JKS");
		connector.setAttribute("keystoreFile", System.getProperty("user.dir") + System.getProperty("file.separator") + "keystore.jks");
		connector.setAttribute("truststorePass", "castore");
		connector.setAttribute("truststoreType", "JKS");
		connector.setAttribute("truststoreFile", System.getProperty("user.dir") + System.getProperty("file.separator") + "trusted_authorities.jks");
		connector.setAttribute("clientAuth", "true");
		connector.setAttribute("sslProtocol", "TLS");
		connector.setAttribute("SSLEnabled", "true");
		connector.setAttribute("maxThreads", "200");
		connector.setAttribute("connectionTimeout", "20000");
		return connector;
	}

	/**
	 * Write down connection-related information to file
	 * the filename = <i>java.io.tmpdir</i>/jclient_token_$uid
	 *
	 * @param sHost
	 *            hostname to connect to, by default localhost
	 * @param iWSPort
	 *            websocket port number for listening
	 * @param sPassword
	 *            the password used by other application to connect to the JBoxServer
	 * @param sUser
	 *            the user from the certificate
	 * @param iDebug
	 *            the debug level received from the command line
	 */
	private static boolean writeTokenFile(final String sHost, final int iWSPort, final String sUser, @SuppressWarnings("unused") final String sHomeUser, final int iDebug) {
		String sUserId = System.getProperty("userid");

		if (sUserId == null || sUserId.length() == 0) {
			sUserId = SystemCommand.bash("id -u " + System.getProperty("user.name")).stdout;

			if (sUserId != null && sUserId.length() > 0)
				System.setProperty("userid", sUserId);
			else {
				logger.severe("User Id empty! Could not get the token file name");
				return false;
			}
		}

		try {
			final int iUserId = Integer.parseInt(sUserId.trim());

			final File tmpDir = new File(System.getProperty("java.io.tmpdir"));

			final File tokenFile = new File(tmpDir, "jclient_token_" + iUserId);

			try (FileWriter fw = new FileWriter(tokenFile, true)) {
				// fw.write("Host=" + sHost + "\n");
				// logger.fine("Host = " + sHost);

				fw.write("WSPort=" + iWSPort + "\n");
				logger.fine("WSPort = " + iWSPort);

				// fw.write("User=" + sUser + "\n");
				// logger.fine("User = " + sUser);

				// fw.write("Home=" + sHomeUser + "\n");
				// logger.fine("Home = " + sHomeUser);

				// fw.write("Debug=" + iDebug + "\n");
				// logger.fine("Debug = " + iDebug);

				// fw.write("PID=" + MonitorFactory.getSelfProcessID() + "\n");
				// logger.fine("PID = " + MonitorFactory.getSelfProcessID());

				fw.flush();
				fw.close();

				return true;

			} catch (final Exception e1) {
				logger.log(Level.SEVERE, "Could not open file " + tokenFile + " to write", e1);
				return false;
			}
		} catch (final Throwable e) {
			logger.log(Level.SEVERE, "Could not get user id! The token file could not be created ", e);

			return false;
		}
	}

	/**
	 * Writes the environment file used by ROOT <br />
	 * It needs to be named jclient_env_$UID, sitting by default in <code>java.io.tmpdir</code> (eg. <code>/tmp</code>) and to contain:
	 * <ol>
	 * <li>alien_API_HOST</li>
	 * <li>alien_API_PORT</li>
	 * <li>alien_API_USER</li>
	 * <li>LD/DYLD_LIBRARY_PATH</li>
	 * </ol>
	 *
	 * @param iPort
	 * @param sPassword
	 * @param sUser
	 * @return <code>true</code> if everything went fine, <code>false</code> if there was an error writing the env file
	 */
	private static boolean writeEnvFile(final String sHost, final int iPort, final String sUser) {
		final String sUserId = System.getProperty("userid");

		if (sUserId == null || sUserId.length() == 0) {
			logger.severe("User Id empty! Could not get the env file name");
			return false;
		}

		try {
			final int iUserId = Integer.parseInt(sUserId.trim());

			final File tmpDir = new File(System.getProperty("java.io.tmpdir"));

			final File envFile = new File(tmpDir, "jclient_env_" + iUserId);

			try (FileWriter fw = new FileWriter(envFile)) {
				fw.write("export alien_API_HOST=" + sHost + "\n");
				logger.fine("export alien_API_HOST=" + sHost);

				fw.write("export alien_API_PORT=" + iPort + "\n");
				logger.fine("export alien_API_PORT=" + iPort);

				fw.write("export alien_API_USER=" + sUser + "\n");
				logger.fine("export alien_API_USER=" + sUser);

				fw.write("export JROOT=1\n");
				logger.fine("export JROOT=1");

				fw.flush();
				fw.close();

				return true;

			} catch (final Exception e1) {
				logger.log(Level.SEVERE, "Could not open file " + envFile.getAbsolutePath() + " to write", e1);
				return false;
			}
		} catch (final Exception e) {
			logger.log(Level.SEVERE, "Could not get user id! The env file could not be created ", e);
			return false;
		}
	}

	/**
	 * Change permissions of the file
	 */
	private static boolean changeMod(final File file, final int chmod) {
		if (file.exists()) {
			try {
				final CommandOutput co = SystemCommand.bash("chmod " + chmod + " " + file.getCanonicalPath(), false);

				if (co.exitCode != 0)
					System.err.println("Could not change permissions: " + co.stderr);

				return co.exitCode == 0;

			} catch (@SuppressWarnings("unused") final IOException e) {
				// ignore
			}
		}
		return false;
	}

	/**
	 * Request token certificate from JCentral
	 */
	private static boolean requestTokenCert() {
		// Get user certificate to connect to JCentral
		Certificate[] cert = null;
		AliEnPrincipal userIdentity = null;
		try {
			cert = JAKeyStore.getKeyStore().getCertificateChain("User.cert");
			if (cert == null) {
				logger.log(Level.SEVERE, "Failed to load certificate");
				return false;
			}
		} catch (KeyStoreException e) {
			e.printStackTrace();
		}

		if (cert instanceof X509Certificate[]) {
			X509Certificate[] x509cert = (X509Certificate[]) cert;
			userIdentity = UserFactory.getByCertificate(x509cert);
		}
		if (userIdentity == null) {
			logger.log(Level.SEVERE, "Failed to get user identity");
			return false;
		}

		// Two files will be the result of this command
		// Check if their location is set by env variables or in config, otherwise put default location in $USER_HOME/.globus/
		String tokencertpath = ConfigUtils.getConfig().gets("tokencert.path",
				System.getProperty("user.home") + System.getProperty("file.separator") + ".globus" + System.getProperty("file.separator") + "tokencert.pem");
		String tokenkeypath = ConfigUtils.getConfig().gets("tokenkey.path",
				System.getProperty("user.home") + System.getProperty("file.separator") + ".globus" + System.getProperty("file.separator") + "tokenkey.pem");

		File tokencertfile = new File(tokencertpath);
		File tokenkeyfile = new File(tokenkeypath);

		// Allow to modify those files if they already exist
		changeMod(tokencertfile, 777);
		changeMod(tokenkeyfile, 777);

		try ( // Open files for writing
				PrintWriter pwritercert = new PrintWriter(tokencertfile);
				PrintWriter pwriterkey = new PrintWriter(tokenkeyfile);

				// We will read all data into temp output stream and then parse it and split into 2 files
				ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
			UIPrintWriter out = new JSONPrintWriter(baos);

			// Create Commander instance just to execute one command
			JAliEnCOMMander commander = new JAliEnCOMMander(userIdentity, null, null, out);
			commander.start();

			// Command to be sent (yes, we need it to be an array, even if it is one word)
			final ArrayList<String> fullCmd = new ArrayList<>();
			fullCmd.add("token");

			synchronized (commander) {
				commander.status.set(1);
				commander.setLine(out, fullCmd.toArray(new String[0]));
				commander.notifyAll();
			}

			while (commander.status.get() == 1)
				try {
					synchronized (commander.status) {
						commander.status.wait(1000);
					}
				} catch (@SuppressWarnings("unused") final InterruptedException ie) {
					// ignore
				}

			// Now parse the reply from JCentral
			JSONParser jsonParser = new JSONParser();
			JSONObject readf = (JSONObject) jsonParser.parse(baos.toString());
			JSONArray jsonArray = (JSONArray) readf.get("results");
			for (Object object : jsonArray) {
				JSONObject aJson = (JSONObject) object;
				pwritercert.print(aJson.get("tokencert"));
				pwriterkey.print(aJson.get("tokenkey"));
				pwritercert.flush();
				pwriterkey.flush();
			}

			// Set correct permissions
			changeMod(tokencertfile, 440);
			changeMod(tokenkeyfile, 400);

			// Execution finished - kill commander
			commander.kill = true;
			return true;

		} catch (final Exception e) {
			logger.log(Level.SEVERE, "Token request failed", e);
			return false;
		}
	}

	/**
	 * Singleton
	 */
	static TomcatServer tomcatServer = null;

	/**
	 * Start Tomcat Server
	 *
	 * @param iDebugLevel
	 */
	public static synchronized void startTomcatServer(final int iDebugLevel) {

		if (tomcatServer != null)
			return;

		logger.log(Level.INFO, "Tomcat starting ...");

		// Request token certificate from JCentral
		if (!ConfigUtils.isCentralService()) {
			if (!requestTokenCert()) {
				return;
			}
			// Create keystore for token certificate
			try {
				if (!JAKeyStore.loadTokenKeyStorage()) {
					System.err.println("Token Certificate could not be loaded.");
					System.err.println("Exiting...");
					return;
				}
			} catch (final Exception e) {
				logger.log(Level.SEVERE, "Error loading token", e);
				System.err.println("Error loading token");
				return;
			}
		}

		// Set dynamic port range for Tomcat server
		int portMin = Integer.parseInt(ConfigUtils.getConfig().gets("port.range.start", "10100"));
		int portMax = Integer.parseInt(ConfigUtils.getConfig().gets("port.range.end", "10200"));
		int port = 8097;

		// Try to launch Tomcat on default port
		try (ServerSocket ssocket = new ServerSocket(port, 10, InetAddress.getByName("127.0.0.1"))) // Fast check if port is available
		{
			ssocket.close();
			// Actually start Tomcat
			tomcatServer = new TomcatServer(port, iDebugLevel);

			logger.log(Level.INFO, "Tomcat listening on port " + port);
			System.out.println("Tomcat is listening on port " + port);
			return; // Everything's ok, exit

		} catch (final Exception ioe) {
			// Port is already in use, maybe there's another user on the machine...
			logger.log(Level.FINE, "Tomcat: Could not listen on port " + port, ioe);
		}

		// Try another ports in range
		for (port = portMin; port < portMax; port++) {
			try (ServerSocket ssocket = new ServerSocket(port, 10, InetAddress.getByName("127.0.0.1"))) // Fast check if port is available
			{
				ssocket.close();
				// Actually start Tomcat
				tomcatServer = new TomcatServer(port, iDebugLevel);

				logger.log(Level.INFO, "Tomcat listening on port " + port);
				System.out.println("Tomcat is listening on port " + port);
				break;
			} catch (final Exception ioe) {
				// Try next one
				logger.log(Level.FINE, "Tomcat: Could not listen on port " + port, ioe);
			}
		}
	}
}
