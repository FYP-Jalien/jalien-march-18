package alien;

import java.util.logging.Level;
import java.util.logging.Logger;

import alien.api.DispatchSSLClient;
import alien.api.JBoxServer;
import alien.api.TomcatServer;
import alien.config.ConfigUtils;
import alien.user.JAKeyStore;

/**
 * @author ron
 * @since Jun 21, 2011
 */
public class JBox {

	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(JBoxServer.class.getCanonicalName());

	private static void logLoud(final Level l, final String msg) {
		logger.log(l, msg);
		System.err.println("> " + msg); // Prepend stderr messages with ">" to make JShell print them
	}

	/**
	 * Debugging method
	 *
	 * @param args
	 */
	public static void main(final String[] args) {
		ConfigUtils.setApplicationName("JBox");
		DispatchSSLClient.setIdleTimeout(10000);

		logLoud(Level.FINE, "Starting JBox");

		// Load certificates
		if (!JAKeyStore.loadKeyStore()) {
			logLoud(Level.SEVERE, "ERROR: JBox failed to load any credentials");
			return;
		}

		if (!JAKeyStore.bootstrapFirstToken()) {
			logLoud(Level.SEVERE, "ERROR: JBox failed to get a token");
			return;
		}

		if (JAKeyStore.isLoaded("token") && !JAKeyStore.isLoaded("user") && !JAKeyStore.isLoaded("host")) {
			logLoud(Level.INFO, "WARNING: JBox is connected to central services with a token that cannot be used to update itself.");
			logLoud(Level.INFO, "Please use a user or host certificate to refresh tokens automatically.");
		}

		final long expirationTime = JAKeyStore.getExpirationTime(JAKeyStore.getKeyStore());
		if (JAKeyStore.expireSoon(expirationTime))
			JAKeyStore.printExpirationTime(expirationTime);

		// Start JBox and Tomcat services
		JBoxServer.startJBoxService();
		TomcatServer.startTomcatServer();

		JAKeyStore.startTokenUpdater();
		TomcatServer.startConnectorReloader();

		// Create /tmp/jclient_token file and export env variables
		if (!ConfigUtils.writeJClientFile(ConfigUtils.exportJBoxVariables()))
			logLoud(Level.INFO, "Failed to export JBox variables");
	}
}
