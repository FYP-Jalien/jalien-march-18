/**
 *
 */
package alien.config;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import lazyj.DBFunctions;
import lazyj.ExtProperties;
import lazyj.cache.ExpirationCache;
import lazyj.commands.SystemCommand;

import lia.Monitor.monitor.AppConfig;

import alien.user.LDAPHelper;

/**
 * @author costing
 * @since Nov 3, 2010
 */
public class ConfigUtils {
	private static ExpirationCache<String, String> seenLoggers = new ExpirationCache<>();

	static transient Logger logger;

	private static Map<String, ExtProperties> otherConfigFiles;

	private static LoggingConfigurator logging = null;

	private static boolean hasDirectDBConnection = false;

	private static ConfigManager cfgManager;

	private static void configureLogging() {
		// now let's configure the logging, if allowed to
		ExtProperties fileConfig = otherConfigFiles.get("config");
		if (fileConfig.getb("jalien.configure.logging", true) && otherConfigFiles.containsKey("logging")) {
			logging = new LoggingConfigurator(otherConfigFiles.get("logging"));

			// tell ML not to configure its logger
			System.setProperty("lia.Monitor.monitor.LoggerConfigClass.preconfiguredLogging", "true");

			// same to lazyj
			System.setProperty("lazyj.use_java_logger", "true");
		}
	}

	private static boolean detectDirectDBConnection(final Map<String, ExtProperties> config) {
		boolean detected = false;

		for (final Map.Entry<String, ExtProperties> entry : config.entrySet()) {
			final ExtProperties prop = entry.getValue();

			if (prop.gets("driver").length() > 0 && prop.gets("password").length() > 0) {
				detected = true;
			}
		}

		return detected;
	}

	/**
	 * Helper method to check if ML config is valid. Reads system properties.
	 *
	 * @return true if lia.Monitor.ConfigURL property is set and non-empty
	 */
	public static boolean hasMLConfig() {
		final String mlConfigURL = System.getProperty("lia.Monitor.ConfigURL");
		return mlConfigURL != null && mlConfigURL.trim().length() > 0;
	}

	private static void storeMlConfig() {
		// Configure the MonaLisa target
		if (!hasMLConfig())
			// write a copy of our main configuration content and, if any, a separate ML configuration file to ML's configuration registry
			for (final String configFile : new String[] { "config", "mlconfig", "App" }) {
				final ExtProperties eprop = otherConfigFiles.get(configFile);

				if (eprop != null) {
					final Properties prop = eprop.getProperties();

					for (final String key : prop.stringPropertyNames())
						AppConfig.setProperty(key, prop.getProperty(key));
				}
			}
		AppConfig.reloadProps();
	}

	private static ConfigManager getDefaultConfigManager() {
		ConfigManager manager = new ConfigManager();
		manager.registerPrimary(new BuiltinConfiguration());
		manager.registerPrimary(new ConfigurationFolders(manager.getConfiguration()));
		manager.registerPrimary(new SystemConfiguration());
		manager.registerPrimary(new MLConfigurationSource());
		boolean isCentralService = detectDirectDBConnection(manager.getConfiguration());
		manager.registerFallback(new DBConfigurationSource(manager.getConfiguration(), isCentralService));
		return manager;
	}

	/**
	 * Initialize ConfigUtils with given ConfigManager.
	 *
	 * This method is called when the class is loaded (see the static block).
	 * The default ConfigManager is constructed in that case.
	 *
	 * This method is usually used only for testing.
	 *
	 * @param m ConfigManager to be used for initialization.
	 */
	public static void init(ConfigManager m) {
		cfgManager = m;
		otherConfigFiles = cfgManager.getConfiguration();
		hasDirectDBConnection = detectDirectDBConnection(otherConfigFiles);
		configureLogging();
		storeMlConfig();

		// Create local logger
		logger = ConfigUtils.getLogger(ConfigUtils.class.getCanonicalName());

		if (logger.isLoggable(Level.FINE))
			logger.log(Level.FINE, "Configuration loaded. Own logging configuration: " + (logging != null ? "true" : "false") + ", ML configuration detected: " + hasMLConfig());
	}

	static {
		init(getDefaultConfigManager());
	}

	private static String userDefinedAppName = null;

	/**
	 * Get the application name, to be used for example in Xrootd transfers in order to be able to group transfers by application. Should be a simple tag, without any IDs in it (to allow grouping).
	 * The value is taken from either the user-defined application name ({@link #setApplicationName(String)} or from the "app.name" configuration variable, or if none of them is defined then it falls
	 * back to the user-specified default value
	 *
	 * @param defaultAppName
	 *            value to return if nothing else is known about the current application
	 * @return the application name
	 * @see #setApplicationName(String)
	 */
	public static String getApplicationName(final String defaultAppName) {
		if (userDefinedAppName != null)
			return userDefinedAppName;

		return getConfig().gets("app.name", defaultAppName);
	}

	/**
	 * Set an explicit application name to be used for example in Xrootd transfer requests. This value will take precedence in front of the "app.config" configuration key or other default values.
	 *
	 * @param appName
	 * @return the previous value of the user-defined application name
	 * @see #getApplicationName(String)
	 */
	public static String setApplicationName(final String appName) {
		final String oldValue = userDefinedAppName;

		userDefinedAppName = appName;

		return oldValue;
	}

	/**
	 * @param referenceClass
	 * @param path
	 * @return the listing of this
	 * @throws URISyntaxException
	 * @throws UnsupportedEncodingException
	 * @throws IOException
	 */
	// TODO: move this method from ConfigUtils to BuiltinProperties
	public static Collection<String> getResourceListing(final Class<?> referenceClass, final String path) throws URISyntaxException, UnsupportedEncodingException, IOException {
		URL dirURL = referenceClass.getClassLoader().getResource(path);
		if (dirURL != null && dirURL.getProtocol().equals("file")) {
			/* A file path: easy enough */
			final String[] listing = new File(dirURL.toURI()).list();

			if (listing != null)
				return Arrays.asList(listing);

			return Collections.emptyList();
		}

		if (dirURL == null) {
			/*
			 * In case of a jar file, we can't actually find a directory.
			 * Have to assume the same jar as clazz.
			 */
			final String me = referenceClass.getName().replace(".", "/") + ".class";
			dirURL = referenceClass.getClassLoader().getResource(me);
		}

		if (dirURL.getProtocol().equals("jar")) {
			/* A JAR path */
			final String jarPath = dirURL.getPath().substring(5, dirURL.getPath().indexOf("!")); // strip out only the JAR file
			try (JarFile jar = new JarFile(URLDecoder.decode(jarPath, "UTF-8"))) {
				final Enumeration<JarEntry> entries = jar.entries(); // gives ALL entries in jar
				final Set<String> result = new HashSet<>(); // avoid duplicates in case it is a subdirectory
				while (entries.hasMoreElements()) {
					final String name = entries.nextElement().getName();
					if (name.startsWith(path)) { // filter according to the path
						String entry = name.substring(path.length());
						final int checkSubdir = entry.indexOf("/");
						if (checkSubdir >= 0)
							// if it is a subdirectory, we just return the directory name
							entry = entry.substring(0, checkSubdir);
						result.add(entry);
					}
				}
				return result;
			}
		}

		throw new UnsupportedOperationException("Cannot list files for URL " + dirURL);
	}

	/**
	 * @return <code>true</code> if direct database access is available
	 */
	public static final boolean isCentralService() {
		return hasDirectDBConnection;
	}

	/**
	 * Get a DB connection to a specific database key. The code relies on the <i>AlienConfig</i> system property to point to a base directory where files named <code>key</code>.properties can be
	 * found. If a file for this key can be found it is returned to the caller, otherwise a <code>null</code> value is returned.
	 *
	 * @param key
	 *            database class, something like &quot;catalogue_admin&quot;
	 * @return the database connection, or <code>null</code> if it is not available.
	 */
	public static final DBFunctions getDB(final String key) {
		final ExtProperties p = getConfiguration(key);

		if (p == null)
			return null;

		return new DBFunctions(p);
	}

	/**
	 * Get the global application configuration
	 *
	 * @return application configuration
	 */
	public static final ExtProperties getConfig() {
		return otherConfigFiles.get("config");
	}

	/**
	 * Get the contents of the configuration file indicated by the key
	 *
	 * @param key
	 * @return configuration contents
	 */
	public static final ExtProperties getConfiguration(final String key) {
		return otherConfigFiles.get(key);
	}

	/**
	 * Set the Java logging properties and subscribe to changes on the configuration files
	 *
	 * @author costing
	 * @since Nov 3, 2010
	 */
	static class LoggingConfigurator implements PropertyChangeListener {
		/**
		 * Logging configuration content, usually loaded from "logging.properties"
		 */
		final ExtProperties prop;

		/**
		 * Set the logging configuration
		 *
		 * @param p
		 */
		LoggingConfigurator(final ExtProperties p) {
			prop = p;

			prop.addPropertyChangeListener(null, this);

			propertyChange(null);
		}

		@Override
		public void propertyChange(final PropertyChangeEvent arg0) {
			final ByteArrayOutputStream baos = new ByteArrayOutputStream();

			try {
				prop.getProperties().store(baos, "AliEn Loggging Properties");
			}
			catch (final Throwable t) {
				System.err.println("Cannot store default props");
				t.printStackTrace();
			}

			final byte[] buff = baos.toByteArray();

			final ByteArrayInputStream bais = new ByteArrayInputStream(buff);

			try {
				LogManager.getLogManager().readConfiguration(bais);
			}
			catch (final Throwable t) {
				System.err.println("Cannot load default props into LogManager");
				t.printStackTrace();
			}
		}
	}

	/**
	 * Get the logger for this component
	 *
	 * @param component
	 * @return the logger
	 */
	public static Logger getLogger(final String component) {
		final Logger l = Logger.getLogger(component);

		if (logging != null) {
			final String s = seenLoggers.get(component);

			if (s == null)
				seenLoggers.put(component, component, 60 * 1000);
		}

		if (l.getFilter() == null)
			l.setFilter(LoggingFilter.getInstance());

		return l;
	}

	private static final String jAliEnVersion = "0.0.1";

	/**
	 * @return JAlien version
	 */
	public static final String getVersion() {
		return jAliEnVersion;
	}

	/**
	 * @return machine platform
	 */
	public static final String getPlatform() {
		final String unameS = SystemCommand.bash("uname -s").stdout.trim();
		final String unameM = SystemCommand.bash("uname -m").stdout.trim();
		return unameS + "-" + unameM;
	}

	/**
	 * @return the site name closest to where this JVM runs
	 */
	public static String getSite() {
		// TODO implement this properly
		return "CERN";
	}

	/**
	 * @return global config map
	 */
	public static HashMap<String, Object> getConfigFromLdap() {
		return getConfigFromLdap(false);
	}

	/**
	 * @param checkContent
	 * @return global config map
	 */
	public static HashMap<String, Object> getConfigFromLdap(final boolean checkContent) {
		final HashMap<String, Object> configuration = new HashMap<>();
		// Get hostname and domain
		String hostName = "";
		String domain = "";
		try {
			hostName = InetAddress.getLocalHost().getCanonicalHostName();
			hostName = hostName.replace("/.$/", "");
			hostName = hostName.replace("dyndns.cern.ch", "cern.ch");
			domain = hostName.substring(hostName.indexOf(".") + 1, hostName.length());
		}
		catch (final UnknownHostException e) {
			logger.severe("Error: couldn't get hostname: " + e.toString());
			return null;
		}

		final HashMap<String, Object> voConfig = LDAPHelper.getVOConfig();
		if (voConfig == null || voConfig.size() == 0)
			return null;

		// We get the site name from the domain and the site root info
		final Set<String> siteset = LDAPHelper.checkLdapInformation("(&(domain=" + domain + "))", "ou=Sites,", "accountName");

		if (checkContent && (siteset == null || siteset.size() == 0 || siteset.size() > 1)) {
			logger.severe("Error: " + (siteset == null ? "null" : String.valueOf(siteset.size())) + " sites found for domain: " + domain);
			return null;
		}

		// users won't be always in a registered domain so we can exit here
		if (siteset.size() == 0 && !checkContent)
			return configuration;

		final String site = siteset.iterator().next();

		// Get the root site config based on site name
		final HashMap<String, Object> siteConfig = LDAPHelper.checkLdapTree("(&(ou=" + site + ")(objectClass=AliEnSite))", "ou=Sites,", "site");

		if (checkContent && siteConfig.size() == 0) {
			logger.severe("Error: cannot find site root configuration in LDAP for site: " + site);
			return null;
		}

		// Get the hostConfig from LDAP based on the site and hostname
		final HashMap<String, Object> hostConfig = LDAPHelper.checkLdapTree("(&(host=" + hostName + "))", "ou=Config,ou=" + site + ",ou=Sites,", "host");

		if (checkContent && hostConfig.size() == 0) {
			logger.severe("Error: cannot find host configuration in LDAP for host: " + hostName);
			return null;
		}

		if (checkContent && !hostConfig.containsKey("host_ce")) {
			logger.severe("Error: cannot find ce configuration in hostConfig for host: " + hostName);
			return null;
		}

		if (hostConfig.containsKey("host_ce")) {
			// Get the CE information based on the site and ce name for the host
			final HashMap<String, Object> ceConfig = LDAPHelper.checkLdapTree("(&(name=" + hostConfig.get("host_ce") + "))", "ou=CE,ou=Services,ou=" + site + ",ou=Sites,", "ce");

			if (checkContent && ceConfig.size() == 0) {
				logger.severe("Error: cannot find ce configuration in LDAP for CE: " + hostConfig.get("host_ce"));
				return null;
			}

			configuration.putAll(ceConfig);
		}
		// We put the config together
		configuration.putAll(voConfig);
		configuration.putAll(siteConfig);
		configuration.putAll(hostConfig);

		// Overwrite values
		configuration.put("organisation", "ALICE");
		// if (appConfig != null) {
		if (otherConfigFiles.containsKey("config")) {
			// final Properties props = appConfig.getProperties();
			final Properties props = otherConfigFiles.get("config").getProperties();
			for (final Object s : props.keySet()) {
				final String key = (String) s;
				configuration.put(key, props.get(key));
			}
		}

		// We create the folders logdir, cachedir, tmpdir, workdir
		final HashMap<String, String> folders_config = new HashMap<>();

		if (configuration.containsKey("host_tmpdir"))
			folders_config.put("tmpdir", (String) configuration.get("host_tmpdir"));
		else
			if (configuration.containsKey("site_tmpdir"))
				folders_config.put("tmpdir", (String) configuration.get("site_tmpdir"));

		if (configuration.containsKey("host_cachedir"))
			folders_config.put("cachedir", (String) configuration.get("host_cachedir"));
		else
			if (configuration.containsKey("site_cachedir"))
				folders_config.put("cachedir", (String) configuration.get("site_cachedir"));

		if (configuration.containsKey("host_logdir"))
			folders_config.put("logdir", (String) configuration.get("host_logdir"));
		else
			if (configuration.containsKey("site_logdir"))
				folders_config.put("logdir", (String) configuration.get("site_logdir"));

		for (final String folder : folders_config.keySet()) {
			final String folderpath = folders_config.get(folder);
			try {
				final File folderf = new File(folderpath);
				if (!folderf.exists()) {
					final boolean created = folderf.mkdirs();
					if (!created)
						logger.severe("Directory for " + folder + "can't be created: " + folderpath);
				}
			}
			catch (final Exception e) {
				logger.severe("Exception on directory creation: " + e.toString());
			}
		}

		return configuration;
	}

	/**
	 * Configuration debugging
	 *
	 * @param args
	 */
	public static void main(final String[] args) {
		System.out.println("Has direct db connection: " + hasDirectDBConnection);

		for (final Map.Entry<String, ExtProperties> entry : otherConfigFiles.entrySet())
			dumpConfiguration(entry.getKey(), entry.getValue());
	}

	private static void dumpConfiguration(final String configName, final ExtProperties content) {
		System.out.println("Dumping configuration content of *" + configName + "*");

		if (content == null) {
			System.out.println("  <null content>");
			return;
		}

		System.out.println("It was loaded from *" + content.getConfigFileName() + "*");

		final Properties p = content.getProperties();

		for (final String key : p.stringPropertyNames())
			System.out.println("    " + key + " : " + p.getProperty(key));
	}

	/**
	 * Get the closest site mapped to current location of the client.
	 * 
	 * @return the close site (or where the job runs), as pointed by the env variable <code>ALIEN_SITE</code>, or, if not defined, the configuration key <code>alice_close_site</code>
	 */
	public static String getCloseSite() {
		final String envSite = ConfigUtils.getConfig().gets("ALIEN_SITE");

		if (envSite.length() > 0)
			return envSite;

		// TODO: actual mapping of the client to a site, no default configuration key for end users

		final String configKey = ConfigUtils.getConfig().gets("alice_close_site");

		if (configKey.length() > 0)
			return configKey;

		return "CERN";
	}
}
