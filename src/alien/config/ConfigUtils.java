/**
 *
 */
package alien.config;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
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
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.Properties;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import alien.user.LDAPHelper;
import lazyj.DBFunctions;
import lazyj.ExtProperties;
import lazyj.cache.ExpirationCache;
import lazyj.commands.SystemCommand;
import lia.Monitor.monitor.AppConfig;

/**
 * @author costing
 * @since Nov 3, 2010
 */
public class ConfigUtils {
	private static ExpirationCache<String, String> seenLoggers = new ExpirationCache<>();

	/**
	 * Logger
	 */
	static transient final Logger logger;

	private static final Map<String, ExtProperties> dbConfigFiles;

	private static final Map<String, ExtProperties> otherConfigFiles;

	private static final String CONFIG_FOLDER;

	private static LoggingConfigurator logging = null;

	private static final ExtProperties appConfig;

	private static boolean hasDirectDBConnection = false;

	static {
		String sConfigFolder = null;

		final HashMap<String, ExtProperties> dbconfig = new HashMap<>();

		final HashMap<String, ExtProperties> otherconfig = new HashMap<>();

		ExtProperties applicationConfig = null;

		final Map<String, ExtProperties> foundProperties = new HashMap<>();

		ExtProperties logConfig = null;

		try {
			for (String name : getResourceListing(ConfigUtils.class, "config/"))
				if (name.endsWith(".properties")) {
					if (name.indexOf('/') > 0)
						name = name.substring(name.lastIndexOf('/') + 1);

					final String key = name.substring(0, name.indexOf('.'));

					try (InputStream is = ConfigUtils.class.getClassLoader().getResourceAsStream("config/" + name)) {
						final ExtProperties prop = new ExtProperties(is);
						foundProperties.put(key, prop);
					}
				}
		} catch (@SuppressWarnings("unused")
		final Throwable t) {
			// cannot load the default configuration files for any reason
		}

		// configuration files in the indicated config folder overwrite the defaults from classpath

		final String defaultConfigLocation = System.getProperty("user.home") + System.getProperty("file.separator") + ".alien" + System.getProperty("file.separator") + "config";

		final String configOption = System.getProperty("AliEnConfig", "config");

		final List<String> configFolders = Arrays.asList(defaultConfigLocation, configOption);

		for (final String path : configFolders) {
			final File f = new File(path);

			if (f.exists() && f.isDirectory() && f.canRead()) {
				final File[] list = f.listFiles();

				if (list != null)
					for (final File sub : list)
						if (sub.isFile() && sub.canRead() && sub.getName().endsWith(".properties")) {
							String sName = sub.getName();
							sName = sName.substring(0, sName.lastIndexOf('.'));

							ExtProperties oldProperties = foundProperties.get(sName);

							if (oldProperties == null)
								oldProperties = new ExtProperties();

							final ExtProperties prop = new ExtProperties(path, sName, oldProperties, true);
							prop.setAutoReload(1000 * 60);

							foundProperties.put(sName, prop);

							// record the last path where some configuration files were loaded from
							sConfigFolder = path;
						}
			}
		}

		for (final Map.Entry<String, ExtProperties> entry : foundProperties.entrySet()) {
			final String sName = entry.getKey();
			final ExtProperties prop = entry.getValue();

			if (sName.equals("config"))
				applicationConfig = prop;
			else {
				prop.makeReadOnly();

				if (sName.equals("logging"))
					logConfig = prop;
				else
					if (prop.gets("driver").length() > 0) {
						dbconfig.put(sName, prop);

						if (prop.gets("password").length() > 0)
							hasDirectDBConnection = true;
					}
					else
						otherconfig.put(sName, prop);
			}
		}

		CONFIG_FOLDER = sConfigFolder;

		dbConfigFiles = Collections.unmodifiableMap(dbconfig);

		otherConfigFiles = Collections.unmodifiableMap(otherconfig);

		final String mlConfigURL = System.getProperty("lia.Monitor.ConfigURL");

		final boolean hasMLConfig = mlConfigURL != null && mlConfigURL.trim().length() > 0;

		if (applicationConfig != null)
			appConfig = applicationConfig;
		else
			if (hasMLConfig) {
				// Started from ML, didn't have its own configuration, so copy the configuration keys from ML's main config file
				appConfig = new ExtProperties(AppConfig.getPropertiesConfigApp());
				appConfig.set("jalien.configure.logging", "false");
			}
			else
				appConfig = new ExtProperties();

		for (final Map.Entry<Object, Object> entry : System.getProperties().entrySet())
			appConfig.set(entry.getKey().toString(), entry.getValue().toString());

		// now let's configure the logging, if allowed to
		if (appConfig.getb("jalien.configure.logging", true) && logConfig != null) {
			logging = new LoggingConfigurator(logConfig);

			// tell ML not to configure its logger
			System.setProperty("lia.Monitor.monitor.LoggerConfigClass.preconfiguredLogging", "true");

			// same to lazyj
			System.setProperty("lazyj.use_java_logger", "true");
		}

		if (!hasMLConfig) {
			// write a copy of our main configuration content and, if any, a separate ML configuration file to ML's configuration registry
			for (final String configFile : new String[] { "config", "mlconfig", "App" }) {
				final ExtProperties eprop = foundProperties.get(configFile);

				if (eprop != null) {
					final Properties prop = eprop.getProperties();

					for (final String key : prop.stringPropertyNames())
						AppConfig.setProperty(key, prop.getProperty(key));
				}
			}
		}
		else {
			// assume running as a library inside ML code, inherit the configuration keys from its main config file
			final Properties mlConfigProperties = AppConfig.getPropertiesConfigApp();

			for (final String key : mlConfigProperties.stringPropertyNames())
				appConfig.set(key, mlConfigProperties.getProperty(key));
		}

		appConfig.makeReadOnly();

		logger = ConfigUtils.getLogger(ConfigUtils.class.getCanonicalName());

		if (logger.isLoggable(Level.FINE))
			logger.log(Level.FINE,
					"Configuration loaded. Own logging configuration: " + (logging != null ? "true" : "false") + ", ML configuration detected: " + hasMLConfig + ", config folder: " + CONFIG_FOLDER);
	}

	/**
	 * @param referenceClass
	 * @param path
	 * @return the listing of this
	 * @throws URISyntaxException
	 * @throws UnsupportedEncodingException
	 * @throws IOException
	 */
	public static Collection<String> getResourceListing(final Class<?> referenceClass, final String path) throws URISyntaxException, UnsupportedEncodingException, IOException {
		URL dirURL = referenceClass.getClassLoader().getResource(path);
		if (dirURL != null && dirURL.getProtocol().equals("file")) {
			/* A file path: easy enough */
			return Arrays.asList(new File(dirURL.toURI()).list());
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
						if (checkSubdir >= 0) {
							// if it is a subdirectory, we just return the directory name
							entry = entry.substring(0, checkSubdir);
						}
						result.add(entry);
					}
				}
				return result;
			}
		}

		throw new UnsupportedOperationException("Cannot list files for URL " + dirURL);
	}

	/**
	 * @return the base directory where the configuration files are
	 */
	public static final String getConfigFolder() {
		return CONFIG_FOLDER;
	}

	/**
	 * @return <code>true</code> if direct database access is available
	 */
	public static final boolean isCentralService() {
		return hasDirectDBConnection;
	}

	/**
	 * Get all database-related configuration files
	 *
	 * @return db configurations
	 */
	public static final Map<String, ExtProperties> getDBConfiguration() {
		return dbConfigFiles;
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
		final ExtProperties p = dbConfigFiles.get(key);

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
		return appConfig;
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
	static class LoggingConfigurator implements Observer {
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

			prop.addObserver(this);

			update(null, null);
		}

		@Override
		public void update(final Observable o, final Object arg) {
			final ByteArrayOutputStream baos = new ByteArrayOutputStream();

			try {
				prop.getProperties().store(baos, "AliEn Loggging Properties");
			} catch (final Throwable t) {
				System.err.println("Cannot store default props");
				t.printStackTrace();
			}

			final byte[] buff = baos.toByteArray();

			final ByteArrayInputStream bais = new ByteArrayInputStream(buff);

			try {
				LogManager.getLogManager().readConfiguration(bais);
			} catch (final Throwable t) {
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
			domain = hostName.substring(hostName.indexOf(".") + 1, hostName.length());
		} catch (final UnknownHostException e) {
			logger.severe("Error: couldn't get hostname");
			e.printStackTrace();
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
		if (appConfig != null) {
			final Properties props = appConfig.getProperties();
			for (final Object s : props.keySet()) {
				final String key = (String) s;
				configuration.put(key, props.get(key));
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
		System.out.println("Config folder: " + CONFIG_FOLDER);
		System.out.println("Has direct db connection: " + hasDirectDBConnection);

		dumpConfiguration("config", appConfig);

		if (logging != null)
			dumpConfiguration("logging", logging.prop);

		System.out.println("\nDatabase connections:");
		for (final Map.Entry<String, ExtProperties> entry : dbConfigFiles.entrySet())
			dumpConfiguration(entry.getKey(), entry.getValue());

		System.out.println("\nOther configuration files:");

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
}
