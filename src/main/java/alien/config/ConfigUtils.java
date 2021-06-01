package alien.config;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
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

import alien.api.JBoxServer;
import alien.api.TomcatServer;
import alien.catalogue.access.AuthorizationFactory;
import alien.monitoring.MonitorFactory;
import alien.site.Functions;
import alien.user.AliEnPrincipal;
import alien.user.LDAPHelper;
import alien.user.UserFactory;
import alien.user.UsersHelper;
import lazyj.DBFunctions;
import lazyj.ExtProperties;
import lazyj.Format;
import lazyj.Utils;
import lazyj.cache.ExpirationCache;
import lazyj.commands.CommandOutput;
import lazyj.commands.SystemCommand;
import lia.Monitor.monitor.AppConfig;

/**
 * @author costing
 * @since Nov 3, 2010
 */
public class ConfigUtils {
	private static ExpirationCache<String, String> seenLoggers = new ExpirationCache<>(1024);

	private static Logger logger;

	private static Map<String, ExtProperties> otherConfigFiles;

	private static LoggingConfigurator logging = null;

	private static boolean hasDirectDBConnection = false;

	private static ConfigManager cfgManager;

	private static void configureLogging() {
		// now let's configure the logging, if allowed to
		final ExtProperties fileConfig = otherConfigFiles.get("config");
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
			for (final String configFile : new String[] { "config", "mlconfig", "app" }) {
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
		final ConfigManager manager = new ConfigManager();
		manager.registerPrimary(new BuiltinConfiguration());
		manager.registerPrimary(new ConfigurationFolders(manager.getConfiguration()));
		manager.registerPrimary(new SystemConfiguration());
		manager.registerPrimary(new MLConfigurationSource());
		final boolean isCentralService = detectDirectDBConnection(manager.getConfiguration());
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
	 * @param m
	 *            ConfigManager to be used for initialization.
	 */
	public static void init(final ConfigManager m) {
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

	/**
	 * @return forced process launching mechanism
	 */
	public static String getProcessLaunchMethod() {
		if (getConfig().getb("forceFork", false))
			return "FORK";

		if (getConfig().getb("forceVFork", false))
			return "VFORK";

		if (getConfig().getb("forcePosixSpawn", false))
			return "POSIX_SPAWN";

		return null;
	}

	/**
	 * Explicitly configure JVM to use the FORK method of launching processes. WARNING: this is impacting a *lot* the performance. Only set it for leaf services that don't process much anyway.
	 */
	public static void switchToForkProcessLaunching() {
		final String method = getProcessLaunchMethod();

		if (method != null)
			System.setProperty("jdk.lang.Process.launchMechanism", method);
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

		System.setProperty("http.agent", appName);

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
		return otherConfigFiles.get(key.toLowerCase());
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

	private static String jAliEnVersion = null;

	/**
	 * @return JAlien version
	 */
	public static final String getVersion() {
		if (jAliEnVersion == null)
			jAliEnVersion = Version.getTag();

		return jAliEnVersion;
	}

	private static final String getBashOutput(final String command) {
		final CommandOutput co = SystemCommand.bash(command);

		if (co != null) {
			final String stdout = co.stdout;

			if (stdout != null)
				return stdout.trim();

			return null;
		}

		return null;
	}

	/**
	 * @return machine platform
	 */
	public static final String getPlatform() {
		final String unameS = getBashOutput("uname -s");

		final String unameM = getBashOutput("uname -m");

		if (unameS == null && unameM == null)
			return null;

		if (unameS != null) {
			if (unameM != null)
				return unameS + "-" + unameM;

			return unameS + "-Unknown";
		}

		return "Unknown-" + unameM;
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
		return getConfigFromLdap(Boolean.parseBoolean(getConfig().gets("verify_ldap", "true")));
	}

	private static String resolvedLocalHostname = null;

	/**
	 * Get the current machine's fqdn. Default is to take it from InetAddress.getLocalHost(), but can be overridden by setting
	 * the <i>hostname</i> configuration key (in config.properties, environment, or JVM runtime parameters)
	 *
	 * @return the local hostname
	 */
	public static String getLocalHostname() {
		if (resolvedLocalHostname != null)
			return resolvedLocalHostname;

		String hostName = getConfig().gets("hostname", null);

		if (hostName == null || hostName.length() == 0 || !hostName.contains(".")) {
			try {
				hostName = InetAddress.getLocalHost().getCanonicalHostName();
			}
			catch (final UnknownHostException e) {
				logger.log(Level.WARNING, "Error: couldn't get hostname", e);
			}
		}

		if (hostName == null || hostName.length() == 0 || !hostName.contains(".")) {
			final Set<String> hostNameCandidates = new HashSet<>();

			final Set<String> ipAddresses = new HashSet<>();

			try {
				final Enumeration<NetworkInterface> cards = NetworkInterface.getNetworkInterfaces();

				while (cards.hasMoreElements()) {
					final NetworkInterface nic = cards.nextElement();

					for (final InterfaceAddress iface : nic.getInterfaceAddresses()) {
						final InetAddress addr = iface.getAddress();

						if (!addr.isAnyLocalAddress() && !addr.isLinkLocalAddress() && !addr.isLoopbackAddress() && !addr.isMulticastAddress()) {
							ipAddresses.add(addr.getHostAddress());

							final String someHostName = addr.getCanonicalHostName();

							if (!addr.getHostAddress().equals(someHostName))
								hostNameCandidates.add(someHostName.toLowerCase());
						}
					}
				}
			}
			catch (final SocketException se) {
				logger.log(Level.WARNING, "Could not enumerate local network interfaces", se);
			}

			if (hostNameCandidates.size() > 1) {
				// try to connect outside and see if any of the local addresses is used
				final String externalAddress = getExternalVisibleAddress(true, null);

				if (hostNameCandidates.contains(externalAddress))
					// great, one matches exactly!
					hostName = externalAddress;
				else {
					// no good idea on which one to choose, pick one of them ...

					for (final String address : ipAddresses) {
						final String candidate = getExternalVisibleAddress(true, address);

						if (hostNameCandidates.contains(candidate)) {
							hostName = candidate;
							break;
						}
					}

					if (hostName == null || hostName.length() == 0 || !hostName.contains("."))
						hostName = hostNameCandidates.iterator().next();
				}
			}
			else if (hostNameCandidates.size() == 1)
				hostName = hostNameCandidates.iterator().next();
		}

		if (hostName == null || hostName.length() == 0)
			return null;

		hostName = hostName.replace("/.$/", "");
		hostName = hostName.replace("dyndns.cern.ch", "cern.ch");

		logger.log(Level.INFO, "Local hostname resolved as " + hostName);

		resolvedLocalHostname = hostName;

		return hostName;
	}

	/**
	 * Connect to an external service and see which address is visible
	 *
	 * @param hostname
	 *            <code>true</code> to return the FQDN (if known), or <code>false</code> to return the IP
	 * @param address optionally pass the address that should be resolved, default is to resolve the default IP address of where the client connects from
	 * @return the IP or FQDN (if known)
	 */
	public static String getExternalVisibleAddress(final boolean hostname, final String address) {
		try {
			final String content = Utils.download("http://alimonitor.cern.ch/services/ip.jsp" + (address != null ? "?address=" + Format.encode(address) : ""), null);

			final String key = hostname ? "FQDN:" : "IP:";

			final int idx = content.indexOf(key);

			if (idx >= 0) {
				int idxTo = content.indexOf('\n', idx);

				if (idxTo < 0)
					idxTo = content.length();

				final String fqdn = content.substring(idx + key.length(), idxTo).toLowerCase();

				if (fqdn.indexOf('.') >= 0)
					return fqdn;
			}
		}
		catch (@SuppressWarnings("unused") final IOException ioe) {
			// ignore
		}

		return null;
	}

	/**
	 * @param checkContent
	 * @return global config map
	 */
	public static HashMap<String, Object> getConfigFromLdap(final boolean checkContent) {
		// Get hostname and domain
		return getConfigFromLdap(checkContent, getLocalHostname());
	}

	/**
	 * @param checkContent
	 * @param hostName
	 * @return LDAP configuration, for a particular hostname
	 */
	public static HashMap<String, Object> getConfigFromLdap(final boolean checkContent, final String hostName) {
		final HashMap<String, Object> configuration = new HashMap<>();

		final HashMap<String, Object> voConfig = LDAPHelper.getVOConfig();
		if (voConfig == null || voConfig.size() == 0)
			return null;

		// Check if the hostname is used anywhere at a site
		final Set<String> siteDNsForHostname = LDAPHelper.checkLdapInformation("host=" + hostName, "ou=Sites,", "dn");

		if (checkContent && (siteDNsForHostname == null || siteDNsForHostname.size() == 0)) {
			logger.severe("Error: " + (siteDNsForHostname == null ? "null" : String.valueOf(siteDNsForHostname.size())) + " entries across all sites for the hostname: " + hostName);
			return null;
		}

		HashMap<String, Object> hostConfig = null;
		String site = null;

		// siteDNsForHostname might contain more than one site. Let's see if one of the them has the configuration
		while (siteDNsForHostname.iterator().hasNext()) {
			final String dn = siteDNsForHostname.iterator().next();

			try {
				site = dn.substring(dn.lastIndexOf('=') + 1);

				// Get the hostConfig from LDAP based on the site and hostname
				hostConfig = LDAPHelper.checkLdapTree("(&(host=" + hostName + "))", "ou=Config,ou=" + site + ",ou=Sites,", "host");

				if (hostConfig.size() != 0)
					break;
			}
			catch (@SuppressWarnings("unused") IndexOutOfBoundsException iobe) {
				// ignore
			}
		}

		if (hostConfig == null) {
			if (checkContent)
				return null;

			hostConfig = new HashMap<>();
		}

		if (checkContent && hostConfig.size() == 0) {
			logger.severe("Error: cannot find host configuration in LDAP for host: " + hostName);
			return null;
		}

		if (checkContent && !hostConfig.containsKey("host_ce")) {
			logger.severe("Error: cannot find ce configuration in hostConfig for host: " + hostName);
			return null;
		}

		if (hostConfig.containsKey("host_ce")) {
			final HashMap<String, Object> ceConfig = getCEConfigFromLdap(checkContent, site, hostConfig.get("host_ce").toString());

			final String partitions = getPartitions("ALICE::" + site + "::" + hostConfig.get("host_ce"));

			ceConfig.put("ce_partition", partitions);

			configuration.putAll(ceConfig);

			// TODO: check to which partitions it belongs
		}

		HashMap<String, Object> monaLisaConfig = null;
		String MLName = null;
		if (hostConfig.containsKey("host_monalisa")) {
			MLName = (String) hostConfig.get("host_monalisa");
			logger.log(Level.INFO, "MLName: " + MLName);
			monaLisaConfig = LDAPHelper.checkLdapTree("(&(name=" + MLName + "))", "ou=MonaLisa,ou=Services,ou=" + site + ",ou=Sites,", "monalisa");
			logger.log(Level.INFO, "MLConfig: " + monaLisaConfig);
			configuration.putAll(monaLisaConfig);
		}

		final HashMap<String, Object> siteConfig = getSiteConfigFromLdap(checkContent, site);
		if (siteConfig == null || siteConfig.size() == 0)
			return null;

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
		else if (configuration.containsKey("site_tmpdir"))
			folders_config.put("tmpdir", (String) configuration.get("site_tmpdir"));

		if (configuration.containsKey("host_cachedir"))
			folders_config.put("cachedir", (String) configuration.get("host_cachedir"));
		else if (configuration.containsKey("site_cachedir"))
			folders_config.put("cachedir", (String) configuration.get("site_cachedir"));

		if (configuration.containsKey("host_logdir"))
			folders_config.put("logdir", (String) configuration.get("host_logdir"));
		else if (configuration.containsKey("site_logdir"))
			folders_config.put("logdir", (String) configuration.get("site_logdir"));

		for (final Map.Entry<String, String> entry : folders_config.entrySet()) {
			final String folderpath = entry.getValue();
			try {
				final File folderf = new File(Functions.resolvePathWithEnv(folderpath));
				if (!folderf.exists()) {
					final boolean created = folderf.mkdirs();
					if (!created)
						logger.severe("Directory for " + entry.getKey() + "can't be created: " + folderpath);
				}
			}
			catch (final Exception e) {
				logger.severe("Exception on directory creation: " + e.toString());
			}
		}

		return configuration;
	}

	/**
	 * @param checkContent
	 * @param site
	 * @return // root site config based on site name
	 */
	public static HashMap<String, Object> getSiteConfigFromLdap(final boolean checkContent, final String site) {
		final HashMap<String, Object> siteConfig = LDAPHelper.checkLdapTree("(&(ou=" + site + ")(objectClass=AliEnSite))", "ou=Sites,", "site");

		if (checkContent && siteConfig.size() == 0) {
			logger.severe("Error: cannot find site root configuration in LDAP for site: " + site);
			return null;
		}
		return siteConfig;
	}

	/**
	 * @param checkContent
	 * @param site
	 * @param cename
	 * @return CE information based on the site and ce name for the host
	 */
	public static HashMap<String, Object> getCEConfigFromLdap(final boolean checkContent, final String site, final String cename) {
		final HashMap<String, Object> ceConfig = LDAPHelper.checkLdapTree("(&(name=" + cename + "))", "ou=CE,ou=Services,ou=" + site + ",ou=Sites,", "ce");

		if (checkContent && ceConfig.size() == 0) {
			logger.severe("Error: cannot find ce configuration in LDAP for CE: " + cename);
			return null;
		}
		return ceConfig;
	}

	/**
	 * 
	 * @param cename
	 * @return partitions for cename
	 */
	public static String getPartitions(final String cename) {
		final Set<String> partitions = LDAPHelper.checkLdapInformation("(&(CEname=" + cename + "))", "ou=Partitions,", "name");

		final StringBuilder sb = new StringBuilder(",");

		for (final String s : partitions)
			sb.append(s).append(",");

		if (sb.length() == 1)
			sb.append(",");

		return sb.toString();
	}

	/**
	 * Configuration debugging
	 *
	 * @param args
	 */
	public static void main(final String[] args) {
		System.out.println("Has direct db connection: " + hasDirectDBConnection);

		System.out.println("Local hostname resolved as: " + getLocalHostname());

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

	private static String closeSiteCacheValue = null;
	private static long closeSiteExpirationTimestamp = 0;

	/**
	 * Get the closest site mapped to current location of the client.
	 *
	 * @return the close site (or where the job runs), as pointed by the env variable <code>ALIEN_SITE</code>, or, if not defined, the configuration key <code>alice_close_site</code>
	 */
	public static synchronized String getCloseSite() {
		if (closeSiteExpirationTimestamp > System.currentTimeMillis() && closeSiteCacheValue != null)
			return closeSiteCacheValue;

		final String envSite = ConfigUtils.getConfig().gets("ALIEN_SITE");

		if (envSite.length() > 0) {
			closeSiteCacheValue = envSite;
			// environment variable is set by jobs, in this case there can be no reconfiguration during the execution (assumed to be 2 days)
			closeSiteExpirationTimestamp = System.currentTimeMillis() + 1000L * 60 * 60 * 48;
			return closeSiteCacheValue;
		}

		final String configKey = ConfigUtils.getConfig().gets("alice_close_site");

		if (configKey.length() > 0) {
			closeSiteCacheValue = configKey;
			// environment variable is set by jobs, in this case there can be no reconfiguration during the execution (assumed to be 2 days)
			closeSiteExpirationTimestamp = System.currentTimeMillis() + 1000L * 60 * 60 * 48;
			return closeSiteCacheValue;
		}

		if (closeSiteCacheValue != null) {
			closeSiteExpirationTimestamp = System.currentTimeMillis() + 1000L * 60;
			// extend the validity for one minute but refresh the value in background

			new Thread("AsyncSiteMapping") {
				@Override
				public void run() {
					closeSiteCacheValue = getSiteMappingFromAlimonitor();
					closeSiteExpirationTimestamp = System.currentTimeMillis() + 1000L * 60 * 30;
				}
			}.start();

			return closeSiteCacheValue;
		}

		closeSiteCacheValue = getSiteMappingFromAlimonitor();
		closeSiteExpirationTimestamp = System.currentTimeMillis() + 1000L * 60 * 30;

		return closeSiteCacheValue;
	}

	private static String getSiteMappingFromAlimonitor() {
		String ret = "CERN";

		try {
			final String closeSiteByML = Utils.download("http://alimonitor.cern.ch/services/getClosestSite.jsp", null);

			if (closeSiteByML != null && closeSiteByML.length() > 0) {
				int idx = closeSiteByML.indexOf('\n');

				if (idx < 0)
					idx = closeSiteByML.indexOf('\r');

				if (idx < 0)
					idx = closeSiteByML.indexOf(' ');

				if (idx > 0)
					ret = closeSiteByML.substring(0, idx);
				else
					ret = closeSiteByML;
			}
		}
		catch (final IOException ioe) {
			logger.log(Level.WARNING, "Cannot contact alimonitor to map you to the closest site", ioe);
		}

		return ret;
	}

	/**
	 * Write the configuration file and export variable that are used by JAliEn-ROOT and JSh
	 *
	 * @return the environment variables to export to the job (as actual env variables or config file)
	 */
	public static HashMap<String, String> exportJBoxVariables() {
		final HashMap<String, String> vars = new HashMap<>();
		final String sHost = "127.0.0.1";

		final AliEnPrincipal alUser = AuthorizationFactory.getDefaultUser();

		if (alUser != null && alUser.getName() != null) {
			vars.put("JALIEN_USER", alUser.getName());
			vars.put("JALIEN_HOME", UsersHelper.getHomeDir(alUser.getName()));
		}

		vars.put("JALIEN_HOST", sHost);

		if (JBoxServer.getPort() > 0) {
			vars.put("JALIEN_PORT", Integer.toString(JBoxServer.getPort()));
			vars.put("JALIEN_PASSWORD", JBoxServer.getPassword());
		}

		if (TomcatServer.getPort() > 0)
			vars.put("JALIEN_WSPORT", Integer.toString(TomcatServer.getPort()));

		vars.put("JALIEN_PID", Integer.toString(MonitorFactory.getSelfProcessID()));

		return vars;
	}

	/**
	 * Write the configuration file that is used by JAliEn-ROOT and JSh <br />
	 * the filename = <i>java.io.tmpdir</i>/jclient_token_$uid
	 *
	 * @param vars key-value map to dump to the file
	 *
	 * @return true if the file was written, false if not
	 */
	public static boolean writeJClientFile(final HashMap<String, String> vars) {
		try {
			final String sUserId = UserFactory.getUserID();

			if (sUserId == null) {
				logger.log(Level.SEVERE, "Cannot get the current user's ID");
				return false;
			}

			final File tmpDir = new File(System.getProperty("java.io.tmpdir"));

			final File tokenFile = new File(tmpDir, "jclient_token_" + sUserId);

			try (FileWriter fw = new FileWriter(tokenFile)) {
				vars.forEach((key, value) -> {
					try {
						fw.write(key + "=" + value + "\n");
					}
					catch (final IOException e) {
						logger.log(Level.SEVERE, "Could not open file " + tokenFile + " to write", e);
					}
				});

				tokenFile.deleteOnExit();

				fw.flush();
				fw.close();

				return true;
			}
		}
		catch (final Throwable e) {
			logger.log(Level.SEVERE, "Could not get user id! The token file could not be created ", e);
			return false;
		}
	}
}
