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
import java.nio.file.FileSystem;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.Properties;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.logging.FileHandler;
import java.util.logging.Filter;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import alien.log.DefaultLoggingFilter;
import alien.user.LDAPHelper;
import lazyj.DBFunctions;
import lazyj.ExtProperties;
import lazyj.cache.ExpirationCache;
import lazyj.commands.SystemCommand;
import lia.Monitor.monitor.AppConfig;
import sun.util.resources.cldr.ts.CurrencyNames_ts;

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
	
	static transient Logger defaultLogger = null; 
	static transient DefaultLoggingFilter defaultLoggerFilter;

	private static final Map<String, ExtProperties> dbConfigFiles;

	private static final Map<String, ExtProperties> otherConfigFiles;

	private static final String CONFIG_FOLDER;

	private static HashMap<String, FileHandler> serviceHandlersLookup;
	
	private static LoggingConfigurator logging = null;
	
	private static SimpleFormatter formatter;

	private static final ExtProperties appConfig;

	private static boolean hasDirectDBConnection = false;

	static {
		String sConfigFolder = null;

		final HashMap<String, ExtProperties> dbconfig = new HashMap<>();

		final HashMap<String, ExtProperties> otherconfig = new HashMap<>();
		
		serviceHandlersLookup = new HashMap<>();

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
		} catch (@SuppressWarnings("unused") final Throwable t) {
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
			formatter = new SimpleFormatter() {
				
				@Override
				public String format(LogRecord record) {
					Date date = new Date(record.getMillis());
					DateFormat formatter = new SimpleDateFormat("HH:mm:ss:SSS");
					String dateFormatted = formatter.format(date);
					return String.join(" ", dateFormatted, record.getLevel().getLocalizedName(), record.getMessage(), "\n");
				}
			};

			// tell ML not to configure its logger
			System.setProperty("lia.Monitor.monitor.LoggerConfigClass.preconfiguredLogging", "true");

			// same to lazyj
			System.setProperty("lazyj.use_java_logger", "true");
		}

		if (!hasMLConfig)
			// write a copy of our main configuration content and, if any, a separate ML configuration file to ML's configuration registry
			for (final String configFile : new String[] { "config", "mlconfig", "App" }) {
				final ExtProperties eprop = foundProperties.get(configFile);

				if (eprop != null) {
					final Properties prop = eprop.getProperties();

					for (final String key : prop.stringPropertyNames())
						AppConfig.setProperty(key, prop.getProperty(key));
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
		
		public ExtProperties getProps() {
			return prop;
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
		
		// if we are responsible for the logging in this context, attach all
		// required handlers and filters to the logger
		
		if(logging != null) {
			
			// prepare default logger
			if(defaultLogger == null) {
				defaultLogger = Logger.getLogger("DefaultLogger");
				prepareDefaultHandler();
			}
			
			// check if serviceLoggers have been prepared, if not so: prepare them.
			if(serviceHandlersLookup.isEmpty()) {
				prepareServiceHandlers();
			}
			for(String serviceKey: serviceHandlersLookup.keySet()) {
				l.addHandler(serviceHandlersLookup.get(serviceKey));
			}

			final String s = seenLoggers.get(component);
			if (s == null)
				seenLoggers.put(component, component, 60 * 1000);
		}
		
		if (l.getFilter() == null)
			l.setFilter(LoggingFilter.getInstance());
		

		return l;
	}
	
	/**
	 * Create the described service loggers, based on logging.properties file.
	 * 
	 */
	private static void prepareServiceHandlers() {
		// get the comma seperated list of loggers
		String serviceNames = logging.getProps().gets("serviceLoggers");
		String[] splittedServiceNames = serviceNames.split(",");
		
		for(String currServiceLoggerName: splittedServiceNames) {
			// get rid of whitespaces in between service names
			currServiceLoggerName = currServiceLoggerName.trim();
			
			try {
				
				// create the output file, based on logger.pattern. If no .pattern is provided
				// use the default alien-logname.log instead
				String outputFileName;
				if(logging.prop.getProperties().keySet().contains(currServiceLoggerName + ".pattern") &&
						logging.prop.gets(currServiceLoggerName + ".pattern").length() > 0) {
					outputFileName = logging.prop.gets(currServiceLoggerName + ".pattern");
				} else {
					// create a logfile with default name then
					outputFileName = currServiceLoggerName + ".log";
				}
				
				File logFile = getLogFile(outputFileName);
				FileHandler serviceFh = new FileHandler(outputFileName, true);
				
				// check if there are filter settings for this service
				if(logging.prop.getProperties().keySet().contains(currServiceLoggerName + ".logrules")) {

					String logrule = logging.prop.gets(currServiceLoggerName + ".logrules");
					
					// split and trim the logrules, also transform them to a collection for faster
					// comparision with logging context within the filter.
					List<String> trimmedLogRules = Arrays.stream(logrule.split(","))
																.map(String::trim)
																.collect(Collectors.toList());
					
					// attach a filter based on the required context properties, retreived from logging.properties
					Filter serviceFilter = new Filter() {
						
						private final DefaultLoggingFilter defaultLoggerFilterRef = ConfigUtils.defaultLoggerFilter;
						{
							// register ourselves
							defaultLoggerFilterRef.registerService();
						}
						
						// trimmedLogRules will be out of scope in the next iteration. To preserve it, we need to 
						// make it final or effectively final, so the keyword is not optional here
						private final List<String> logConditions = trimmedLogRules;
						
						@Override
						public boolean isLoggable(LogRecord record) {
							// check the context 
							String currLogContext = (String) Context.getLoggingContext();
							if(currLogContext != null && currLogContext.length() > 0) {
								// loggingContext is comma seperated, so split it up
								String[] currLogContextItems = currLogContext.split(",");
								if(Collections.indexOfSubList(Arrays.asList(currLogContextItems), logConditions) > -1) {
									// all logrules were part of the current context!
									defaultLoggerFilterRef.notifyRecordConsumed(record);
									return true;
								} else {
									// one or more of the required logrules were missing in the context
									defaultLoggerFilterRef.notifyRecordRejected(record);
									return false;
								}
								
							}
							
							// the thread context could not be found or was empty, do not log this record.
							defaultLoggerFilterRef.notifyRecordRejected(record);
							return false;
						}
					};
					
					// try to parse the provided log level from logging.properties. Otherwise use Level=Finest as default
					try {
						logging.prop.reload();
						serviceFh.setLevel(
								Level.parse(logging.getProps().gets(currServiceLoggerName + ".level")));
					} catch(Exception e) {
						System.err.println("No logging level for " + currServiceLoggerName + " provided. Falling back to 'Finest'.");
						serviceFh.setLevel(Level.FINEST);
					}
					
					serviceFh.setFormatter(formatter);
					serviceFh.setFilter(serviceFilter);
					// make sure to register at the default logger to not lose any records
					
				} else {
				}
				
				
				serviceHandlersLookup.put(currServiceLoggerName, serviceFh);
			} catch (SecurityException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
	}
	
	/**
	 * This special handler will make sure that no messages are lost. In case the threadContext does not meet the requirements from 
	 * other loggers, they will notify the default logger, which preserves the log records until all other loggers have reported
	 * back to him. If any of the service loggers decides to log the record, the record will be removed from the default logger.
	 * 
	 * TODO: add a property in logging.properties to change the pattern of default logging.
	 *  
	 */
	private static void prepareDefaultHandler() {
		FileHandler defaultFileHandler = null;
		
		// prepare the logger by removing any handlers
		if(defaultLogger == null) {
			System.err.println("Could not prepare the default logger!");
			System.err.println("We are supposed to use contextual logging, but have problems creating the default logger instance.");

			// what now? System exit? 
	}
		
		try {
			// remove all, but the default file handler from the logger
			for(java.util.logging.Handler h: defaultLogger.getHandlers()) {
				defaultLogger.removeHandler(h);
			}
			
			defaultFileHandler = new FileHandler("jalien_default.log", true);
			defaultLoggerFilter = new DefaultLoggingFilter(defaultLogger);
			defaultFileHandler.setFormatter(formatter);
			defaultLogger.addHandler(defaultFileHandler);
			defaultLogger.setLevel(Level.ALL);
			defaultFileHandler.setFilter(defaultLoggerFilter);
			
		} catch (SecurityException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/**
	 * 
	 * Used by <code>prepareServiceHandlers</code> during pattern parsing/creating default log file.
	 * This also checks/creates the folder structure provided.
	 * The file path needs to be seperated by whatever value is present in file.seperator (by default
	 * this is the same as <code>FileSystem.getDefault().getSeperator()</code>, but can be overwritten)
	 * 
	 * @param path - the relative file path
	 */
	private static File getLogFile(String path) throws IOException{
		final String fileSeperator = System.getProperty("file.seperator") != null ? System.getProperty("file.seperator")  
				: java.nio.file.FileSystems.getDefault().getSeparator();
		
		// seperate the filename from folder structure
		List<String> pathFragments = new LinkedList<String>(Arrays.asList(path.split(fileSeperator)));
		
		// reduce the list to only contain the folder structure while preserving the file name
		 String fileName = pathFragments.remove(pathFragments.size() - 1);
		 
		 File directory;
		 if(pathFragments.size() > 0) {
			// join the path fragments
			 directory = new File(
					 	pathFragments.stream()
					 				 .collect(Collectors.joining(fileSeperator)));
			 if(! directory.exists()) {
				 // create the directory including subfolders if it doesn't exist
				 directory.mkdirs();
			 }
		 } else {
			 // use the current directory
			 directory = new File(".");
		 }
		  
		 // finally create the file itself (might throw IOException)
	    File file = new File(directory.getName() + fileSeperator + fileName);    
        return file;
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
