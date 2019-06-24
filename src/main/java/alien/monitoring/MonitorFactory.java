package alien.monitoring;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;
import apmon.ApMon;
import apmon.ApMonException;
import lazyj.ExtProperties;
import lazyj.Utils;
import lia.Monitor.modules.DiskDF;
import lia.Monitor.modules.MemInfo;
import lia.Monitor.modules.Netstat;
import lia.Monitor.modules.SysInfo;
import lia.Monitor.modules.monIPAddresses;
import lia.Monitor.modules.monLMSensors;
import lia.Monitor.modules.monProcIO;
import lia.Monitor.modules.monProcLoad;
import lia.Monitor.modules.monProcStat;

/**
 * @author costing
 */
public final class MonitorFactory {

	/**
	 * For giving incremental thread IDs
	 */
	static final AtomicInteger aiFactoryIndex = new AtomicInteger(0);

	private static Monitor selfMonitor = null;

	private static Monitor systemMonitor = null;

	private static final Random random = new Random(System.currentTimeMillis());

	private static final ThreadFactory threadFactory = new ThreadFactory() {

		@Override
		public Thread newThread(final Runnable r) {
			final Thread t = new Thread(r);

			t.setName("alien.monitor.MonitorFactory - " + aiFactoryIndex.incrementAndGet());
			t.setDaemon(true);

			return t;
		}
	};

	private static final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1, threadFactory);

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(MonitorFactory.class.getCanonicalName());

	private static ApMon apmonInstance = null;

	private static final Map<String, Monitor> monitors = new HashMap<>();

	private static int selfProcessID = 0;

	static {
		final Thread t = new Thread() {
			@Override
			public void run() {
				if (getConfigBoolean("System", "enabled", true))
					enableSystemMonitoring();

				if (getConfigBoolean("Self", "enabled", true))
					enableSelfMonitoring();
			}
		};

		t.setDaemon(true);
		t.start();
	}

	private MonitorFactory() {
		// disable this constructor, only static methods
	}

	/**
	 * Get the monitor for this component
	 *
	 * @param component
	 * @return the monitor
	 */
	public static Monitor getMonitor(final String component) {
		Monitor m;

		synchronized (monitors) {
			m = monitors.get(component);

			if (m == null && getConfigBoolean(component, "enabled", true)) {
				m = new Monitor(component);

				final int interval = getConfigInt(component, "period", isJob() ? 120 : 60);

				final ScheduledFuture<?> future = executor.scheduleAtFixedRate(m, random.nextInt(interval), interval, TimeUnit.SECONDS);

				m.future = future;
				m.interval = interval;

				monitors.put(component, m);
			}
		}

		return m;
	}

	/**
	 * Enable periodic sending of background host monitoring
	 */
	public static synchronized void enableSystemMonitoring() {
		if (systemMonitor != null)
			return;

		final String component = "System";

		systemMonitor = getMonitor(component);

		try {
			if (getConfigBoolean(component, "monProcIO", true))
				systemMonitor.addModule(new monProcIO());
		}
		catch (final Exception e) {
			logger.log(Level.WARNING, "Cannot instantiate monProcIO", e);
		}

		try {
			if (getConfigBoolean(component, "monProcStat", true))
				systemMonitor.addModule(new monProcStat());
		}
		catch (final Exception e) {
			logger.log(Level.WARNING, "Cannot instantiate monProcStat", e);
		}

		try {
			if (getConfigBoolean(component, "monProcLoad", true))
				systemMonitor.addModule(new monProcLoad());
		}
		catch (final Exception e) {
			logger.log(Level.WARNING, "Cannot instantiate monProcLoad", e);
		}

		try {
			if (getConfigBoolean(component, "monIPAddresses", true))
				systemMonitor.addModule(new monIPAddresses());
		}
		catch (final Exception e) {
			logger.log(Level.WARNING, "Cannot instantiate monIPAddresses", e);
		}

		try {
			if (getConfigBoolean(component, "monLMSensors", false))
				systemMonitor.addModule(new monLMSensors());
		}
		catch (final Exception e) {
			logger.log(Level.WARNING, "Cannot instantiate monLMSensors", e);
		}

		try {
			if (getConfigBoolean(component, "DiskDF", isJob() ? false : true))
				systemMonitor.addModule(new DiskDF());
		}
		catch (final Exception e) {
			logger.log(Level.WARNING, "Cannot instantiate DiskDF", e);
		}

		try {
			if (getConfigBoolean(component, "MemInfo", true))
				systemMonitor.addModule(new MemInfo());
		}
		catch (final Exception e) {
			logger.log(Level.WARNING, "Cannot instantiate MemInfo", e);
		}

		try {
			if (getConfigBoolean(component, "Netstat", true))
				systemMonitor.addModule(new Netstat());
		}
		catch (final Exception e) {
			logger.log(Level.WARNING, "Cannot instantiate Netstat", e);
		}

		try {
			if (getConfigBoolean(component, "SysInfo", true))
				systemMonitor.addModule(new SysInfo());
		}
		catch (final Exception e) {
			logger.log(Level.WARNING, "Cannot instantiate SysInfo", e);
		}
	}

	/**
	 * Enable periodic sending of internal parameters
	 */
	public static synchronized void enableSelfMonitoring() {
		if (selfMonitor != null)
			return;

		selfMonitor = getMonitor("Self");

		if (selfMonitor == null)
			return;

		selfMonitor.addMonitoring("self", new SelfMonitor());

		final ApMon apmon = getApMonSender();

		if (apmon != null) {
			final int pid = getSelfProcessID();

			if (pid > 0) {
				logger.log(Level.FINE, "Enabling background self monitoring of PID " + pid + " every " + selfMonitor.interval + " seconds");

				apmon.setJobMonitoring(true, selfMonitor.interval);
				apmon.addJobToMonitor(pid, System.getProperty("user.dir"), selfMonitor.getClusterName(), selfMonitor.getNodeName());
			}
			else
				logger.log(Level.WARNING, "Could not determine self pid so external process tracking is disabled");
		}
		else
			logger.log(Level.WARNING, "ApMon is null, so self monitoring cannot run");
	}

	private static boolean isJob() {
		final String test = ConfigUtils.getConfig().gets("APMON_CONFIG", null);

		return test != null && test.length() > 0;
	}

	private static Vector<String> getApMonDestinations() {
		if (isJob()) {
			final StringTokenizer st = new StringTokenizer(ConfigUtils.getConfig().gets("APMON_CONFIG", null), ","); //$NON-NLS-1$

			final Vector<String> vReturn = new Vector<>(st.countTokens());

			while (st.hasMoreTokens())
				vReturn.add(st.nextToken());

			return vReturn;
		}

		final ExtProperties p = getConfig();

		if (p == null) {
			final Vector<String> v = new Vector<>(1);
			v.add("localhost");
			return v;
		}

		return p.toVector("destinations");
	}

	/**
	 * @return monitoring configuration
	 */
	static ExtProperties getConfig() {
		return ConfigUtils.getConfiguration("monitoring");
	}

	/**
	 * @param component
	 * @param key
	 * @param defaultValue
	 * @return the boolean value for this key
	 */
	static boolean getConfigBoolean(final String component, final String key, final boolean defaultValue) {
		final String sValue = getConfigString(component, key, null);

		return Utils.stringToBool(sValue, defaultValue);
	}

	/**
	 * @param component
	 * @param key
	 * @param defaultValue
	 * @return the double value for this key
	 */
	static double getConfigDouble(final String component, final String key, final double defaultValue) {
		final String sValue = getConfigString(component, key, null);

		if (sValue == null)
			return defaultValue;

		try {
			return Double.parseDouble(sValue);
		}
		catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
			return defaultValue;
		}
	}

	/**
	 * @param component
	 * @param key
	 * @param defaultValue
	 * @return the integer value for this key
	 */
	static int getConfigInt(final String component, final String key, final int defaultValue) {
		final String sValue = getConfigString(component, key, null);

		if (sValue == null)
			return defaultValue;

		try {
			return Integer.parseInt(sValue);
		}
		catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
			return defaultValue;
		}
	}

	/**
	 * @param component
	 * @param key
	 * @param defaultValue
	 * @return the string value for this key
	 */
	static String getConfigString(final String component, final String key, final String defaultValue) {
		final ExtProperties prop = MonitorFactory.getConfig();

		if (prop == null)
			return defaultValue;

		String comp = component;

		while (comp != null) {
			final String sValue = prop.gets(comp + ".monitor." + key, null);

			if (sValue != null)
				return sValue;

			if (comp.length() == 0)
				break;

			final int idx = comp.lastIndexOf('.');

			if (idx >= 0)
				comp = comp.substring(0, idx);
			else
				comp = "";
		}

		return defaultValue;
	}

	/**
	 * Get the ApMon sender
	 *
	 * @return the sender
	 */
	public static synchronized ApMon getApMonSender() {
		if (apmonInstance != null)
			return apmonInstance;

		final Vector<String> destinations = getApMonDestinations();

		logger.log(Level.FINE, "ApMon destinations : " + destinations);

		try {
			apmonInstance = new ApMon(destinations);
		}
		catch (final IOException ioe) {
			logger.log(Level.SEVERE, "Cannot instantiate ApMon because IOException ", ioe);
		}
		catch (final ApMonException e) {
			logger.log(Level.SEVERE, "Cannot instantiate ApMon because ApMonException ", e);
		}

		return apmonInstance;
	}

	private static final String PROC_SELF = "/proc/self";

	/**
	 * Get JVM's process ID
	 *
	 * @return the process id, if it can be determined, or <code>-1</code> if not
	 */
	public static final int getSelfProcessID() {
		if (selfProcessID != 0)
			return selfProcessID;

		try {
			// on Linux
			selfProcessID = Integer.parseInt((new File(PROC_SELF)).getCanonicalFile().getName());

			return selfProcessID;
		}
		catch (@SuppressWarnings("unused") final Throwable t) {
			// ignore
		}

		try {
			selfProcessID = Integer.parseInt(System.getProperty("pid"));

			return selfProcessID;
		}
		catch (@SuppressWarnings("unused") final Throwable t) {
			// ignore
		}

		try {
			final String s = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();

			selfProcessID = Integer.parseInt(s.substring(0, s.indexOf('@')));
		}
		catch (@SuppressWarnings("unused") final Throwable t) {
			// ignore
		}

		// selfProcessID = -1;

		return selfProcessID;
	}

	private static String thisHostname = null;

	/**
	 * @return this machine's FQDN
	 */
	public static String getSelfHostname() {
		if (thisHostname == null)
			try {
				thisHostname = InetAddress.getLocalHost().getCanonicalHostName();
			}
			catch (@SuppressWarnings("unused") final UnknownHostException uhe) {
				thisHostname = "localhost";
			}

		return thisHostname;
	}
}
