package alien.monitoring;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;
import apmon.ApMon;
import lazyj.Format;
import lia.Monitor.monitor.MCluster;
import lia.Monitor.monitor.MFarm;
import lia.Monitor.monitor.MNode;
import lia.Monitor.monitor.MonitoringModule;
import lia.Monitor.monitor.Result;
import lia.Monitor.monitor.eResult;
import lia.util.DynamicThreadPoll.SchJobInt;

/**
 * @author costing
 */
public class Monitor implements Runnable {

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(Monitor.class.getCanonicalName());

	private final String component;

	private final Collection<SchJobInt> modules;

	/**
	 * Scheduled task, so that it can be canceled later if needed
	 */
	ScheduledFuture<?> future = null;

	/**
	 * Collecting interval
	 */
	int interval = 0;

	private final ConcurrentHashMap<String, MonitoringObject> monitoringObjects = new ConcurrentHashMap<>();

	/**
	 * MonALISA Cluster name
	 */
	private final String clusterName;

	/**
	 * MonALISA Node name
	 */
	private final String nodeName;

	/**
	 * @param component
	 */
	Monitor(final String component) {
		this.component = component;

		this.modules = new HashSet<>();

		final String clusterPrefix = MonitorFactory.getConfigString(component, "cluster_prefix", "ALIEN");
		final String clusterSuffix = MonitorFactory.getConfigString(component, "cluster_suffix", "Nodes");

		String cluster = "";

		if (clusterPrefix != null && clusterPrefix.length() > 0)
			cluster = clusterPrefix + "_";

		cluster += component;

		if (clusterSuffix != null && clusterSuffix.length() > 0)
			cluster += "_" + clusterSuffix;

		clusterName = MonitorFactory.getConfigString(component, "cluster_name", cluster);

		final String pattern = MonitorFactory.getConfigString(component, "node_name", "${hostname}");

		nodeName = Format.replace(pattern, "${hostname}", MonitorFactory.getSelfHostname());
	}

	/**
	 * Get the ML cluster name
	 *
	 * @return cluster name
	 */
	String getClusterName() {
		return clusterName;
	}

	/**
	 * Get the ML node name
	 *
	 * @return node name
	 */
	String getNodeName() {
		return nodeName;
	}

	/**
	 * Add MonALISA monitoring module
	 *
	 * @param module
	 */
	void addModule(final SchJobInt module) {
		if (module != null) {
			if (module instanceof MonitoringModule) {
				final MonitoringModule job = (MonitoringModule) module;

				final MFarm mfarm = new MFarm(component);
				final MCluster mcluster = new MCluster(clusterName, mfarm);
				final MNode mnode = new MNode(nodeName, mcluster, mfarm);

				job.init(mnode, ConfigUtils.getConfig().gets(module.getClass().getCanonicalName() + ".args"));
			}

			modules.add(module);
		}
	}

	/**
	 * Add this extra monitoring object.
	 *
	 * @param key
	 * @param obj
	 */
	public void addMonitoring(final String key, final MonitoringObject obj) {
		monitoringObjects.put(key, obj);
	}

	/**
	 * @param key
	 * @return the monitoring object for this key
	 */
	public MonitoringObject get(final String key) {
		return monitoringObjects.get(key);
	}

	/**
	 * Increment an access counter
	 *
	 * @param counterKey
	 * @return the new absolute value of the counter
	 */
	public long incrementCounter(final String counterKey) {
		return incrementCounter(counterKey, 1);
	}

	/**
	 * Increment an access counter
	 *
	 * @param counterKey
	 * @param count
	 * @return the new absolute value of the counter
	 */
	public long incrementCounter(final String counterKey, final long count) {
		final MonitoringObject mo = monitoringObjects.get(counterKey);

		Counter c;

		if (mo == null) {
			c = new Counter(counterKey);

			final MonitoringObject old = monitoringObjects.putIfAbsent(counterKey, c);

			if (old != null && (old instanceof Counter))
				c = (Counter) old;
		}
		else
			if (mo instanceof Counter)
				c = (Counter) mo;
			else
				return -1;

		return c.increment(count);
	}

	/**
	 * Add a measurement value. This can be the time (recommended in seconds) that took a command to executed, a file size (in bytes) and so on.
	 *
	 * @param key
	 * @param quantity
	 *            how much to add to the previous value
	 * @return accumulated so far, or <code>-1</code> if there was any error
	 */
	public double addMeasurement(final String key, final double quantity) {
		final MonitoringObject mo = monitoringObjects.get(key);

		Measurement t;

		if (mo == null) {
			t = new Measurement(key);

			final MonitoringObject old = monitoringObjects.putIfAbsent(key, t);

			if (old != null && (old instanceof Measurement))
				t = (Measurement) old;
		}
		else
			if (mo instanceof Measurement)
				t = (Measurement) mo;
			else
				return -1;

		return t.addMeasurement(quantity);
	}

	/**
	 * Add a timing result, in milliseconds
	 * 
	 * @param key
	 * @param timing the duration of a measurement, converted to milliseconds
	 * @return accumulated so far, or <code>-1</code> if there was any error
	 */
	public double addMeasurement(final String key, final Timing timing) {
		return addMeasurement(key, timing.getMillis());
	}

	/**
	 * Get the CacheMonitor for this key.
	 *
	 * @param key
	 * @return the existing, or newly created, object, or <code>null</code> if a different type of object was already associated to this key
	 */
	public CacheMonitor getCacheMonitor(final String key) {
		final MonitoringObject mo = monitoringObjects.get(key);

		CacheMonitor cm;

		if (mo == null) {
			cm = new CacheMonitor(key);

			final MonitoringObject old = monitoringObjects.putIfAbsent(key, cm);

			if (old != null && (old instanceof CacheMonitor))
				cm = (CacheMonitor) old;
		}
		else
			if (mo instanceof CacheMonitor)
				cm = (CacheMonitor) mo;
			else
				return null;

		return cm;

	}

	/**
	 * Increment the hit count for the given key
	 *
	 * @param key
	 * @see #incrementCacheMisses(String)
	 * @see #getCacheMonitor(String)
	 */
	public void incrementCacheHits(final String key) {
		final CacheMonitor cm = getCacheMonitor(key);

		if (cm == null)
			return;

		cm.incrementHits();
	}

	/**
	 * Increment the misses count for the given key
	 *
	 * @param key
	 * @see #incrementCacheHits(String)
	 * @see #getCacheMonitor(String)
	 */
	public void incrementCacheMisses(final String key) {
		final CacheMonitor cm = getCacheMonitor(key);

		if (cm == null)
			return;

		cm.incrementMisses();
	}

	@Override
	protected void finalize() throws Throwable {
		run();
	}

	@Override
	public void run() {
		final ApMon apmon = MonitorFactory.getApMonSender();

		if (apmon == null)
			return;

		final List<Object> values = new ArrayList<>();

		for (final SchJobInt module : modules) {
			final Object o;

			try {
				o = module.doProcess();
			}
			catch (final Throwable t) {
				logger.log(Level.WARNING, "Exception running module " + module + " for component " + component, t);

				continue;
			}

			if (o == null)
				continue;

			if (o instanceof Collection<?>)
				values.addAll((Collection<?>) o);
			else
				values.add(o);
		}

		sendResults(values);

		if (monitoringObjects.size() > 0) {
			final Vector<String> paramNames = new Vector<>(monitoringObjects.size());
			final Vector<Object> paramValues = new Vector<>(monitoringObjects.size());

			for (final MonitoringObject mo : monitoringObjects.values())
				mo.fillValues(paramNames, paramValues);

			sendParameters(paramNames, paramValues);
		}
	}

	/**
	 * Send a bunch of results
	 *
	 * @param values
	 */
	public void sendResults(final Collection<Object> values) {
		if (values == null || values.size() == 0)
			return;

		final Vector<String> paramNames = new Vector<>();
		final Vector<Object> paramValues = new Vector<>();

		for (final Object o : values)
			if (o instanceof Result) {
				final Result r = (Result) o;

				if (r.param == null)
					continue;

				for (int i = 0; i < r.param.length; i++) {
					paramNames.add(r.param_name[i]);
					paramValues.add(Double.valueOf(r.param[i]));
				}
			}
			else
				if (o instanceof eResult) {
					final eResult er = (eResult) o;

					if (er.param == null)
						continue;

					for (int i = 0; i < er.param.length; i++) {
						paramNames.add(er.param_name[i]);
						paramValues.add(er.param[i].toString());
					}

				}

		sendParameters(paramNames, paramValues);
	}

	/**
	 * Send these parameters
	 *
	 * @param paramNames
	 *            the names
	 * @param paramValues
	 *            values associated to the names, Strings or Numbers
	 */
	public void sendParameters(final Vector<String> paramNames, final Vector<Object> paramValues) {
		if (paramNames == null || paramValues == null || (paramNames.size() == 0 && paramValues.size() == 0))
			return;

		if (paramValues.size() != paramNames.size()) {
			logger.log(Level.WARNING, "The names and the values arrays have different sizes (" + paramNames.size() + " vs " + paramValues.size() + ")");
			return;
		}

		final ApMon apmon = MonitorFactory.getApMonSender();

		if (apmon == null)
			return;

		if (logger.isLoggable(Level.FINEST))
			logger.log(Level.FINEST, "Sending on " + clusterName + " / " + nodeName + "\n" + paramNames + "\n" + paramValues);

		try {
			synchronized (apmon) {
				apmon.sendParameters(clusterName, nodeName, paramNames.size(), paramNames, paramValues);
			}
		}
		catch (final Throwable t) {
			logger.log(Level.SEVERE, "Cannot send ApMon datagram", t);
		}
	}

	/**
	 * Send only one parameter. This method of sending is less efficient than {@link #sendParameters(Vector, Vector)} and so it should only be used when there is exactly one parameter to be sent.
	 *
	 * @param parameterName
	 *            parameter name
	 * @param parameterValue
	 *            the value, should be either a String or a Number
	 * @see #sendParameters(Vector, Vector)
	 */
	public void sendParameter(final String parameterName, final Object parameterValue) {
		final Vector<String> paramNames = new Vector<>(1);
		paramNames.add(parameterName);

		final Vector<Object> paramValues = new Vector<>(1);
		paramValues.add(parameterValue);

		sendParameters(paramNames, paramValues);
	}

	@Override
	public String toString() {
		return clusterName + "/" + nodeName + " : " + modules;
	}
}
