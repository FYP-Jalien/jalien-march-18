/**
 *
 */
package alien.servlets;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import lazyj.Format;
import lazyj.LRUMap;
import lazyj.RequestWrapper;
import lazyj.Utils;
import lazyj.cache.ExpirationCache;
import lia.util.ShutdownManager;
import lia.util.StringFactory;

/**
 * @author costing
 * @since Apr 28, 2011
 */
public class TextCache extends HttpServlet {
	private static final long serialVersionUID = 6024682549531639348L;

	private static final ExpirationCache<String, Integer> defaultNamespaceExpiration = new ExpirationCache<>(1024);

	private static int getDefaultExpiration(final String namespace) {
		final Integer i = defaultNamespaceExpiration.get(namespace);

		if (i != null)
			return i.intValue();

		int nsDefault = 60 * 60;

		try {
			try {
				nsDefault = Integer.parseInt(System.getProperty("alien.servlets.TextCache.ttl_" + namespace));
			}
			catch (@SuppressWarnings("unused") final Throwable t1) {
				nsDefault = Integer.parseInt(System.getProperty("alien.servlets.TextCache.ttl"));
			}
		}
		catch (@SuppressWarnings("unused") final Throwable t) {
			// ignore
		}

		defaultNamespaceExpiration.put(namespace, Integer.valueOf(nsDefault), 60 * 5 * 1000);

		return nsDefault;
	}

	private static final class CacheValue implements Serializable {
		private static final long serialVersionUID = 1L;

		public final String value;

		public final long expires;

		public final AtomicInteger accesses = new AtomicInteger(1);

		public CacheValue(final String value, final long expires) {
			this.value = value;
			this.expires = expires;
		}
	}

	private static final class EntryComparator implements Comparator<Map.Entry<String, CacheValue>>, Serializable {
		private static final long serialVersionUID = -6092398826822045152L;

		public EntryComparator() {
			// nothing
		}

		@Override
		public int compare(final Entry<String, CacheValue> o1, final Entry<String, CacheValue> o2) {
			final int diff = o2.getValue().accesses.intValue() - o1.getValue().accesses.intValue();

			if (diff != 0)
				return diff;

			return o2.getKey().compareTo(o1.getKey());
		}

	}

	private static final EntryComparator entryComparator = new EntryComparator();

	private static final class CleanupThread extends Thread {
		public CleanupThread() {
			setName("alien.servlets.ThreadCache.CleanupThread");
			setDaemon(true);
		}

		@Override
		public void run() {
			while (true)
				try {
					Thread.sleep(1000 * 30);

					final long now = System.currentTimeMillis();

					final Vector<String> parameters = new Vector<>();
					final Vector<Object> values = new Vector<>();

					for (final Map.Entry<String, Namespace> nsEntry : namespaces.entrySet()) {
						final String nsName = nsEntry.getKey();

						final Namespace namespace = nsEntry.getValue();

						long soonestToExpire = 0;
						long latestToExpire = now;

						synchronized (namespace) {
							final Iterator<Map.Entry<String, CacheValue>> it = namespace.cache.entrySet().iterator();

							while (it.hasNext()) {
								final Map.Entry<String, CacheValue> entry = it.next();

								final long expires = entry.getValue().expires;

								if (expires < now) {
									notifyEntryRemoved(namespace, entry.getKey(), entry.getValue(), true, "EXPIRED_");

									it.remove();
								}
								else {
									if (soonestToExpire == 0 || expires < soonestToExpire)
										soonestToExpire = expires;

									if (expires > latestToExpire)
										latestToExpire = expires;
								}
							}

							parameters.add(nsName + "_size");
							values.add(Integer.valueOf(namespace.cache.size()));
						}

						if (soonestToExpire > 0) {
							parameters.add(nsName + "_hours");
							values.add(Double.valueOf((latestToExpire - soonestToExpire) / (3600000d)));
						}
					}

					if (monitor != null)
						monitor.sendParameters(parameters, values);
				}
				catch (@SuppressWarnings("unused") final Throwable t) {
					// ignore
				}
		}
	}

	/**
	 * Close opened logger stream, if needed
	 */
	static synchronized final void closeStreams() {
		if (requestLogger != null) {
			requestLogger.flush();
			requestLogger.close();
			requestLogger = null;
		}
	}

	/**
	 * Goes through the entries and removes the expired ones
	 */
	static final CleanupThread cleanup;

	/**
	 * Big cache structure
	 */
	final static Map<String, Namespace> namespaces = new ConcurrentHashMap<>();

	static {
		cleanup = new CleanupThread();
		cleanup.start();

		ShutdownManager.getInstance().addModule(() -> {
			for (final Map.Entry<String, Namespace> entry : namespaces.entrySet()) {
				final Namespace namespace = entry.getValue();

				synchronized (namespace) {
					for (final Map.Entry<String, CacheValue> entryToDelete : namespace.cache.entrySet())
						notifyEntryRemoved(namespace, entryToDelete.getKey(), entryToDelete.getValue(), false, "SHUTDOWN_");
				}
			}

			closeStreams();
		});
	}

	/**
	 * Monitoring component
	 */
	static transient final Monitor monitor = MonitorFactory.getMonitor(TextCache.class.getCanonicalName());

	private static PrintWriter requestLogger = null;

	private static int logCounter = 0;

	/**
	 * Last log file name
	 */
	static String lastLogFile = null;

	private static class MyGZIPOutputStream extends GZIPOutputStream {
		// The FileOutputStream object is closed by close() on this object
		@SuppressWarnings("resource")
		public MyGZIPOutputStream(final String filename) throws IOException {
			super(new FileOutputStream(filename));
			lastLogFile = filename;
			def.setLevel(Deflater.BEST_COMPRESSION);
		}
	}

	/**
	 * Call this one entry is removed to log the number of hits
	 *
	 * @param namespace
	 * @param key
	 * @param value
	 * @param removeFromKeysSet
	 */
	static synchronized void notifyEntryRemoved(final Namespace namespace, final String key, final CacheValue value, final boolean removeFromKeysSet, final String monitorPrefix) {
		if (removeFromKeysSet)
			namespace.keys.remove(key);

		if (monitor != null)
			monitor.incrementCounter(monitorPrefix + namespace.name);

		if (requestLogger == null)
			try {
				requestLogger = new PrintWriter(new OutputStreamWriter(new MyGZIPOutputStream("cache.log-" + System.currentTimeMillis() + ".gz")));
			}
			catch (final IOException e) {
				System.err.println("Could not write to cache.log: " + e.getMessage());
				return;
			}

		requestLogger.println(System.currentTimeMillis() + " " + value.accesses + " " + namespace + " " + key);

		if (requestLogger.checkError())
			requestLogger = null;
		else if (++logCounter > 1000) {
			logCounter = 0;

			if (lastLogFile != null) {
				final File f = new File(lastLogFile);

				if (!f.exists())
					closeStreams();
			}
		}
	}

	/**
	 * @author costing
	 *
	 */
	public static final class NotifyLRUMap extends LRUMap<String, CacheValue> {
		private static final long serialVersionUID = -9117776082771411054L;

		private final Namespace namespace;

		/**
		 * @param iCacheSize
		 * @param namespace
		 */
		public NotifyLRUMap(final int iCacheSize, final Namespace namespace) {
			super(iCacheSize);

			this.namespace = namespace;
		}

		@Override
		protected boolean removeEldestEntry(final java.util.Map.Entry<String, CacheValue> eldest) {
			final boolean ret = super.removeEldestEntry(eldest);

			if (ret)
				notifyEntryRemoved(namespace, eldest.getKey(), eldest.getValue(), true, "EVICTED_");

			return ret;
		}

	}

	private static final class Namespace implements Serializable {
		private static final long serialVersionUID = 1L;

		public final Map<String, CacheValue> cache;
		public final Set<String> keys;
		public final String name;

		public Namespace(final String name) {
			int size;

			this.name = name;

			try {
				try {
					size = Integer.parseInt(System.getProperty("alien.servlets.TextCache.size_" + name));
				}
				catch (@SuppressWarnings("unused") final Throwable t1) {
					size = Integer.parseInt(System.getProperty("alien.servlets.TextCache.size"));
				}
			}
			catch (@SuppressWarnings("unused") final Throwable t) {
				size = 50000;
			}

			cache = new NotifyLRUMap(size, this);
			keys = new ConcurrentSkipListSet<>();
		}

		@Override
		public String toString() {
			return name;
		}
	}

	private static final Namespace getNamespace(final String name) {
		Namespace ret = namespaces.get(name);

		if (ret != null)
			return ret;

		ret = new Namespace(name);

		namespaces.put(name, ret);

		return ret;
	}

	private static long slowQueryThreshold = 0;

	private static long slowQueryThresholdCheck = 0;

	private static synchronized long getSlowQueryThreshold() {
		if (System.currentTimeMillis() - slowQueryThresholdCheck > 1000 * 60) {
			slowQueryThreshold = ConfigUtils.getConfig().getl("alien.servlets.TextCache.logSlowQueries", 0);

			slowQueryThresholdCheck = System.currentTimeMillis();
		}

		return slowQueryThreshold;
	}

	@Override
	protected void doGet(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
		try (Timing timing = new Timing(monitor, "ms_to_answer")) {
			try (PrintWriter pwOut = response.getWriter()) {
				execRealGet(new RequestWrapper(request), request, response, pwOut);
			}

			timing.endTiming();

			final double duration = timing.getMillis();

			final long logSlowQueries = getSlowQueryThreshold();

			if (logSlowQueries > 0 && duration > logSlowQueries)
				System.err.println("Slow query : " + Format.point(duration) + "ms : " + request.getRemoteAddr() + " : " + request.getQueryString());

			logRequest(request);
		}
	}

	private static final void execRealGet(final RequestWrapper rw, final HttpServletRequest request, final HttpServletResponse response, final PrintWriter pwOut) {
		final String ns = rw.gets("ns", "default");

		final String key = rw.gets("key");

		response.setContentType("text/plain");

		if (key.length() == 0) {
			if (rw.gets("clean", null) != null) {
				for (final Map.Entry<String, Namespace> entry : namespaces.entrySet()) {
					final Namespace namespace = entry.getValue();

					if ("default".equals(ns) || namespace.name.equals(ns))
						synchronized (namespace) {
							for (final Map.Entry<String, CacheValue> entryToDelete : namespace.cache.entrySet())
								notifyEntryRemoved(namespace, entryToDelete.getKey(), entryToDelete.getValue(), false, "CLEAN_");

							namespace.cache.clear();
							namespace.keys.clear();
						}
				}

				System.err.println("Clean request came from " + request.getRemoteAddr() + " / " + request.getHeader("User-Agent") + " for ns " + ns);
			}
			else if (rw.gets("ns").length() == 0) {
				for (final Map.Entry<String, Namespace> entry : namespaces.entrySet()) {
					final Namespace namespace = entry.getValue();

					int min = -1;
					int max = 0;
					long total = 0;

					long hits = 0;

					int nssize;

					synchronized (namespace) {
						for (final CacheValue c : namespace.cache.values()) {
							final int size = c.value.length();

							min = (min < 0 || size < min) ? size : min;
							max = Math.max(max, size);
							total += size;

							hits += c.accesses.intValue();
						}

						nssize = namespace.cache.size();
					}

					if (min < 0)
						pwOut.println(entry.getKey() + " : empty");
					else
						pwOut.println(entry.getKey() + " : " + nssize + " / " + namespace.keys.size() + " keys (min: " + min + ", avg: " + Format.point((double) total / nssize) + ", max: " + max
								+ ", total: " + Format.size(total) + ") : " + hits + " hits");
				}

				final Runtime r = Runtime.getRuntime();

				pwOut.println("\nJava memory stats: " + Format.size(r.totalMemory()) + " total memory, " + Format.size(r.maxMemory()) + " max memory, " + Format.size(r.freeMemory()) + " free");
				pwOut.println("Java version: " + System.getProperty("java.version"));
				pwOut.println("Uptime: " + Format.toInterval(ManagementFactory.getRuntimeMXBean().getUptime()));
			}
			else {
				final Namespace namespace = namespaces.get(ns);

				if (namespace == null)
					pwOut.println("No such namespace: " + ns);
				else {
					int min = -1;
					int max = 0;
					long total = 0;
					int hits = 0;

					final boolean values = rw.gets("values").length() > 0;

					final ArrayList<Map.Entry<String, CacheValue>> entries;

					synchronized (namespace) {
						entries = new ArrayList<>(namespace.cache.entrySet());
					}

					Collections.sort(entries, entryComparator);

					for (final Map.Entry<String, CacheValue> me : entries) {
						final CacheValue cv = me.getValue();

						final int size = cv.value.length();

						min = (min < 0 || size < min) ? size : min;
						max = Math.max(max, size);
						total += size;

						hits += cv.accesses.intValue();

						pwOut.println(me.getKey() + " : size " + size + ", " + cv.accesses + " hits" + (values ? " : " + cv.value : ""));
					}

					final int nssize = namespace.cache.size();

					pwOut.print("\n\n----------------\n\n" + nssize + " entries");

					if (nssize > 0)
						pwOut.println("(min: " + min + ", avg: " + Format.point((double) total / nssize) + ", max: " + max + ", total: " + Format.size(total) + ") : " + hits + " hits");
					else
						pwOut.println();
				}
			}

			return;
		}

		final Namespace namespace = getNamespace(ns);

		String value = rw.gets("value", null);

		if (value != null) {
			// a SET operation

			CacheValue old;

			if (rw.getb("ifnull", false) == true) {
				synchronized (namespace) {
					old = namespace.cache.get(key);
				}

				if (old != null && old.expires >= System.currentTimeMillis()) {
					if (monitor != null)
						monitor.incrementCounter("SET_WAITING_" + ns);

					return;
				}
			}

			if (monitor != null)
				monitor.incrementCounter("SET_" + ns);

			if (value.indexOf("eof") >= 0) {
				value = StringFactory.get(value);

				if (monitor != null)
					monitor.incrementCounter("SET_EOF_" + ns);
			}

			final CacheValue cv = new CacheValue(value, System.currentTimeMillis() + rw.getl("timeout", getDefaultExpiration(ns)) * 1000);

			synchronized (namespace) {
				old = namespace.cache.put(key, cv);
			}

			if (old != null)
				notifyEntryRemoved(namespace, key, old, false, "OVERWRITE_");
			else
				namespace.keys.add(key);

			return;
		}

		if (rw.getb("clear", false)) {
			int removed = 0;

			for (final String keyValue : rw.getValues("key")) {
				String sLargestPart = "";

				final String[] parts = keyValue.split("\\.(\\+|\\*)|\\(|\\)|\\|");

				for (final String part : parts)
					if (part.length() > sLargestPart.length())
						sLargestPart = part;

				if (sLargestPart.equals(keyValue)) {
					CacheValue old;

					synchronized (namespace) {
						old = namespace.cache.remove(keyValue);
					}

					if (old != null) {
						notifyEntryRemoved(namespace, keyValue, old, true, "CLEAR_");
						removed++;
					}

					continue;
				}

				final Pattern p;

				try {
					p = Pattern.compile("^" + keyValue + "$");
				}
				catch (final PatternSyntaxException e) {
					pwOut.println("ERR: invalid pattern syntax: " + keyValue + " : " + e.getMessage());
					return;
				}

				final int largestPartSize = sLargestPart.length();

				Matcher m = null;

				final Iterator<String> it = namespace.keys.iterator();

				while (it.hasNext()) {
					final String itKey = it.next();

					if (largestPartSize > 0 && (itKey.length() < largestPartSize || itKey.indexOf(sLargestPart) < 0))
						continue;

					if (m == null)
						m = p.matcher(itKey);
					else
						m.reset(itKey);

					if (m.matches()) {
						final CacheValue old;

						synchronized (namespace) {
							old = namespace.cache.remove(itKey);
						}

						if (old != null) {
							notifyEntryRemoved(namespace, itKey, old, false, "CLEARPATTERN_");
							removed++;
						}

						it.remove();
					}
				}
			}

			pwOut.println("OK: removed " + removed + " values from ns '" + ns + "' matching " + Arrays.toString(rw.getValues("key")));

			if (removed > 100)
				System.err.println("Removed " + removed + " matching values from ns '" + ns + "' matching " + Arrays.toString(rw.getValues("key")) + ", request issued by " + request.getRemoteAddr()
						+ " / " + request.getHeader("User-Agent"));

			return;
		}

		final CacheValue existing;

		synchronized (namespace) {
			existing = namespace.cache.get(key);
		}

		if (existing == null) {
			if (monitor != null)
				monitor.incrementCounter("NULL_" + ns);

			pwOut.println("ERR: null");
			return;
		}

		if (existing.expires < System.currentTimeMillis()) {
			if (monitor != null)
				monitor.incrementCounter("EXPIRED_" + ns);

			pwOut.println("ERR: expired");
			return;
		}

		existing.accesses.incrementAndGet();

		if (monitor != null)
			monitor.incrementCounter("HIT_" + ns);

		if (existing.value.indexOf("eof") >= 0)
			if (monitor != null)
				monitor.incrementCounter("HIT_EOF_" + ns);

		pwOut.println(existing.value);
	}

	/**
	 * Call the remote text cache to remove all entries related to the given path
	 *
	 * @param lfn
	 */
	public static void invalidateLFN(final String lfn) {
		try {
			invalidateCache("whereis", "irtc_" + lfn, "irc_" + lfn);
			invalidateCache("access", lfn);
		}
		catch (@SuppressWarnings("unused") final Throwable t) {
			// ignore
		}
	}

	/**
	 * Remove entries from the text cache from a given namespace and one or more patterns
	 *
	 * @param ns
	 * @param pattern
	 * @return the outcome of the query as indicated by the server
	 * @throws IOException
	 */
	public static String invalidateCache(final String ns, final String... pattern) throws IOException {
		final String url = ConfigUtils.getConfig().gets("alien.servlets.TextCache.URL", "http://alice-cache.cern.ch:8888/TextCache");

		if (url.trim().length() == 0)
			return null;

		return invalidateCacheEntry(url, ns, pattern);
	}

	/**
	 * @param baseURL
	 *            URL to TextCache
	 * @param ns
	 *            namespace
	 * @param pattern
	 *            key pattern to remove
	 * @return the outcome of the query as indicated by the server
	 * @throws IOException
	 */
	public static String invalidateCacheEntry(final String baseURL, final String ns, final String... pattern) throws IOException {
		final StringBuilder sb = new StringBuilder(baseURL);

		sb.append("?clear=true&ns=").append(Format.encode(ns));

		for (final String p : pattern)
			sb.append("&key=").append(Format.encode(p));

		return Utils.download(sb.toString(), null);
	}

	private static final SimpleDateFormat apacheTimeFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss ZZ");

	private static PrintWriter pwLogOut = null;

	private static long lastOpened = 0;

	private static synchronized PrintWriter getLogWriter() {
		if (System.currentTimeMillis() - lastOpened > 1000 * 60) {
			if (pwLogOut != null)
				pwLogOut.close();

			pwLogOut = null;

			if (ConfigUtils.getConfig().getb("alien.servlets.TextCache.web_log", false))
				try {
					pwLogOut = new PrintWriter(new FileWriter("access_log", true));
				}
				catch (final IOException ioe) {
					System.err.println("Cannot open access_log: " + ioe.getMessage());
				}

			lastOpened = System.currentTimeMillis();
		}

		return pwLogOut;
	}

	private static String getCurrentPage(final HttpServletRequest request) {
		@SuppressWarnings("deprecation")
		final StringBuffer sb = javax.servlet.http.HttpUtils.getRequestURL(request);

		if (request.getQueryString() != null)
			sb.append('?').append(request.getQueryString());

		return sb.toString();
	}

	/**
	 * Log the request in an apache-like access log
	 *
	 * @param request
	 */
	public synchronized static void logRequest(final HttpServletRequest request) {
		@SuppressWarnings("resource")
		final PrintWriter pw = getLogWriter();

		if (pw != null)
			try {
				final String sIP = request.getRemoteAddr();

				String sDate;

				synchronized (apacheTimeFormat) {
					sDate = apacheTimeFormat.format(new Date());
				}

				final String sURL = request.getMethod() + " " + getCurrentPage(request) + " HTTP/1.1";

				pw.println(sIP + " [" + sDate + "] \"" + sURL + "\"");
			}
			catch (@SuppressWarnings("unused") final Throwable t) {
				// ignore
			}
	}
}
