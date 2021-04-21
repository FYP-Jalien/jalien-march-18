/**
 *
 */
package alien.catalogue;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import lazyj.DBFunctions;
import lazyj.Format;
import lazyj.cache.GenericLastValuesCache;

/**
 * @author costing
 * @since Nov 3, 2010
 */
public final class CatalogueUtils {

	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(CatalogueUtils.class.getCanonicalName());

	/**
	 * Monitoring component
	 */
	static final Monitor monitor = MonitorFactory.getMonitor(CatalogueUtils.class.getCanonicalName());

	private static GenericLastValuesCache<Integer, Host> hostsCache = new GenericLastValuesCache<>() {
		private static final long serialVersionUID = 1L;

		@Override
		protected boolean cacheNulls() {
			return false;
		}

		@Override
		protected Host resolve(final Integer key) {
			try (DBFunctions db = ConfigUtils.getDB("alice_users")) {
				if (db != null) {
					db.setReadOnly(true);
					db.setQueryTimeout(15);

					if (!db.query("SELECT * FROM HOSTS WHERE hostIndex=?;", false, key))
						return null;

					if (db.moveNext())
						return new Host(db);
				}
			}

			return null;
		}
	};

	/**
	 * Get the host for this index
	 *
	 * @param idx
	 * @return the Host or <code>null</code> if there is no such host
	 */
	public static Host getHost(final int idx) {
		return hostsCache.get(Integer.valueOf(idx <= 0 ? 1 : idx));
	}

	/**
	 * @return all configured catalogue hosts
	 */
	public static Set<Host> getAllHosts() {
		final Set<Host> ret = new HashSet<>();

		try (DBFunctions db = ConfigUtils.getDB("alice_users")) {
			if (db != null) {
				db.setReadOnly(true);
				db.setQueryTimeout(15);
				db.query("SELECT hostIndex FROM HOSTS;");

				while (db.moveNext())
					ret.add(getHost(db.geti(1)));
			}
		}

		return ret;
	}

	private static List<GUIDIndex> guidIndexCache = null;
	private static long guidIndexCacheUpdated = 0;

	private static final ReentrantReadWriteLock guidIndexRWLock = new ReentrantReadWriteLock();
	private static final ReadLock guidIndexReadLock = guidIndexRWLock.readLock();
	private static final WriteLock guidIndexWriteLock = guidIndexRWLock.writeLock();

	/**
	 * For how long the caches are active
	 */
	public static final long CACHE_TIMEOUT = 1000 * 60 * 5;

	private static void updateGuidIndexCache() {
		guidIndexReadLock.lock();

		try {
			if (System.currentTimeMillis() - guidIndexCacheUpdated > CACHE_TIMEOUT || guidIndexCache == null || guidIndexCache.size() == 0) {
				guidIndexReadLock.unlock();

				guidIndexWriteLock.lock();

				try {
					if (System.currentTimeMillis() - guidIndexCacheUpdated > CACHE_TIMEOUT || guidIndexCache == null || guidIndexCache.size() == 0) {
						if (logger.isLoggable(Level.FINER))
							logger.log(Level.FINER, "Updating GUIDINDEX cache");

						try (DBFunctions db = ConfigUtils.getDB("alice_users")) {
							if (db != null) {
								db.setReadOnly(true);
								db.setQueryTimeout(60);

								if (db.query("SELECT SQL_NO_CACHE * FROM GUIDINDEX ORDER BY guidTime ASC;")) {
									final LinkedList<GUIDIndex> ret = new LinkedList<>();

									while (db.moveNext())
										ret.add(new GUIDIndex(db));

									if (ret.size() > 0) {
										guidIndexCache = ret;

										guidIndexCacheUpdated = System.currentTimeMillis();

										logger.log(Level.FINER, "Finished updating GUIDINDEX cache");
									}
									else
										logger.log(Level.WARNING, "Empty GUID index cache after query");
								}
								else
									logger.log(Level.WARNING, "DB query failed updating GUID index cache");
							}
							else
								logger.log(Level.WARNING, "Cannot get a DB connection to update GUID Index cache");
						}
					}
				}
				finally {
					guidIndexWriteLock.unlock();
					guidIndexReadLock.lock();
				}
			}
		}
		finally {
			guidIndexReadLock.unlock();
		}
	}

	/**
	 * Get the GUIDINDEX entry that contains this timestamp (in milliseconds)
	 *
	 * @param timestamp
	 * @return the GUIDIndex that contains this timestamp (in milliseconds)
	 */
	public static GUIDIndex getGUIDIndex(final long timestamp) {
		updateGuidIndexCache();

		if (guidIndexCache == null)
			return null;

		GUIDIndex old = null;

		for (final GUIDIndex idx : guidIndexCache) {
			if (idx.guidTime > timestamp)
				return old;

			old = idx;
		}

		return old;
	}

	/**
	 * Get all GUIDINDEX rows
	 *
	 * @return all GUIDINDEX rows
	 */
	public static List<GUIDIndex> getAllGUIDIndexes() {
		updateGuidIndexCache();

		if (guidIndexCache == null)
			return null;

		return Collections.unmodifiableList(guidIndexCache);
	}

	private static List<IndexTableEntry> indextable = null;
	private static Map<String, IndexTableEntry> tableentries = null;
	private static long lastIndexTableUpdate = 0;

	private static final ReentrantReadWriteLock indextableRWLock = new ReentrantReadWriteLock();
	private static final ReadLock indextableReadLock = indextableRWLock.readLock();
	private static final WriteLock indextableWriteLock = indextableRWLock.writeLock();

	private static void updateIndexTableCache() {
		indextableReadLock.lock();

		try {
			if (System.currentTimeMillis() - lastIndexTableUpdate > CACHE_TIMEOUT || indextable == null || indextable.size() == 0) {
				indextableReadLock.unlock();

				indextableWriteLock.lock();

				try {
					if (System.currentTimeMillis() - lastIndexTableUpdate > CACHE_TIMEOUT || indextable == null || indextable.size() == 0) {
						if (logger.isLoggable(Level.FINER))
							logger.log(Level.FINER, "Updating INDEXTABLE cache");

						try (DBFunctions db = ConfigUtils.getDB("alice_users")) {
							if (db != null) {
								db.setReadOnly(true);
								db.setQueryTimeout(60);

								if (db.query("SELECT SQL_NO_CACHE * FROM INDEXTABLE order by length(lfn) desc,lfn;")) {
									final List<IndexTableEntry> newIndextable = new ArrayList<>();
									final Map<String, IndexTableEntry> newTableentries = new HashMap<>();

									while (db.moveNext()) {
										final IndexTableEntry entry = new IndexTableEntry(db);

										newIndextable.add(entry);

										newTableentries.put(db.gets("lfn"), entry);
									}

									if (newIndextable.size() > 0) {
										logger.log(Level.FINER, "INDEXTABLE cache updated successfully");

										indextable = newIndextable;
										tableentries = newTableentries;

										lastIndexTableUpdate = System.currentTimeMillis();
									}
									else
										logger.log(Level.WARNING, "Empty list of INDEXTABLE entries");
								}
								else
									logger.log(Level.WARNING, "DB query error updating the INDEXTABLE entries");
							}
							else
								logger.log(Level.WARNING, "Could not get a DB connection to update INDEXTABLE cache");
						}
					}
				}
				finally {
					indextableWriteLock.unlock();
				}

				indextableReadLock.lock();
			}
		}
		finally {
			indextableReadLock.unlock();
		}
	}

	/**
	 * When it is known that the indextable content might have changed (eg. after a moveDirectory operation), call this to be sure the correct table is used
	 */
	public static void invalidateIndexTableCache() {
		lastIndexTableUpdate = 0;
	}

	/**
	 * When it is known that the GUID Index table was changed
	 */
	public static void invalidateGUIDIndexTableCache() {
		guidIndexCacheUpdated = 0;
	}

	/**
	 * Get the base folder for this table name
	 *
	 * @param hostId
	 *
	 * @param tableName
	 * @return entry in INDEXTABLE for this table name
	 */
	public static IndexTableEntry getIndexTable(final int hostId, final int tableName) {
		updateIndexTableCache();

		if (indextable == null)
			return null;

		for (final IndexTableEntry ite : indextable)
			if (ite.tableName == tableName && ite.hostIndex == hostId)
				return ite;

		return null;
	}

	/**
	 * @return all known L%L tables
	 */
	public static Collection<IndexTableEntry> getAllIndexTables() {
		updateIndexTableCache();

		if (indextable == null)
			return null;

		return Collections.unmodifiableList(indextable);
	}

	/**
	 * For a given path, get the closest match for LFNs from INDEXTABLE
	 *
	 * @param pattern
	 * @return the best match, or <code>null</code> if none could be found
	 */
	public static IndexTableEntry getClosestMatch(final String pattern) {
		updateIndexTableCache();

		if (indextable == null)
			return null;

		if (monitor != null)
			monitor.incrementCounter("INDEXTABLE_lookup");

		String searchFor = pattern;

		while (searchFor.length() > 0) {
			final IndexTableEntry entry = tableentries.get(searchFor);

			if (entry != null)
				return entry;

			final int idx = searchFor.lastIndexOf('/', searchFor.length() - 2);

			if (idx >= 0)
				searchFor = searchFor.substring(0, idx + 1);
			else
				break;
		}

		return null;
	}

	/**
	 * @param pattern
	 * @return all tables that belong to this tree
	 */
	public static Set<IndexTableEntry> getAllMatchingTables(final String pattern) {
		final IndexTableEntry best = getClosestMatch(pattern);

		if (best == null)
			return Collections.emptySet();

		final Set<IndexTableEntry> ret = new LinkedHashSet<>();

		ret.add(best);

		for (final IndexTableEntry ite : indextable)
			if (ite.lfn.startsWith(pattern))
				ret.add(ite);

		return ret;
	}

	/**
	 * @param pattern
	 * @return the Java pattern
	 */
	public static Pattern dbToJavaPattern(final String pattern) {
		String p = Format.replace(pattern, "*", "%");
		p = Format.replace(p, "%%", "%");
		p = Format.replace(p, ".", "\\.");
		p = Format.replace(p, "_", ".");
		p = Format.replace(p, "%", ".*");

		return Pattern.compile(p);
	}

	/**
	 * @param path
	 * @return <code>true</code> if this path is held in a separate table
	 */
	public static IndexTableEntry getSeparateTable(final String path) {
		if (path == null || path.length() == 0 || !path.startsWith("/"))
			return null;

		updateIndexTableCache();

		if (!path.endsWith("/"))
			return tableentries.get(path + "/");

		return tableentries.get(path);
	}

	/**
	 * Create a local file with the list of GUIDs that have no LFNs pointing to them any more
	 *
	 * @param outputFile
	 *            file name that will contain the list of GUIDs at the end
	 *
	 * @throws IOException
	 *             if the indicated local file cannot be created
	 */
	public static void guidCleanup(final String outputFile) throws IOException {
		if (Runtime.getRuntime().totalMemory() < 100 * 1024 * 1024 * 1024L) {
			System.err.println("The cleanup should run in a JVM with a _lot_ of memory, 128 at least, if not 200GB");
			return;
		}

		try (PrintWriter pw = new PrintWriter(new FileWriter(outputFile))) {
			final HashMap<UUID, Long> guids = new HashMap<>(1600000000);

			final long started = System.currentTimeMillis();

			int cnt = 0;

			final List<GUIDIndex> guidTables = new ArrayList<>(CatalogueUtils.getAllGUIDIndexes());

			Collections.sort(guidTables);
			Collections.reverse(guidTables);

			int invalid = 0;

			long totalSize = 0;

			final long LIMIT = 10000000;

			long lTotalSpaceToReclaim = 0;

			for (final GUIDIndex idx : guidTables) {
				cnt++;

				System.err.println("Reached G" + idx.tableName + "L (" + cnt + " / " + guidTables.size() + ")");

				final Host h = CatalogueUtils.getHost(idx.hostIndex);

				try (DBFunctions gdb = h.getDB()) {

					gdb.query("set wait_timeout=31536000;");

					gdb.setReadOnly(true);

					int read;

					long offset = 0;

					do {
						read = 0;

						final String q = "select guid,size from G" + idx.tableName + "L LIMIT ? OFFSET ?;";

						while (!gdb.query(q, false, Long.valueOf(LIMIT), Long.valueOf(offset)))
							System.err.println("Retrying query " + q);

						while (gdb.moveNext()) {
							read++;

							try {
								final byte[] data = gdb.getBytes(1);

								if (data != null && data.length == 16) {
									final UUID uuid = GUID.getUUID(data);

									if (uuid != null) {
										guids.put(uuid, Long.valueOf(gdb.getl(2)));
										totalSize += gdb.getl(2);
									}
									else
										invalid++;
								}
								else
									invalid++;
							}
							catch (@SuppressWarnings("unused") final Exception e) {
								invalid++;
							}

							if (guids.size() % 1000000 == 0) {
								System.err.println("Reached " + guids.size() + " in G" + idx.tableName + "L");
								System.err.println(Format.toInterval(System.currentTimeMillis() - started) + " : free " + Format.size(Runtime.getRuntime().freeMemory()) + " / total "
										+ Format.size(Runtime.getRuntime().totalMemory()));
							}
						}

						offset += read;
					} while (read == LIMIT);
				}

				if (guids.size() > 1300000000) {
					System.err.println("Intermediate cleanup @ " + guids.size());

					lTotalSpaceToReclaim += lfnCleanup(guids, pw);

					System.err.println("So far will reclaim: " + Format.size(lTotalSpaceToReclaim));
				}
			}

			System.err.println("Final parsing starting with " + guids.size() + " UUIDs in memory, " + invalid + " rows had invalid GUID representation, total size: " + Format.size(totalSize));
			System.err.println(Format.toInterval(System.currentTimeMillis() - started) + " : free " + Format.size(Runtime.getRuntime().freeMemory()) + " / total "
					+ Format.size(Runtime.getRuntime().totalMemory()));

			lTotalSpaceToReclaim += lfnCleanup(guids, pw);

			System.err.println("Total space to reclaim: " + Format.size(lTotalSpaceToReclaim));
		}
	}

	private static long lfnCleanup(final Map<UUID, Long> guids, final PrintWriter pw) {
		final Collection<IndexTableEntry> indextableCollection = CatalogueUtils.getAllIndexTables();

		int cnt = 0;

		final int LIMIT = 15000000;

		for (final IndexTableEntry ite : indextableCollection) {
			cnt++;

			System.err.println("Checking the content of L" + ite.tableName + "L from " + ite.hostIndex + " (" + cnt + "/" + indextableCollection.size() + "), " + guids.size() + " UUIDs left");

			try (DBFunctions db = ite.getDB()) {

				db.query("set wait_timeout=31536000;");

				db.setReadOnly(true);

				long read = 0;

				long offset = 0;

				do {
					read = 0;

					final String q = "SELECT guid FROM L" + ite.tableName + "L where guid is not null LIMIT " + LIMIT + " OFFSET " + offset + ";";

					while (!db.query(q))
						System.err.println("Retrying query");

					while (db.moveNext()) {
						read++;
						try {
							final byte[] data = db.getBytes(1);

							if (data != null && data.length == 16) {
								final UUID uuid = GUID.getUUID(data);

								if (uuid != null)
									guids.remove(uuid);
							}
						}
						catch (@SuppressWarnings("unused") final Exception e) {
							// ignore
						}
					}

					offset += read;
				} while (read == LIMIT);
			}
		}

		long ret = 0;

		for (final Map.Entry<UUID, Long> uuid : guids.entrySet()) {
			pw.println(uuid.getKey() + " " + uuid.getValue());

			ret += uuid.getValue().longValue();
		}

		guids.clear();

		return ret;
	}

	/**
	 * Mark LFN_BOOKED entries as delete for a job
	 *
	 * @param queueId
	 */
	public static void cleanLfnBookedForJob(final long queueId) {
		for (final Host h : CatalogueUtils.getAllHosts()) {
			try (DBFunctions db = ConfigUtils.getDB(h.db)) {
				db.setReadOnly(false);
				db.setQueryTimeout(60);

				if (logger.isLoggable(Level.FINER))
					logger.log(Level.FINER, "cleanLfnBookedForJob: deleting for job " + queueId + " in host: " + h.db);

				db.query("update LFN_BOOKED set expiretime=-1 where jobId=?", false, Long.valueOf(queueId));
			}
		}
	}
}
