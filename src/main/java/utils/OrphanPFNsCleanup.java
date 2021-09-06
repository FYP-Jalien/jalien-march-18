package utils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.security.GeneralSecurityException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.catalogue.CatalogueUtils;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.Host;
import alien.catalogue.PFN;
import alien.catalogue.access.AccessTicket;
import alien.catalogue.access.AccessType;
import alien.catalogue.access.XrootDEnvelope;
import alien.config.ConfigUtils;
import alien.io.protocols.Factory;
import alien.io.xrootd.envelopes.XrootDEnvelopeSigner;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.se.SE;
import alien.se.SEUtils;
import lazyj.DBFunctions;
import lazyj.DBFunctions.DBConnection;
import lazyj.Format;
import lia.Monitor.monitor.AppConfig;

/**
 * Go over the orphan_pfns and try to physically remove the entries
 *
 * @author costing
 *
 */
public class OrphanPFNsCleanup {
	/**
	 * logger
	 */
	static final Logger logger = ConfigUtils.getLogger(OrphanPFNsCleanup.class.getCanonicalName());

	/**
	 * ML monitor object
	 */
	static final Monitor monitor = MonitorFactory.getMonitor(OrphanPFNsCleanup.class.getCanonicalName());

	/**
	 * Thread pool per SE
	 */
	static final Map<Integer, CachedThreadPool> EXECUTORS = new ConcurrentHashMap<>();

	/**
	 * One thread per SE
	 */
	static Map<Integer, SEThread> SE_THREADS = new ConcurrentHashMap<>();

	/**
	 * Whether or not the stats have changed and they should be printed on screen
	 */
	volatile static boolean dirtyStats = true;

	/**
	 * @author costing
	 */
	static final class MainOrphanMover extends Thread {
		/**
		 * Default constructor
		 */
		public MainOrphanMover() {
			setName("MainOrphanMover");
		}

		private int updateCount = -1;

		private ResultSet resultSet = null;

		private Statement stat = null;

		private final void executeClose() {
			if (resultSet != null) {
				try {
					resultSet.close();
				}
				catch (@SuppressWarnings("unused") final Throwable t) {
					// ignore
				}

				resultSet = null;
			}

			if (stat != null) {
				try {
					stat.close();
				}
				catch (@SuppressWarnings("unused") final Throwable t) {
					// ignore
				}

				stat = null;
			}
		}

		@SuppressWarnings("resource")
		private final boolean executeQuery(final DBConnection dbc, final String query) {
			executeClose();

			try {
				stat = dbc.getConnection().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

				if (stat.execute(query, Statement.NO_GENERATED_KEYS)) {
					updateCount = -1;

					resultSet = stat.getResultSet();
				}
				else {
					updateCount = stat.getUpdateCount();

					executeClose();
				}

				return true;
			}
			catch (final SQLException e) {
				logger.log(Level.WARNING, "Exception executing the query", e);

				return false;
			}
		}

		@Override
		public void run() {
			while (true) {
				try {
					for (final Host h : CatalogueUtils.getAllHosts())
						try (DBFunctions db = h.getDB()) {
							db.setReadOnly(true);
							db.query("SELECT distinct se FROM orphan_pfns;");

							final List<Integer> ses = new LinkedList<>();

							while (db.moveNext())
								ses.add(Integer.valueOf(db.geti(1)));

							for (final Integer seNumber : ses)
								try (DBFunctions db2 = h.getDB()) {
									db2.query("CREATE TABLE IF NOT EXISTS orphan_pfns_" + seNumber + " LIKE orphan_pfns_0;", true);

									final DBConnection dbc = db2.getConnection();
									dbc.setReadOnly(false);

									executeQuery(dbc, "LOCK TABLES orphan_pfns WRITE, orphan_pfns_" + seNumber + " WRITE;");

									try {
										long lStart = System.currentTimeMillis();

										final String sWhere = "WHERE se" + (seNumber.intValue() > 0 ? "=" + seNumber : " is null");
										executeQuery(dbc, "INSERT IGNORE INTO orphan_pfns_" + seNumber + " SELECT * FROM orphan_pfns " + sWhere);

										logger.log(Level.INFO, "Inserted into " + h.db + ".orphan_pfns_" + seNumber + " " + updateCount + " from " + h.db + ".orphan_pfns, took "
												+ Format.toInterval(System.currentTimeMillis() - lStart));

										lStart = System.currentTimeMillis();

										executeQuery(dbc, "DELETE FROM orphan_pfns " + sWhere);

										logger.log(Level.INFO,
												"Deleted " + updateCount + " from " + h.db + ".orphan_pfns " + sWhere + ", took " + Format.toInterval(System.currentTimeMillis() - lStart));

										if (updateCount > 0)
											startSEThread(seNumber.intValue() > 0 ? SEUtils.getSE(seNumber) : null);
									}
									finally {
										executeQuery(dbc, "UNLOCK TABLES;");
										executeClose();
										dbc.free();
									}
								}
						}
				}
				catch (final Throwable t) {
					logger.log(Level.SEVERE, "Exception in the main mover", t);
				}
				try {
					sleep(1000L * 60 * ConfigUtils.getConfig().geti("utils.OrphanPFNsCleanup.mainMoverSleep", 30));
				}
				catch (@SuppressWarnings("unused") final InterruptedException e) {
					break;
				}
			}
		}
	}

	/**
	 * Start a new thread to deal with a particular SE
	 *
	 * @param theSE
	 */
	static synchronized void startSEThread(final SE theSE) {
		final Integer se = Integer.valueOf(theSE != null ? theSE.seNumber : 0);

		if (!SE_THREADS.containsKey(se)) {
			final SEThread t = new SEThread(theSE);

			SE_THREADS.put(se, t);

			if (logger.isLoggable(Level.INFO))
				logger.log(Level.INFO, "Starting SE thread for " + se + " (" + (theSE != null ? theSE.seName : "AliEn GUIDs") + ")");

			t.start();
		}
		else if (logger.isLoggable(Level.INFO))
			logger.log(Level.INFO, "Not starting an SE thread for " + se + " (" + (theSE != null ? theSE.seName : "AliEn GUIDs") + ") because the key is already in SE_THREADS");
	}

	/**
	 * @param args
	 * @throws FileNotFoundException
	 */
	public static void main(final String[] args) throws FileNotFoundException {
		AppConfig.getProperty("lia.Monitor.group"); // initialize it

		long lastCheck = 0;

		final MainOrphanMover mover = new MainOrphanMover();
		mover.start();

		try (PrintWriter pw = new PrintWriter("OrphanPFNsCleanup.progress")) {
			while (true) {
				try {
					if (System.currentTimeMillis() - lastCheck > ConfigUtils.getConfig().geti("utils.OrphanPFNsCleanup.SE_list_check_interval", 60 * 2) * 1000L * 60) {
						logger.log(Level.INFO, "Waking up the cleanup threads");

						startSEThread(null);

						for (final SE se : SEUtils.getSEs(null))
							startSEThread(se);

						lastCheck = System.currentTimeMillis();
					}
				}
				catch (final Throwable t) {
					logger.log(Level.SEVERE, "Exception in the SE thread check/start part", t);
				}

				try {
					Thread.sleep(1000 * 60);
				}
				catch (@SuppressWarnings("unused") final InterruptedException ie) {
					// ignore
				}

				final long count = reclaimedCount.getAndSet(0);
				final long size = reclaimedSize.getAndSet(0);

				try (DBFunctions db = ConfigUtils.getDB("alice_users")) {
					db.setQueryTimeout(30);

					if (count > 0)
						db.query("UPDATE orphan_pfns_status SET status_value=status_value+" + count + " WHERE status_key='reclaimedc';");

					if (size > 0)
						db.query("UPDATE orphan_pfns_status SET status_value=status_value+" + size + " WHERE status_key='reclaimedb';");
				}

				if (dirtyStats) {
					final String message = "Removed: " + removed + " (" + Format.size(reclaimedSpace.longValue()) + "), failed to remove: " + failed + " (delta: " + count + " files, "
							+ Format.size(size) + "), sem. status: " + concurrentQueryies.availablePermits();

					syslog(message);

					pw.println((new Date()) + " : " + message);
					pw.flush();

					dirtyStats = false;
				}
			}
		}
	}

	private static final class SEThread extends Thread {
		final SE se;
		final int seNumber;

		public SEThread(final SE se) {
			this.se = se;
			seNumber = se != null ? se.seNumber : 0;
		}

		private static final int getPoolSize(final int seNumber) {
			int ret = ConfigUtils.getConfig().geti("utils.OrphanPFNsCleanup.threadsPerSE", 16);
			ret = ConfigUtils.getConfig().geti("utils.OrphanPFNsCleanup.threadsPerSE." + seNumber, ret);

			return ret;
		}

		@Override
		public void run() {
			setName("SEThread (" + (se != null ? (se.getName() + " - " + se.seNumber) : "AliEn GUIDs") + ") - just started");

			CachedThreadPool executor = EXECUTORS.get(Integer.valueOf(seNumber));

			try {
				int tasks = 0;

				while (true) {
					boolean nothingToDelete = true;

					for (final Host h : CatalogueUtils.getAllHosts())
						try (DBFunctions db = h.getDB()) {
							db.setReadOnly(true);

							concurrentQueryies.acquireUninterruptibly();
							boolean ok;

							try {
								// TODO : what to do with these PFNs ? Iterate over them
								// and release them from the catalogue nevertheless ?
								// db.query("DELETE FROM orphan_pfns WHERE se="+seNumber+" AND fail_count>10;");

								if (seNumber > 0)
									ok = db.query("SELECT binary2string(guid),size,md5sum,pfn, flags FROM orphan_pfns_" + seNumber
											+ " WHERE fail_count<10 ORDER BY size/((fail_count * 5) + 1) DESC LIMIT 100000;", true);
								else
									ok = db.query("SELECT binary2string(guid) FROM orphan_pfns_0 ORDER BY size DESC LIMIT 10000000;");
							}
							finally {
								concurrentQueryies.release();
							}

							if (ok && db.moveNext()) {
								nothingToDelete = false;

								if (executor == null) {
									// lazy init of the thread pool
									logger.log(Level.INFO, "Creating a new cached thread pool of " + getPoolSize(seNumber) + " threads for " + (se != null ? se.getName() : "GUIDs"));

									executor = new CachedThreadPool(getPoolSize(seNumber), 1, TimeUnit.MINUTES, r -> {
										final Thread t = new Thread(r);
										t.setName("Cleanup of " + (se != null ? se.getName() : "GUIDs") + " - " + seNumber);

										return t;
									});

									EXECUTORS.put(Integer.valueOf(seNumber), executor);
								}
								else {
									final int threads = getPoolSize(seNumber);

									executor.setCorePoolSize(threads);
									executor.setMaximumPoolSize(threads);
								}

								List<UUID> nullUUIDs = new ArrayList<>();

								do {
									if (seNumber > 0)
										executor.submit(new CleanupTask(h, db.gets(1), seNumber, db.getl(2), db.gets(3), db.gets(4), db.geti(5)));
									else {
										try {
											nullUUIDs.add(UUID.fromString(db.gets(1)));

											if (nullUUIDs.size() > 10000) {
												executor.submit(new NullSETask(h, nullUUIDs));
												nullUUIDs = new ArrayList<>();
											}
										}
										catch (final Throwable t) {
											logger.log(Level.SEVERE, "Cannot queue the GUID: " + db.gets(1), t);
										}
									}

									tasks++;
								} while (db.moveNext());

								if (nullUUIDs.size() > 0)
									executor.submit(new NullSETask(h, new ArrayList<>(nullUUIDs)));
							}
						}

					if (nothingToDelete) {
						// there are no tasks for this SE now, check again
						// sometime later

						if (logger.isLoggable(Level.INFO))
							logger.log(Level.INFO, "No more PFNs to clean up for " + (se != null ? se.getName() : "AliEn GUIDs") + " - " + seNumber
									+ ", freeing the respective thread and executor for now after executing " + tasks + " tasks");

						return;
					}

					setName("SEThread (" + (se != null ? (se.getName() + " - " + se.seNumber) : "AliEn GUIDs") + ") - " + tasks + " tasks");

					int queued;

					do {
						try {
							Thread.sleep(5000);
						}
						catch (@SuppressWarnings("unused") final InterruptedException ie) {
							// ignore
						}

						queued = executor.getQueue().size() + executor.getActiveCount();

						setName("SEThread (" + (se != null ? (se.getName() + " - " + se.seNumber) : "AliEn GUIDs") + ") - " + tasks + " total tasks, " + queued + " queued");
					} while (queued > 0);
				}
			}
			catch (final Throwable t) {
				logger.log(Level.SEVERE, "Caught exception in the SE thread (" + seNumber + ")", t);
			}
			finally {
				try {
					if (executor != null) {
						executor.shutdown();

						EXECUTORS.remove(Integer.valueOf(seNumber));
					}
				}
				catch (@SuppressWarnings("unused") final Throwable t) {
					// ignore
				}

				SE_THREADS.remove(Integer.valueOf(seNumber));
			}
		}
	}

	/**
	 * Number of files kept
	 */
	static final AtomicInteger kept = new AtomicInteger();

	/**
	 * Number of failed attempts
	 */
	static final AtomicInteger failed = new AtomicInteger();

	/**
	 * Number of successfully removed files
	 */
	static final AtomicInteger removed = new AtomicInteger();

	/**
	 * Amount of space reclaimed
	 */
	static final AtomicLong reclaimedSpace = new AtomicLong();

	/**
	 * Fail one file
	 *
	 * @param se
	 */
	static final void failOne(final SE se) {
		failed.incrementAndGet();

		monitor.incrementCounter(se.getName() + "_fail_count");

		monitor.incrementCounter("TOTAL_fail_count");

		dirtyStats = true;
	}

	private static final AtomicLong reclaimedCount = new AtomicLong();
	private static final AtomicLong reclaimedSize = new AtomicLong(0);

	/**
	 * Successful deletion of one file
	 *
	 * @param se
	 *
	 * @param size
	 */
	static final void successOne(final SE se, final long size) {
		removed.incrementAndGet();
		reclaimedCount.incrementAndGet();

		if (size > 0) {
			reclaimedSpace.addAndGet(size);
			reclaimedSize.addAndGet(size);
		}

		monitor.incrementCounter(se.getName() + "_success_count");
		monitor.addMeasurement(se.getName() + "_success_size", size);

		monitor.incrementCounter("TOTAL_success_count");
		monitor.addMeasurement("TOTAL_success_size", size);

		dirtyStats = true;
	}

	/**
	 * Lock for a fixed number of DB queries in parallel
	 */
	static final Semaphore concurrentQueryies = new Semaphore(ConfigUtils.getConfig().geti("utils.OrphanPFNsCleanup.concurrentQueries", 32));

	private static class NullSETask implements Runnable {
		final Host h;
		final List<UUID> uuids;

		public NullSETask(final Host h, final List<UUID> uuids) {
			this.h = h;
			this.uuids = uuids;
		}

		@Override
		public void run() {
			concurrentQueryies.acquireUninterruptibly();

			try (DBFunctions db = h.getDB()) {
				final Set<GUID> guids = GUIDUtils.getGUIDs(uuids.toArray(new UUID[0]));

				for (final GUID g : guids)
					g.delete(true);

				for (final UUID u : uuids)
					db.query("DELETE FROM orphan_pfns_0 WHERE guid=string2binary(?);", false, u.toString());
			}
			finally {
				concurrentQueryies.release();
			}
		}
	}

	private static class CleanupTask implements Runnable {
		final Host h;
		final String sGUID;
		final int seNumber;
		final long size;
		final String md5;
		final String knownPFN;
		final int flags;

		public CleanupTask(final Host h, final String sGUID, final int se, final long size, final String md5, final String knownPFN, final int flags) {
			this.h = h;
			this.sGUID = sGUID;
			this.seNumber = se;
			this.size = size;
			this.md5 = md5;
			this.knownPFN = knownPFN;
			this.flags = flags;
		}

		@Override
		public void run() {
			final UUID uuid = UUID.fromString(sGUID);

			final GUID guid;

			if ((flags & 1) == 1)
				guid = new GUID(uuid);
			else {
				concurrentQueryies.acquireUninterruptibly();

				try {
					guid = GUIDUtils.getGUID(uuid, true);
				}
				finally {
					concurrentQueryies.release();
				}
			}

			final SE se = SEUtils.getSE(seNumber);

			if (se == null) {
				syslog("Cannot find any se with seNumber=" + seNumber);
				kept.incrementAndGet();
				return;
			}

			if (!guid.exists()) {
				guid.size = size > 0 ? size : 123456;
				guid.md5 = md5 != null ? md5 : "130254d9540d6903fa6f0ab41a132361";
			}

			final PFN pfn;

			try {
				pfn = knownPFN == null || knownPFN.length() == 0 ? new PFN(guid, se) : new PFN(knownPFN, guid, se);
			}
			catch (final Throwable t) {
				syslog("Cannot generate the entry for " + seNumber + " (" + se.getName() + ") and " + sGUID);
				t.printStackTrace();

				kept.incrementAndGet();
				return;
			}

			concurrentQueryies.acquireUninterruptibly();

			final XrootDEnvelope env;

			try {
				env = new XrootDEnvelope(AccessType.DELETE, pfn);
			}
			finally {
				concurrentQueryies.release();
			}

			try {
				if (se.needsEncryptedEnvelope)
					XrootDEnvelopeSigner.encryptEnvelope(env);
				else
					// new xrootd implementations accept signed-only envelopes
					XrootDEnvelopeSigner.signEnvelope(env);
			}
			catch (final GeneralSecurityException e) {
				e.printStackTrace();
				return;
			}

			pfn.ticket = new AccessTicket(AccessType.DELETE, env);

			try (DBFunctions db2 = h.getDB()) {
				if (!Factory.xrootd.delete(pfn)) {
					syslog("Could not delete from " + se.getName());

					concurrentQueryies.acquireUninterruptibly();
					try {
						db2.query("UPDATE orphan_pfns_" + seNumber + " SET fail_count=fail_count+1 WHERE guid=string2binary(?);", false, sGUID);

						failOne(se);
					}
					finally {
						concurrentQueryies.release();
					}
				}
				else {
					concurrentQueryies.acquireUninterruptibly();

					try {
						syslog("Successfully deleted the replica of " + guid.guid + " (" + Format.size(guid.size) + ") from " + se.getName());

						if (guid.exists()) {
							successOne(se, guid.size);

							// we have just physically this entry, do _not_ queue this pfn again
							if (guid.removePFN(se, false) != null) {
								if (guid.getPFNs().size() == 0) {
									// already purged all entries
									if (guid.delete(false))
										syslog("  Deleted the GUID " + guid.guid + " since this was the last replica");
									else
										syslog("  Failed to delete the GUID even if this was the last replica:\n" + guid);
								}
								else
									syslog("  Kept the GUID " + guid.guid + " since it still has " + guid.getPFNs().size() + " replicas");
							}
							else
								syslog("  Failed to remove the replica on " + se.getName() + " from " + guid.guid);
						}
						else {
							successOne(se, size);

							if ((flags & 1) == 0)
								syslog("  GUID " + guid.guid + " doesn't exist in the catalogue any more");
						}

						db2.query("DELETE FROM orphan_pfns_" + seNumber + " WHERE guid=string2binary(?);", false, sGUID);
					}
					finally {
						concurrentQueryies.release();
					}
				}
			}
			catch (final IOException e) {
				// e.printStackTrace();

				failOne(se);

				syslog("Exception deleting " + guid.guid + " from " + se.getName() + " : " + e.getMessage());

				if (logger.isLoggable(Level.FINER))
					logger.log(Level.FINER, "Exception deleting from " + se.getName(), e);

				concurrentQueryies.acquireUninterruptibly();

				try (DBFunctions db2 = h.getDB()) {
					db2.query("UPDATE orphan_pfns_" + seNumber + " SET fail_count=fail_count+1 WHERE guid=string2binary(?);", false, sGUID);
				}
				finally {
					concurrentQueryies.release();
				}
			}
		}
	}

	private static void syslog(final String message) {
		System.err.println(System.currentTimeMillis() + " " + message);
	}

}
