package alien.test.cassandra;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.datastax.driver.core.ConsistencyLevel;

import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.LFN_CSD;
import alien.catalogue.PFN;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import alien.test.JobDiscoverer;

/**
 *
 */
public class CatalogueToCassandraThreads {
	/** Array of thread-dir */
	static final HashMap<Long, LFN> activeThreadFolders = new HashMap<>();

	/** Thread pool */
	static ThreadPoolExecutor tPool = null;

	/**
	 * When to exit
	 */
	static boolean shouldexit = false;

	/**
	 * Unique Ctime for auto insertion
	 */
	public static final Date ctime_fixed = new Date(1483225200000L);

	/**
	 * limit
	 */
	static final int origlimit = 50000;

	/** Entries processed */
	static AtomicInteger global_count = new AtomicInteger();
	/**
	 * Limit number of entries
	 */
	static AtomicInteger limit = new AtomicInteger(origlimit);

	/**
	 * Limit number of entries
	 */
	static AtomicInteger timing_count = new AtomicInteger();

	/**
	 * Limit number of entries for folders
	 */
	static AtomicInteger timing_count_dirs = new AtomicInteger();

	/**
	 * total milliseconds
	 */
	static AtomicLong ns_count = new AtomicLong();

	/**
	 * total milliseconds for folders
	 */
	static AtomicLong ns_count_dirs = new AtomicLong();

	/** File for tracking created folders */
	static PrintWriter out = null;
	/**
	 * Various log files
	 */
	static PrintWriter pw = null;
	/**
	 * Log file
	 */
	static PrintWriter failed_folders = null;
	/**
	 * Log file
	 */
	static PrintWriter failed_files = null;
	/**
	 * Log file
	 */
	static PrintWriter failed_collections = null;
	/**
	 * Log files
	 */
	static PrintWriter failed_ses = null;
	/**
	 * Suffix for log files
	 */
	static String logs_suffix = "";
	/**
	 * Counter stuck threads
	 */
	static int count_thread_stuck = 0;

	/**
	 * Default Cassandra consistency
	 */
	static ConsistencyLevel clevel = ConsistencyLevel.QUORUM;
	/**
	 * Monitoring component
	 */
	static final Monitor monitor = MonitorFactory.getMonitor(CatalogueToCassandraThreads.class.getCanonicalName());

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(final String[] args) throws IOException {
		if (args.length < 1) {
			System.err.println("Usage: CatalogueToCassandraThreads real|auto ");
			System.exit(-3);
		}

		final String[] newargs = new String[args.length - 1];
		for (int i = 1; i < args.length; i++)
			newargs[i - 1] = args[i];

		if ("real".equals(args[0]))
			main_real(newargs);
		else if ("auto".equals(args[0]))
			main_auto(newargs);
		else {
			System.err.println("Usage: CatalogueToCassandraThreads real|auto ");
			System.exit(-3);
		}
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main_real(final String[] args) throws IOException {
		final int nargs = args.length;

		if (nargs < 1) {
			System.err.println("Usage: ./run.sh alien/src/test/CatalogueToCassandraThreads <alien_path> [<pool_size>] [<logs_suffix>]");
			System.err.println("E.g. <alien_path> -> /alice/sim/2016/");
			System.err.println("E.g. <pool_size> -> 8");
			System.err.println("E.g. <logs-suffix> -> alice-sim-2016");
			System.err.println("E.g. <consistency> -> 1-one 2-quorum");
			System.exit(-3);
		}

		for (int i = 0; i < nargs; i++) {
			System.out.println("Parameter " + i + ": " + args[i]);
			System.out.flush();
		}

		int pool_size = 16;
		if (nargs > 1)
			pool_size = Integer.parseInt(args[1]);
		System.out.println("Pool size: " + pool_size);
		tPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(pool_size);
		tPool.setKeepAliveTime(1, TimeUnit.MINUTES);
		tPool.allowCoreThreadTimeOut(true);

		if (nargs > 2)
			logs_suffix = "-" + args[2];

		final int consistency = Integer.parseInt(args[3]);
		if (consistency == 1)
			clevel = ConsistencyLevel.ONE;

		System.out.println("Printing output to: out" + logs_suffix);
		out = new PrintWriter(new FileOutputStream("out" + logs_suffix));
		out.println("Starting: " + new Date());
		out.flush();

		System.out.println("Printing folders to: folders" + logs_suffix);
		pw = new PrintWriter(new FileOutputStream("folders" + logs_suffix));

		System.out.println("Printing failed folders to failed_folders" + logs_suffix);
		failed_folders = new PrintWriter(new FileOutputStream("failed_folders" + logs_suffix));

		System.out.println("Printing failed collections to failed_collections" + logs_suffix);
		failed_collections = new PrintWriter(new FileOutputStream("failed_collections" + logs_suffix));

		System.out.println("Printing failed files to failed_files" + logs_suffix);
		failed_files = new PrintWriter(new FileOutputStream("failed_files" + logs_suffix));

		System.out.println("Printing failed ses to failed_ses" + logs_suffix);
		failed_ses = new PrintWriter(new FileOutputStream("failed_ses" + logs_suffix));

		System.out.println("Going to create " + args[0] + " hierarchy. Time: " + new Date());

		// Control-C catch
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				final List<Runnable> tQueue = tPool.shutdownNow();

				try {
					while (!tPool.awaitTermination(5, TimeUnit.SECONDS))
						System.out.println("Waiting for threads finishing..." + tPool.getActiveCount());
				}
				catch (final InterruptedException e) {
					System.err.println("Something went wrong in shutdown!: " + e);
				}

				try {
					try (PrintWriter pendingTasks = new PrintWriter(new FileOutputStream("pendingTasks" + logs_suffix))) {
						pendingTasks.println("Dumping tQueue\n");

						for (final Runnable r : tQueue) {
							final Object realTask = JobDiscoverer.findRealTask(r);
							pendingTasks.println(realTask.toString());
						}

						pendingTasks.println("Dumping activeThreadFolders\n");

						for (final LFN l : activeThreadFolders.values())
							pendingTasks.println(l.lfn);
					}
				}
				catch (final Exception e) {
					System.err.println("Something went wrong dumping tasks!: " + e.toString() + " - " + tQueue.toString());
				}
			}
		});

		tPool.submit(new Recurse(LFNUtils.getLFN(args[0])));

		try {
			while (!tPool.awaitTermination(20, TimeUnit.SECONDS)) {
				final int tCount = tPool.getActiveCount();
				final int qSize = tPool.getQueue().size();
				System.out.println("Awaiting completion of threads..." + tCount + " - " + qSize);
				if (tCount == 0 && qSize == 0) {
					tPool.shutdown();
					System.out.println("Shutdown executor");
				}
				if (tCount == 1 && qSize == 0) {
					count_thread_stuck++;
					if (count_thread_stuck == 5) {
						tPool.shutdownNow();
						System.out.println("Shutdown executor with 1-0: " + count_thread_stuck);
					}
				}
			}
		}
		catch (final InterruptedException e) {
			System.err.println("Something went wrong!: " + e);
		}

		System.out.println("Final count: " + global_count.toString());

		out.println("Final count: " + global_count.toString() + " - " + new Date());

		double ms_per_i = 0;
		final int cnt = timing_count.get();

		if (cnt > 0) {
			ms_per_i = ns_count.get() / (double) cnt;
			System.out.println("Final ns/i: " + ms_per_i);
			ms_per_i = ms_per_i / 1000000.;
		}
		else
			System.out.println("!!!!! Zero timing count !!!!!");

		double ms_per_i_dirs = 0;
		final int cnt_dirs = timing_count_dirs.get();

		if (cnt_dirs > 0) {
			ms_per_i_dirs = ns_count_dirs.get() / (double) cnt_dirs;
			System.out.println("Final ns/createDir: " + ms_per_i_dirs);
			ms_per_i_dirs = ms_per_i_dirs / 1000000.;
		}
		else
			System.out.println("!!!!! Zero timing count for dirs !!!!!");

		System.out.println("Final timing count: " + cnt);
		System.out.println("Final ms/i: " + ms_per_i);

		System.out.println("Final timing count dirs: " + cnt_dirs);
		System.out.println("Final ms/createDir: " + ms_per_i_dirs);

		out.println("Final timing count: " + cnt + " - " + new Date());
		out.println("Final ms/i: " + ms_per_i);
		out.println("Final timing count dirs: " + cnt_dirs + " - " + new Date());
		out.println("Final ms/createDir: " + ms_per_i_dirs);
		out.close();

		pw.close();
		failed_folders.close();
		failed_collections.close();
		failed_files.close();
		failed_ses.close();
		DBCassandra.shutdown();
	}

	/**
	 * auto-generated paths
	 *
	 * @param args
	 * @throws IOException
	 */
	public static void main_auto(final String[] args) throws IOException {
		final int nargs = args.length;

		if (nargs < 1) {
			System.err.println("Usage: ./run.sh alien/src/test/CatalogueToCassandraThreads <alien_path> [<pool_size>] [<logs_suffix>]");
			System.err.println("E.g. <base> -> 0");
			System.err.println("E.g. <limit> -> 1000");
			System.err.println("E.g. <alien_path> -> /alice/");
			System.err.println("E.g. <pool_size> -> 8");
			System.err.println("E.g. <logs-suffix> -> alice-md5-1B");
			System.err.println("E.g. <consistency> -> 1-one 2-quorum");
			System.exit(-3);
		}

		for (int i = 0; i < nargs; i++) {
			System.out.println("Parameter " + i + ": " + args[i]);
			System.out.flush();
		}

		final long base = Long.parseLong(args[0]);
		final long limitArg = Long.parseLong(args[1]);

		int pool_size = 16;
		if (nargs > 3)
			pool_size = Integer.parseInt(args[3]);
		System.out.println("Pool size: " + pool_size);
		tPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(pool_size);
		tPool.setKeepAliveTime(1, TimeUnit.MINUTES);
		tPool.allowCoreThreadTimeOut(true);

		if (nargs > 4)
			logs_suffix = "-" + args[4];

		final int consistency = Integer.parseInt(args[5]);
		if (consistency == 1)
			clevel = ConsistencyLevel.ONE;

		System.out.println("Printing output to: out" + logs_suffix);
		out = new PrintWriter(new FileOutputStream("out" + logs_suffix));
		out.println("Starting: " + new Date());
		out.flush();

		System.out.println("Going to insert new db consistency: " + clevel.toString() + " limit: " + limitArg + " in " + args[2] + " hierarchy. Time: " + new Date());
		out.println("Going to insert new db consistency: " + clevel.toString() + " limit: " + limitArg + " base: " + base + " in " + args[2] + " hierarchy. Time: " + new Date());
		out.flush();

		// Create LFN paths and submit them to create LFN_CSD to insert in
		// Cassandra
		int counter = 0;
		for (long i = base; i < limitArg; i++) {
			while (tPool.getQueue().size() > 300000) { // keep the pool queue size small
				try {
					Thread.sleep(3000);
				}
				catch (final InterruptedException e) {
					System.err.println("Cannot sleep in AddPath loop?!: " + e);
				}
			}
			tPool.submit(new AddPath(i));
			counter++;
			if (counter % 50000 == 0)
				System.out.println("Submitted " + counter + " tasks - queue size: " + tPool.getQueue().size());
		}

		try {
			while (!tPool.awaitTermination(20, TimeUnit.SECONDS)) {
				final int tCount = tPool.getActiveCount();
				final int qSize = tPool.getQueue().size();
				System.out.println("Awaiting completion of threads..." + tCount + " - " + qSize);
				if (tCount == 0 && qSize == 0) {
					tPool.shutdown();
					System.out.println("Shutdown executor");
				}
			}
		}
		catch (final InterruptedException e) {
			System.err.println("Something went wrong!: " + e);
		}

		double ms_per_i = 0;
		final int cnt = timing_count.get();

		if (cnt > 0) {
			ms_per_i = ns_count.get() / (double) cnt;
			System.out.println("Final ns/i: " + ms_per_i);
			ms_per_i = ms_per_i / 1000000.;
		}
		else
			System.out.println("!!!!! Zero timing count !!!!!");

		System.out.println("Final timing count: " + cnt);
		System.out.println("Final ms/i: " + ms_per_i);

		out.println("Final timing count: " + cnt + " - " + new Date());
		out.println("Final ms/i: " + ms_per_i);
		out.close();
		DBCassandra.shutdown();
	}

	private static class AddPath implements Runnable {
		final long root;

		public AddPath(final long r) {
			this.root = r;
		}

		@Override
		public void run() {
			final long last_part = root % 10000;
			final long left = root / 10000;
			final long medium_part = left % 100;
			final long first_part = left / 100;
			final String lfnparent = "/cassandra/" + first_part + "/" + medium_part + "/" + last_part + "/";

			boolean created = false;
			for (int i = 0; i < 3; i++) {
				try {
					if (LFN_CSD.createDirectory(lfnparent, "_auto", clevel)) {
						created = true;
						break;
					}
					Thread.sleep(500);
				}
				catch (final Exception e) {
					System.out.println("There was a timeout/exception on the createDirectory level: " + lfnparent + " Exception: " + e);
				}
			}

			if (!created) {
				out.println("Cannot create dir in AddPath after 3 tries: " + lfnparent);
				return;
			}

			for (int i = 1; i <= 10; i++) {
				final String lfn = "file" + i + "_" + root; // lfnparent +

				final int counted = global_count.incrementAndGet();
				if (counted % 5000 == 0) {
					out.println("LFN: " + lfnparent + lfn + " Estimation: " + (ns_count.get() / (double) counted) / 1000000. + " - Count: " + counted + " Time: " + new Date());
					out.flush();
				}

				final LFN_CSD lfnc = new LFN_CSD(lfnparent + lfn, false, "_auto", null, null);
				lfnc.size = ThreadLocalRandom.current().nextInt(100000);
				lfnc.jobid = ThreadLocalRandom.current().nextInt(1000000);
				lfnc.checksum = "ee31e454013aa515f0bc806aa907ba51";
				lfnc.perm = "755";
				lfnc.ctime = ctime_fixed;
				lfnc.owner = "aliprod";
				lfnc.gowner = "aliprod";
				lfnc.id = UUID.randomUUID();

				final HashMap<Integer, String> pfns = new HashMap<>();
				if (i % 2 == 0) {
					lfnc.type = 'f';
					pfns.put(Integer.valueOf(ThreadLocalRandom.current().nextInt(30)), "");
					pfns.put(Integer.valueOf(ThreadLocalRandom.current().nextInt(30)), "");
				}
				else {
					lfnc.type = 'l';
					pfns.put(Integer.valueOf(0), "/cassandra/0/100/10000");
				}
				lfnc.pfns = pfns;

				final HashMap<String, String> metadata = new HashMap<>();
				metadata.put("version", "v1");
				lfnc.metadata = metadata;

				// Insert into lfns_auto
				try (Timing t = new Timing()) {
					if (!lfnc.insert("_auto", clevel)) {
						final String msg = "Error inserting lfn: " + lfnc.getCanonicalName() + " Time: " + new Date();
						System.err.println(msg);
					}
					else {
						final long duration_ns = t.getNanos();
						ns_count.addAndGet(duration_ns);
						timing_count.incrementAndGet();
					}
				}
			}
		}

	}

	private static class Recurse implements Runnable {
		final LFN dir;

		public Recurse(final LFN folder) {
			this.dir = folder;
		}

		@Override
		public String toString() {
			return this.dir.getCanonicalName();
		}

		static final Comparator<LFN> comparator = (o1, o2) -> {
			if (o1.lfn.contains("archive") || o1.lfn.contains(".zip"))
				return -1;

			if (o2.lfn.contains("archive") || o2.lfn.contains(".zip"))
				return 1;

			return 0;
		};

		public static List<LFN> getZipMembers(final Map<GUID, LFN> whereis, final LFN lfn) {
			final List<LFN> members = new ArrayList<>();

			final String lfn_guid = lfn.guid.toString();
			final String lfn_guid_start = "guid:";

			for (final Map.Entry<GUID, LFN> entry : whereis.entrySet()) {
				final Set<PFN> pfns = entry.getKey().getPFNs();
				for (final PFN pf : pfns)
					if (pf.pfn.startsWith(lfn_guid_start) && pf.pfn.contains(lfn_guid))
						members.add(entry.getValue());
			}

			return members;
		}

		@Override
		public void run() {
			if (shouldexit)
				return;

			if (dir == null) {
				final String msg = "LFN DIR is null!";
				failed_folders.println(msg);
				failed_folders.flush();
				return;
			}

			// final DateFormat df = new
			// SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

			final int counted = global_count.get();

			if (counted >= limit.get()) {
				// out.println("LFN: " + dir.getCanonicalName() + " - Count: " +
				// counted + " Time: " + new Date());
				out.println("LFN: " + dir.getCanonicalName() + " Estimation: " + (ns_count.get() / (double) counted) / 1000000. + " - Count: " + counted + " Time: " + new Date());
				out.flush();
				limit.set(counted + origlimit);
			}

			// order the list
			final List<LFN> list = dir.list();

			if (list.isEmpty())
				return;

			// If folder not null or empty, we register threadId to dir
			final long threadId = Thread.currentThread().getId();
			activeThreadFolders.put(Long.valueOf(threadId), dir);
			// System.out.println("Thread "+threadId+" doing "+dir.lfn);

			// Sort the list by archive
			Collections.sort(list, comparator);

			// Files that will excluded since they are included in archives
			final Set<LFN> members_of_archives = new HashSet<>();

			// whereis of each file
			final Map<GUID, LFN> whereis = new HashMap<>();
			for (final LFN fi : list)
				if (fi.isFile()) {
					final GUID g = GUIDUtils.getGUID(fi);
					if (g == null) {
						failed_files.println("LFN is orphan!: " + fi.getCanonicalName());
						failed_files.flush();
						members_of_archives.add(fi); // Ignore files without guid
						continue;
					}

					if (g.getPFNs().size() == 0) {
						failed_files.println("LFN without pfns!: " + fi.getCanonicalName());
						failed_files.flush();
						members_of_archives.add(fi); // Ignore files without pfns
						continue;
					}

					whereis.put(g, fi);
				}

			for (final LFN l : list) {
				global_count.incrementAndGet();

				if (l.isDirectory()) {
					// insert the dir
					final LFN_CSD lfnc = new LFN_CSD(l, false, true);
					final long start = System.nanoTime();
					// if (!lfnc.insert(null, clevel)) {
					boolean created = false;
					for (int i = 0; i < 3; i++) {
						try {
							if (!LFN_CSD.createDirectory(l.getCanonicalName(), null, clevel, lfnc.owner, lfnc.gowner, lfnc.jobid, lfnc.perm, lfnc.ctime)) {
								final String msg = "Error inserting directory: " + l.getCanonicalName() + " Time: " + new Date();
								System.err.println(msg);
								failed_folders.println(msg);
								failed_folders.flush();
								continue;
							}
							created = true;
							break;
						}
						catch (final Exception e) {
							final String msg = "Exception inserting directory: " + l.getCanonicalName() + " Time: " + new Date() + " Exception: " + e;
							System.err.println(msg);
							failed_folders.println(msg);
							failed_folders.flush();
							continue;
						}
					}

					if (!created) {
						System.err.println("Cannot create dir in Recurse after 3 tries: " + l.getCanonicalName());
						out.println("Cannot create dir in Recurse after 3 tries: " + l.getCanonicalName());
						return;
					}

					final long duration_ns = System.nanoTime() - start;
					ns_count_dirs.addAndGet(duration_ns);
					timing_count_dirs.incrementAndGet();

					try {
						if (!shouldexit)
							tPool.submit(new Recurse(l));
					}
					catch (final RejectedExecutionException ree) {
						final String msg = "Interrupted directory: " + l.getCanonicalName() + " Parent: " + dir.getCanonicalName() + " Time: " + new Date() + " Message: " + ree.getMessage();
						System.err.println(msg);
						failed_folders.println(msg);
						failed_folders.flush();
						return;
					}
				}
				else if (l.isCollection()) {
					final LFN_CSD lfnc = new LFN_CSD(l, false, true);
					final long start = System.nanoTime();
					if (!lfnc.insert(null, clevel)) {
						final String msg = "Error inserting collection: " + l.getCanonicalName() + " Time: " + new Date();
						System.err.println(msg);
						failed_collections.println(msg);
						failed_collections.flush();
					}
					else {
						final long duration_ns = System.nanoTime() - start;
						ns_count.addAndGet(duration_ns);
						timing_count.incrementAndGet();
					}
				}
				else if (l.isFile()) {
					if (members_of_archives.contains(l))
						continue;

					final List<LFN> zip_members = getZipMembers(whereis, l);
					final boolean isArchive = !zip_members.isEmpty();
					final boolean isMember = zip_members.contains(l);

					// create json file in the hierarchy
					final LFN_CSD lfnc = new LFN_CSD(l, false, true);

					if (isArchive)
						lfnc.type = 'a';

					Set<PFN> pfns = null;
					// we have the pfns in the map
					for (final Map.Entry<GUID, LFN> entry : whereis.entrySet())
						if (entry.getValue().equals(l))
							pfns = entry.getKey().getPFNs();

					if (pfns != null) {
						final HashMap<Integer, String> pfnset = new HashMap<>();
						if (isMember) {
							lfnc.type = 'm';
							pfnset.put(Integer.valueOf(0), l.getCanonicalName());
						}
						else
							for (final PFN p : pfns) {
								final int se = p.seNumber;
								if (se <= 0) {
									failed_ses.println("SE null: " + p.seNumber + " - " + p.pfn);
									failed_ses.flush();
									continue;
								}
								pfnset.put(Integer.valueOf(se), p.getPFN());
							}
						lfnc.pfns = pfnset;
					}

					try (Timing timing = new Timing(monitor, "ms_insert_cassandra")) {
						if (!lfnc.insert(null, clevel)) {
							final String msg = "Error inserting file: " + l.getCanonicalName() + " Time: " + new Date();
							System.err.println(msg);
							failed_files.println(msg);
							failed_files.flush();
						}
						else {
							timing.endTiming();

							final long duration_ns = timing.getNanos();
							ns_count.addAndGet(duration_ns);
							timing_count.incrementAndGet();
						}
					}
				}
			}
			// Remove from list
			activeThreadFolders.remove(Long.valueOf(threadId));
		}
	}

}
