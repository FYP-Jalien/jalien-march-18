package alien.test.cassandra;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.ServletException;

import org.apache.catalina.LifecycleException;

import com.datastax.driver.core.ConsistencyLevel;

import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.LFN_CSD;
import alien.catalogue.PFN;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import alien.test.cassandra.tomcat.EmbeddedTomcat;
import alien.test.cassandra.tomcat.servlet.LocalCache;

/**
 *
 */
public class CatalogueTestWhereisGenerated {
	/** Array of thread-dir */
	static final HashMap<Long, LFN> activeThreadFolders = new HashMap<>();

	/** Thread pool */
	static ThreadPoolExecutor tPool = null;

	/**
	 * Unique Ctime for auto insertion
	 */
	public static Date ctime_fixed = null;
	static {
		try {
			ctime_fixed = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss").parse("2017-01-01 00:00:00");
		} catch (ParseException e) {
			System.err.println(e);
			System.exit(-1);
		}
	}

	/** Entries processed */
	static AtomicLong global_count = new AtomicLong();
	/**
	 * Inserts
	 */
	static AtomicLong global_count_insert = new AtomicLong();
	/**
	 * Limit number of entries
	 */
	static long limit_count;

	/**
	 * Limit number of entries
	 */
	static AtomicLong timing_count = new AtomicLong();
	/**
	 * Insert timing
	 */
	static AtomicLong timing_count_insert = new AtomicLong();

	/**
	 * total milliseconds
	 */
	static AtomicLong ns_count = new AtomicLong();
	/**
	 * Insert total time
	 */
	static AtomicLong ns_count_insert = new AtomicLong();

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
	 * DB type: 0 MySQL 1 Cassandra
	 */
	static int type = 1;
	/**
	 * Suffix for column family name
	 */
	static String lfntable = "_auto";
	/**
	 * Random number generator
	 */
	static final Random rdm = new Random();
	/**
	 * Monitoring component
	 */
	static transient final Monitor monitor = MonitorFactory.getMonitor(CatalogueTestWhereisGenerated.class.getCanonicalName());

	/**
	 * Signal to stop
	 */
	static boolean limit_reached = false;

	/**
	 * Desired consistency level
	 */
	static ConsistencyLevel clevel = ConsistencyLevel.QUORUM;

	/**
	 * When to stop
	 */
	static boolean no_termination = true;

	/**
	 * Read to write ratio
	 */
	static Integer read_write_ratio = Integer.valueOf(0);

	/**
	 * Offset
	 */
	static Integer base_for_insert = Integer.valueOf(500000000);

	/**
	 * Tomcat server
	 */
	static EmbeddedTomcat tomcat = null;

	/**
	 * auto-generated paths
	 *
	 * @param args
	 * @throws IOException
	 */
	public static void main(final String[] args) throws IOException {
		final int nargs = args.length;

		if (nargs < 1) {
			System.err.println("Usage: ./run.sh alien/src/test/CatalogueTestWhereisGenerated <...>");
			System.err.println("E.g. <base> -> 0");
			System.err.println("E.g. <limit> -> 1000 (it goes over 1000*10) files");
			System.err.println("E.g. <stddev> -> for the gaussian distribution, e.g. 100000 ");
			System.err.println("E.g. <type> -> 0-MYSQL 1-CASSANDRA");
			System.err.println("E.g. <count limit> -> 0 for unlimited");
			System.err.println("E.g. <pool_size> -> 12");
			System.err.println("E.g. <logs-suffix> -> auto-whereis-5M");
			System.err.println("E.g. <read/write ratio 1/n> -> 10");
			System.err.println("E.g. <base for insert> -> 500000 (5B)");
			System.err.println("E.g. [<consistency> -> 1-one 2-quorum]");
			System.err.println("E.g. [<lfn_table> -> _auto]");
			System.exit(-3);
		}

		final long base = Long.parseLong(args[0]);
		final long limit = Long.parseLong(args[1]);
		final long stddev = Long.parseLong(args[2]);
		type = Integer.parseInt(args[3]);
		limit_count = Long.parseLong(args[4]);
		if (limit_count == 0)
			limit_count = Long.MAX_VALUE;

		int pool_size = 16;
		if (nargs > 3)
			pool_size = Integer.parseInt(args[5]);
		System.out.println("Pool size: " + pool_size);
		tPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(pool_size);
		tPool.setKeepAliveTime(1, TimeUnit.MINUTES);
		tPool.allowCoreThreadTimeOut(true);

		if (nargs > 4)
			logs_suffix = "-" + args[6];

		read_write_ratio = Integer.valueOf(args[7]);
		base_for_insert = Integer.valueOf(args[8]);

		if (nargs > 7) {
			final int cl = Integer.parseInt(args[9]);
			if (cl == 1)
				clevel = ConsistencyLevel.ONE;
		}

		if (nargs > 10)
			lfntable = args[10];

		startLocalCacheTomcat();

		System.out.println("Printing output to: out" + logs_suffix);
		out = new PrintWriter(new FileOutputStream("out" + logs_suffix));
		out.println("Starting: " + new Date());
		out.flush();

		System.out.println("Going to whereis autogenerated with gaussian stddev:" + stddev + " and consistency: " + clevel.toString() + " type: " + type + " limit_count: " + limit_count + " limit: "
				+ limit + "*10 in " + " hierarchy. Time: " + new Date());

		// Control-C catch
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				no_termination = false;
				tPool.shutdownNow();

				try {
					while (!tPool.awaitTermination(5, TimeUnit.SECONDS))
						System.out.println("Waiting for threads finishing..." + tPool.getActiveCount());
				} catch (final InterruptedException e) {
					System.err.println("Something went wrong in shutdown!: " + e);
				}

			}
		});

		// int limit_minus_base = (int) (limit - base);
		// // Create LFN paths and submit them
		// for (long i = base; i < limit; i++) {
		// long newValue = rdm.nextInt(limit_minus_base) + base;
		// tPool.submit(new AddPath(newValue));
		// }

		// Create LFN paths and submit them
		int counter = 0;
		int write_n = 0;
		int counter_write = 0;
		long limit_minus_base = limit - base;
		String type_last_op = "read";
		long last_value = 0;
		while (no_termination) {
			while (tPool.getQueue().size() > 300000) { // keep the pool queue size small
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					System.err.println("Cannot sleep in AddPath loop?!: " + e);
				}
			}

			if (read_write_ratio.intValue() != 0 && counter_write == read_write_ratio.intValue()) {
				tPool.submit(new AddPathInsert(base_for_insert.intValue() + write_n));
				write_n++;
				counter_write = 0;
				type_last_op = "write";
				last_value = base_for_insert.intValue() + write_n;
			}
			else {
				type_last_op = "read";
				long newValue = ((long) (rdm.nextGaussian() * stddev)) + (limit_minus_base / 2);
				if (newValue < base || newValue > limit)
					continue;
				last_value = newValue;
				tPool.submit(new AddPathRead(newValue));
			}
			counter++;
			counter_write++;
			if (counter % 10000 == 0)
				System.out.println("Submitted " + counter + " tasks - Last op: " + type_last_op + " Last value: " + last_value + " - queue size: " + tPool.getQueue().size() + " dirCache size: "
						+ LFN_CSD.dirCacheSize() + " - dirCache_get_hit: " + LFN_CSD.dirCacheGet() + " - dirCache_put: " + LFN_CSD.dirCachePut() + " - ratio:"
						+ (double) LFN_CSD.dirCacheGet() / (double) LFN_CSD.dirCachePut());
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
		} catch (final InterruptedException e) {
			System.err.println("Something went wrong!: " + e);
		}

		double ms_per_i = 0;
		final long cnt = timing_count.get();

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

	private static class AddPathRead implements Runnable {
		final long root;

		public AddPathRead(final long r) {
			this.root = r;
		}

		@SuppressWarnings("incomplete-switch")
		@Override
		public void run() {
			if (limit_reached)
				return;

			final long last_part = root % 10000;
			final long left = root / 10000;
			final long medium_part = left % 100;
			final long first_part = left / 100;
			final String lfnparent = "/cassandra/" + first_part + "/" + medium_part + "/" + last_part + "/";

			for (int i = 1; i <= 10; i++) {
				if (limit_reached)
					break;

				final String lfn = lfnparent + "file" + i + "_" + root;

				final long counted = global_count.incrementAndGet();
				if (counted % 5000 == 0) {
					out.println("LFN: " + lfn + " Estimation: " + (ns_count.get() / counted) / 1000000. + " - Count: " + counted + " Time: " + new Date());
					out.flush();
				}

				boolean error = false;
				final Timing timing = new Timing();
				switch (type) {
				case 0: // LFN
					final LFN temp = LFNUtils.getLFN(lfn);
					if (temp == null) {
						final String msg = "Failed to get lfn temp: " + lfn;
						failed_files.println(msg);
						failed_files.flush();
						error = true;
						break;
					}
					final Set<PFN> pfns = temp.whereis();
					if (pfns == null || pfns.isEmpty()) {
						final String msg = "Failed to get PFNS: " + lfn;
						failed_files.println(msg);
						failed_files.flush();
						error = true;
						break;
					}
					break;
				case 1: // LFN_CSD
					final LFN_CSD lfnc = new LFN_CSD(lfn, false, lfntable, null, null);
					final HashMap<Integer, String> pfnsc = lfnc.whereis(lfntable, clevel);
					if (pfnsc == null || pfnsc.isEmpty()) {
						final String msg = "Failed to get PFNS: " + lfn;
						failed_files.println(msg);
						failed_files.flush();
						System.err.println(msg);
						continue;
					}
					break;
				}

				if (error)
					continue;

				final long duration_ns = timing.getNanos();
				ns_count.addAndGet(duration_ns);
				final long counter2 = timing_count.incrementAndGet();

				if (monitor != null)
					monitor.addMeasurement("ms_whereis_cassandra", duration_ns / 1000000.);

				if (counter2 >= limit_count)
					limit_reached = true;
			}
		}
	}

	private static class AddPathInsert implements Runnable {
		final long root;

		public AddPathInsert(final long r) {
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
					if (LFN_CSD.createDirectory(lfnparent, lfntable, clevel)) {
						created = true;
						break;
					}
					Thread.sleep(500);
				} catch (Exception e) {
					System.out.println("There was a timeout/exception on the createDirectory level: " + lfnparent + " Exception: " + e);
				}
			}

			if (!created) {
				out.println("Cannot create dir in AddPath after 3 tries: " + lfnparent);
				return;
			}

			for (int i = 1; i <= 10; i++) {
				final String lfn = "file" + i + "_" + root; // lfnparent +

				final long counted = global_count_insert.incrementAndGet();
				if (counted % 5000 == 0) {
					out.println("LFN: " + lfnparent + lfn + " Estimation for insert: " + (ns_count_insert.get() / counted) / 1000000. + " - Count: " + counted + " Time: " + new Date());
					out.flush();
				}

				final LFN_CSD lfnc = new LFN_CSD(lfnparent + lfn, false, lfntable, null, null);
				lfnc.size = rdm.nextInt(100000);
				lfnc.jobid = rdm.nextInt(1000000);
				lfnc.checksum = "ee31e454013aa515f0bc806aa907ba51";
				lfnc.perm = "755";
				lfnc.ctime = ctime_fixed;
				lfnc.owner = "aliprod";
				lfnc.gowner = "aliprod";
				lfnc.id = UUID.randomUUID();

				final HashMap<Integer, String> pfns = new HashMap<>();
				if (i % 2 == 0) {
					lfnc.type = 'f';
					pfns.put(Integer.valueOf(rdm.nextInt(30)), "");
					pfns.put(Integer.valueOf(rdm.nextInt(30)), "");
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
				final long start = System.nanoTime();
				if (!lfnc.insert(lfntable, clevel)) {
					final String msg = "Error inserting lfn: " + lfnc.getCanonicalName() + " Time: " + new Date();
					System.err.println(msg);
				}
				else {
					final long duration_ns = System.nanoTime() - start;
					ns_count_insert.addAndGet(duration_ns);
					timing_count_insert.incrementAndGet();
				}
			}
		}

	}

	private static void startLocalCacheTomcat() {
		try {
			tomcat = new EmbeddedTomcat("*");
		} catch (final ServletException se) {
			System.err.println("Cannot create the Tomcat server: " + se.getMessage());
			return;
		}

		tomcat.addServlet(LocalCache.class.getName(), "/*");

		// Start the server
		try {
			tomcat.start();
		} catch (final LifecycleException le) {
			System.err.println("Cannot start the Tomcat server: " + le.getMessage());
			return;
		}

		System.out.println("Ready to accept HTTP calls on " + tomcat.getAddress() + ":" + tomcat.getPort());

		return;
	}

}
