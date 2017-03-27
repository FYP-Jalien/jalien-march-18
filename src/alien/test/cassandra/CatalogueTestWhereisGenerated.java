package alien.test.cassandra;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.datastax.driver.core.ConsistencyLevel;

import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.LFN_CSD;
import alien.catalogue.PFN;

/**
 *
 */
public class CatalogueTestWhereisGenerated {
	/** Array of thread-dir */
	static final HashMap<Long, LFN> activeThreadFolders = new HashMap<>();

	/** Thread pool */
	static ThreadPoolExecutor tPool = null;

	static boolean shouldexit = false;

	/** Entries processed */
	static AtomicLong global_count = new AtomicLong();
	/**
	 * Limit number of entries
	 */
	static long limit_count;

	/**
	 * Limit number of entries
	 */
	static AtomicLong timing_count = new AtomicLong();

	/**
	 * total milliseconds
	 */
	static AtomicLong ns_count = new AtomicLong();

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
	 * Log file
	 */
	static PrintWriter used_threads = null;
	/**
	 * Suffix for log files
	 */
	static String logs_suffix = "";

	static int type = 1;

	static String lfntable = null;

	static final Random rdm = new Random();

	/**
	 * Signal to stop
	 */
	static boolean limit_reached = false;

	static ConsistencyLevel clevel = ConsistencyLevel.QUORUM;

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
			System.err.println("E.g. <type> -> 0-MYSQL 1-CASSANDRA");
			System.err.println("E.g. <count limit> -> limit to count");
			System.err.println("E.g. <pool_size> -> 12");
			System.err.println("E.g. <logs-suffix> -> auto-whereis-5M");
			System.err.println("E.g. [<consistency> -> 1-one 2-quorum]");
			System.err.println("E.g. [<lfn_table> -> _auto]");
			System.exit(-3);
		}

		final long base = Long.parseLong(args[0]);
		final long limit = Long.parseLong(args[1]);
		type = Integer.parseInt(args[2]);
		limit_count = Long.parseLong(args[3]);

		int pool_size = 16;
		if (nargs > 2)
			pool_size = Integer.parseInt(args[4]);
		System.out.println("Pool size: " + pool_size);
		tPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(pool_size);
		tPool.setKeepAliveTime(1, TimeUnit.MINUTES);
		tPool.allowCoreThreadTimeOut(true);

		if (nargs > 3)
			logs_suffix = "-" + args[5];

		if (nargs > 6) {
			final int cl = Integer.parseInt(args[6]);
			if (cl == 1)
				clevel = ConsistencyLevel.ONE;
		}

		if (nargs > 7)
			lfntable = args[7];

		System.out.println("Printing output to: out" + logs_suffix);
		out = new PrintWriter(new FileOutputStream("out" + logs_suffix));
		out.println("Starting: " + new Date());
		out.flush();

		System.out.println("Printing threads to used_threads" + logs_suffix);
		used_threads = new PrintWriter(new FileOutputStream("used_threads" + logs_suffix));
		used_threads.println(logs_suffix + " - " + pool_size);
		used_threads.close();

		System.out.println("Going to whereis autogenerated consistency: " + clevel.toString() + " type: " + type + " limit_count: " + limit_count + " limit: " + limit + "*10 in "
				+ " hierarchy. Time: " + new Date());

		int limit_minus_base = (int) (limit - base);
		// Create LFN paths and submit them
		for (long i = base; i < limit; i++) {
			long newValue = rdm.nextInt(limit_minus_base) + base;
			tPool.submit(new AddPath(newValue));
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

	private static class AddPath implements Runnable {
		final long root;

		public AddPath(final long r) {
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
				final long start = System.nanoTime();
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
						continue;
					}
					break;
				}

				if (error)
					continue;

				final long duration_ns = System.nanoTime() - start;
				ns_count.addAndGet(duration_ns);
				final long counter2 = timing_count.incrementAndGet();

				if (counter2 >= limit_count)
					limit_reached = true;
			}
		}
	}

}
