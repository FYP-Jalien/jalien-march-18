package alien.test.cassandra;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.LFN_CSD;
import alien.catalogue.PFN;
import alien.monitoring.Timing;

/**
 * @author mmmartin
 */
public class CatalogueTestsWhereis {

	/** Thread pool */
	static ThreadPoolExecutor tPool = null;

	/** Entries processed */
	static AtomicInteger global_count = new AtomicInteger();
	/**
	 * how many entries to create
	 */
	static int limit = 1000000;
	/**
	 * total milliseconds
	 */
	static AtomicLong ns_count = new AtomicLong();

	/** File for tracking created folders */
	static PrintWriter out = null;
	/**
	 * tracking failed folder
	 */
	static PrintWriter failed_folders = null;
	/**
	 * Log file
	 */
	static PrintWriter failed_files = null;
	/**
	 * print activity
	 */
	static PrintWriter pw = null;
	/**
	 * Add this suffix to all files
	 */
	static String logs_suffix = "";

	/**
	 * Signal to stop
	 */
	static boolean limit_reached = false;

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(final String[] args) throws IOException {
		final int nargs = args.length;

		if (nargs < 1) {
			System.err.println("Usage: ./run.sh alien/src/test/CatalogueTestsWhereis <alien_path> <type> [<pool_size>] [<logs_suffix>] [<limit>]");
			System.err.println("E.g. <alien_path> -> /alice/data/2012/,/alice/data/2016/");
			System.err.println("E.g. <type> 0-DB 1-CASSANDRA");
			System.err.println("E.g. <pool_size> -> 8");
			System.err.println("E.g. <logs-suffix> -> alice-data-2016");
			System.err.println("E.g. <limit> -> 2000000");
			System.exit(-3);
		}

		int type = 0;
		if (nargs > 1)
			type = Integer.parseInt(args[1]);

		int pool_size = 16;
		if (nargs > 2)
			pool_size = Integer.parseInt(args[2]);
		System.out.println("Pool size: " + pool_size);
		tPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(pool_size);
		tPool.setKeepAliveTime(5, TimeUnit.SECONDS);
		tPool.allowCoreThreadTimeOut(true);

		if (nargs > 3)
			logs_suffix = "-" + args[3];

		if (nargs > 4)
			limit = Integer.parseInt(args[4]);

		System.out.println("Printing output to: test_out" + logs_suffix + "-" + pool_size + "t" + "-" + type);
		out = new PrintWriter(new FileOutputStream("test_out" + logs_suffix + "-" + pool_size + "t" + "-" + type));
		out.println("Starting: " + new Date());
		out.flush();

		System.out.println("Printing folders to: test_folders" + logs_suffix + "-" + pool_size + "t" + "-" + type);
		pw = new PrintWriter(new FileOutputStream("test_folders" + logs_suffix + "-" + pool_size + "t" + "-" + type));

		System.out.println("Printing failed folders to test_failed_folders" + logs_suffix + "-" + pool_size + "t" + "-" + type);
		failed_folders = new PrintWriter(new FileOutputStream("test_failed_folders" + logs_suffix + "-" + pool_size + "t" + "-" + type));

		System.out.println("Printing failed files to test_failed_files" + logs_suffix + "-" + pool_size + "t" + "-" + type);
		failed_files = new PrintWriter(new FileOutputStream("test_failed_files" + logs_suffix + "-" + pool_size + "t" + "-" + type));

		System.out.println("Going to loop over " + args[0] + " hierarchy. Time: " + new Date());

		// Control-C catch
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				tPool.shutdown();

				try {
					while (!tPool.awaitTermination(5, TimeUnit.SECONDS))
						System.out.println("Waiting for threads finishing..." + tPool.getActiveCount());
				}
				catch (final InterruptedException e) {
					System.err.println("Something went wrong in shutdown!: " + e);
				}

			}
		});

		if (type == 0) {
			System.out.println("Running as DB LFN");
			final String[] folders = args[0].split(",");
			for (final String s : folders) {
				System.out.println("Recursing: " + s);
				tPool.submit(new RecurseLFN(LFNUtils.getLFN(s)));
			}
		}
		else if (type == 1) {
			System.out.println("Running as Cassandra LFN");
			final String[] folders = args[0].split(",");
			for (final String s : folders) {
				System.out.println("Recursing: " + s);
				tPool.submit(new RecurseLFNCassandra(LFNUtils.getLFN(s)));
			}
		}

		try {
			while (!tPool.awaitTermination(10, TimeUnit.SECONDS)) {
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

		double ms_per_ls = 0;
		final int cnt = global_count.get();

		if (cnt > 0) {
			if (limit_reached)
				System.out.println("Limit reached!: " + limit);

			else
				System.out.println("Limit not reached!");

			ms_per_ls = ns_count.get() / (double) cnt;
			System.out.println("Final ns/w: " + ms_per_ls);
			ms_per_ls = ms_per_ls / 1000000.;
		}
		else
			System.out.println("!!!!! Zero count !!!!!");

		System.out.println("Final count: " + cnt);
		System.out.println("Final ms/w: " + ms_per_ls);

		out.println("Final count: " + cnt + " - " + new Date());
		out.println("Final ms/w: " + ms_per_ls);
		out.close();
		pw.close();
		failed_folders.close();
		failed_files.close();
		DBCassandra.shutdown();
	}

	private static class RecurseLFN implements Runnable {
		final LFN dir;

		public RecurseLFN(final LFN folder) {
			this.dir = folder;
		}

		@Override
		public String toString() {
			return this.dir.getCanonicalName();
		}

		@Override
		public void run() {
			if (dir == null) {
				final String msg = "LFN DIR is null!";
				failed_folders.println(msg);
				failed_folders.flush();
				return;
			}

			if (limit_reached)
				return;

			// pw.println(dir.getCanonicalName());
			// pw.flush();

			// DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			final List<LFN> list = dir.list();

			if (list.isEmpty())
				return;

			for (final LFN l : list) {
				if (limit_reached)
					break;

				if (l.isFile()) {
					try (Timing t = new Timing()) {
						// Add lfn again
						final LFN temp = LFNUtils.getLFN(l.getCanonicalName());
						if (temp == null) {
							final String msg = "Failed to get lfn temp: " + l.getCanonicalName();
							failed_files.println(msg);
							failed_files.flush();
							continue;
						}
						final Set<PFN> pfns = l.whereis();
						if (pfns == null || pfns.isEmpty()) {
							final String msg = "Failed to get PFNS: " + l.getCanonicalName();
							failed_files.println(msg);
							failed_files.flush();
							continue;
						}

						final long duration_ns = t.getNanos();

						ns_count.addAndGet(duration_ns);

						final int counter2 = global_count.incrementAndGet();
						if (counter2 >= limit)
							limit_reached = true;

						if (counter2 % 5000 == 0) {
							// out.println("LFN: " + dir.getCanonicalName() + " - Count: " + counter2 + " Time: " + new Date());
							out.println("LFN: " + dir.getCanonicalName() + " Estimation: " + (ns_count.get() / (double) counter2) / 1000000. + " - Count: " + counter2 + " Time: " + new Date());
							out.flush();
						}
					}
				}
				else if (l.isDirectory())
					try {
						if (!limit_reached)
							tPool.submit(new RecurseLFN(l));
					}
					catch (final RejectedExecutionException ree) {
						final String msg = "Interrupted directory: " + l.getCanonicalName() + " Parent: " + dir.getCanonicalName() + " Time: " + new Date() + " Message: " + ree.getMessage();
						System.err.println(msg);
						failed_folders.println(msg);
						failed_folders.flush();
						return;
					}
			}
		}
	}

	private static class RecurseLFNCassandra implements Runnable {
		final LFN dir;

		public RecurseLFNCassandra(final LFN folder) {
			this.dir = folder;
		}

		@Override
		public String toString() {
			return this.dir.getCanonicalName();
		}

		@Override
		public void run() {
			if (dir == null) {
				final String msg = "LFN DIR is null!";
				failed_folders.println(msg);
				failed_folders.flush();
				return;
			}

			if (limit_reached)
				return;

			// pw.println(dir.getCanonicalName());
			// pw.flush();

			// DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			final List<LFN> list = dir.list();

			if (list.isEmpty())
				return;

			for (final LFN l : list) {
				if (limit_reached)
					break;

				final LFN_CSD lfnc = new LFN_CSD(l);

				if (lfnc.isFile() || lfnc.isMemberOfArchive() || lfnc.isArchive()) {
					final long start = System.nanoTime();
					final HashMap<Integer, String> pfns = lfnc.whereis();
					if (pfns == null || pfns.isEmpty()) {
						final String msg = "Failed to get PFNS: " + l.getCanonicalName();
						failed_files.println(msg);
						failed_files.flush();
						continue;
					}
					final long duration_ns = System.nanoTime() - start;

					ns_count.addAndGet(duration_ns);

					final int counter2 = global_count.incrementAndGet();
					if (counter2 >= limit)
						limit_reached = true;

					if (counter2 % 2000 == 0) {
						out.println("LFN: " + dir.getCanonicalName() + " - Count: " + counter2 + " Time: " + new Date());
						out.flush();
					}

				}
				else if (lfnc.isDirectory())
					try {
						if (!limit_reached)
							tPool.submit(new RecurseLFNCassandra(l));
					}
					catch (final RejectedExecutionException ree) {
						final String msg = "Interrupted directory: " + l.getCanonicalName() + " Parent: " + dir.getCanonicalName() + " Time: " + new Date() + " Message: " + ree.getMessage();
						System.err.println(msg);
						failed_folders.println(msg);
						failed_folders.flush();
						return;
					}
			}
		}
	}

}
