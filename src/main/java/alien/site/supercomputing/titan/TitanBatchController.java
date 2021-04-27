package alien.site.supercomputing.titan;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * @author psvirin
 *
 */
public class TitanBatchController {
	private HashMap<String, TitanBatchInfo> batchesInfo = new HashMap<>();
	private final String globalWorkdir;
	private final List<TitanJobStatus> idleRanks;

	private static final int minTtl = 300;
	/**
	 * TODO: document this
	 */
	public static HashMap<String, Object> siteMap = new HashMap<>();

	/**
	 * @param global_work_dir
	 */
	public TitanBatchController(final String global_work_dir) {
		if (global_work_dir == null)
			throw new IllegalArgumentException("No global workdir specified");
		globalWorkdir = global_work_dir;
		idleRanks = new LinkedList<>();
	}

	/**
	 * @return <code>true<code> if the update was successful
	 */
	public boolean updateDatabaseList() {
		// ls -d */ -1 | sed -e 's#/$##' | grep -E '^[0-9]+$' | sort -g
		// ProcessBuilder pb = newProcessBuilder(System.getProperty("user.dir")+"/src/generate_list.sh",filename);
		System.out.println("Starting database update");
		final ProcessBuilder pb = new ProcessBuilder("/bin/bash", "-c", "for i in $(ls -d " + globalWorkdir + "/*/ | egrep \"/[0-9]+/\"); do basename $i; done");
		final HashMap<String, TitanBatchInfo> tmpBatchesInfo = new HashMap<>();
		int dbcount = 0;
		try {
			final Process p = pb.start();
			try (BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
				String line = null;
				while ((line = reader.readLine()) != null)
					// if(batchesInfo.get(line) == null)
					try {
						final TitanBatchInfo bi = batchesInfo.get(line);
						if (bi == null)
							tmpBatchesInfo.put(line, new TitanBatchInfo(Long.valueOf(line), globalWorkdir + "/" + line));
						else
							tmpBatchesInfo.put(line, bi);
						dbcount++;
						System.out.println("Now controlling batch: " + line);
					}
					catch (@SuppressWarnings("unused") final InvalidParameterException e) {
						System.err.println("Not a batch folder at " + globalWorkdir + "/" + line + " , skipping....");
					}
					catch (final Exception e) {
						System.err.println(e.getMessage());
						System.err.println("Unable to initialize batch folder at " + globalWorkdir + "/" + line + " , skipping....");
					}
			}
			
			batchesInfo = tmpBatchesInfo;
		}
		catch (final IOException e) {
			System.err.println("Error running batch info reader process: " + e.getMessage());
		}
		catch (final Exception e) {
			System.err.println("Exception at database list update: " + e.getMessage());
		}
		System.out.println(String.format("Now controlling %d batches", Integer.valueOf(dbcount)));

		return !batchesInfo.isEmpty();
	}

	/**
	 * @return TODO
	 */
	public boolean queryDatabases() {
		idleRanks.clear();
		final long current_timestamp = System.currentTimeMillis() / 1000L;
		for (final Object o : batchesInfo.values()) {
			final TitanBatchInfo bi = (TitanBatchInfo) o;
			System.out.println("Querying: " + bi.pbsJobId);
			if (!checkBatchTtlValid(bi, current_timestamp))
				continue;
			if (!bi.isRunning()) {
				System.out.println("Batch " + bi.pbsJobId + " not running, cleaning up.");
				bi.cleanup();
				continue;
			}
			try {
				idleRanks.addAll(bi.getIdleRanks());
			}
			catch (final Exception e) {
				System.err.println("Exception caught in queryDatabases: " + e.getMessage());
				continue;
			}
		}
		return !idleRanks.isEmpty();
	}

	/**
	 * @return TODO
	 */
	public List<TitanJobStatus> queryRunningDatabases() {
		final List<TitanJobStatus> runningRanks = new LinkedList<>();
		final long current_timestamp = System.currentTimeMillis() / 1000L;
		for (final Object o : batchesInfo.values()) {
			final TitanBatchInfo bi = (TitanBatchInfo) o;
			if (!checkBatchTtlValid(bi, current_timestamp))
				continue;
			try {
				runningRanks.addAll(bi.getRunningRanks());
			}
			catch (@SuppressWarnings("unused") final Exception e) {
				continue;
			}
		}
		return runningRanks;
	}

	/**
	 * @return TODO
	 */
	public final List<ProcInfoPair> getBatchesMonitoringData() {
		final List<ProcInfoPair> l = new LinkedList<>();
		for (final Object o : batchesInfo.values()) {
			final TitanBatchInfo bi = (TitanBatchInfo) o;
			l.addAll(bi.getMonitoringData());
		}
		return l;
	}

	private static boolean checkBatchTtlValid(final TitanBatchInfo bi, final long current_timestamp) {
		// EXPERIMENTAL
		// return bi.getTtlLeft(current_timestamp) > minTtl;
		return bi.getTtlLeft(current_timestamp) > minTtl * 8;
	}

	/**
	 *
	 */
	public void runDataExchange() {
		// List<TitanJobStatus> idleRanks = queryDatabases();
		// for(TitanJobStatus)
		final int count = idleRanks.size();
		System.out.println(String.format("We can start %d jobs", Integer.valueOf(count)));

		if (count == 0)
			return;

		// create upload threads
		final ArrayList<Thread> upload_threads = new ArrayList<>();
		for (final TitanJobStatus js : idleRanks)
			if (js.status.equals("D")) {
				final JobUploader ju = new JobUploader(js);
				// ju.setDbName(dbname);
				upload_threads.add(ju);
				ju.start();
			}

		// join all threads
		for (final Thread t : upload_threads)
			try {
				t.join();
			}
			catch (@SuppressWarnings("unused") final InterruptedException e) {
				System.err.println("Join for upload thread has been interrupted");
			}

		System.out.println("Everything joined");
		System.out.println("================================================");

		// if(count>0) {
		// monitor.sendParameter("ja_status", getJaStatusForML("REQUESTING_JOB"));
		// monitor.sendParameter("TTL", siteMap.get("TTL"));
		// }

		upload_threads.clear();
		System.out.println("========= Starting download threads ==========");
		int cnt = idleRanks.size();
		final Date d1 = new Date();
		JobDownloader.initialize();
		for (final TitanJobStatus js : idleRanks) {
			// System.out.println(siteMap.toString());
			// TitanJobStatus js = idleRanks.pop();

			final JobDownloader jd = new JobDownloader(js, siteMap);
			// jd.setDbName(dbname);
			upload_threads.add(jd);
			jd.start();
			System.out.println("Starting downloader " + cnt--);
			if (cnt == 0)
				break;
			// count--;
			// System.out.println("Wants to start Downloader thread");
			// System.out.println(js.batch.origTtl);
			// System.out.println(js.batch.numCores);
		}

		System.out.println("Count: " + count);
		final Date d3 = new Date();

		// join all threads
		for (final Thread t : upload_threads)
			try {
				t.join();
				System.out.println("Joined downloader " + ++cnt);
			}
			catch (@SuppressWarnings("unused") final InterruptedException e) {
				System.err.println("Join for upload thread has been interrupted");
			}
		idleRanks.clear();
		final Date d2 = new Date();
		System.out.println("Everything joined");
		System.out.println("Downloading took: " + (d2.getTime() - d1.getTime()) / 1000 + " seconds");
		System.out.println("Created downloaders during: " + (d3.getTime() - d1.getTime()) / 1000 + " seconds");
		System.out.println("================================================");
	}
}
