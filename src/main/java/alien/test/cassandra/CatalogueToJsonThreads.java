package alien.test.cassandra;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.PFN;
import alien.test.JobDiscoverer;
import lazyj.commands.SystemCommand;

/**
 *
 */
public class CatalogueToJsonThreads {

	/** Array of thread-dir */
	static final HashMap<Long, LFN> activeThreadFolders = new HashMap<>();

	/** Thread pool */
	static ThreadPoolExecutor tPool = null;

	/**
	 * limit
	 */
	static final int origlimit = 5000000;
	/** Entries processed */
	static AtomicInteger global_count = new AtomicInteger();
	/**
	 * Limit number of entries
	 */
	static AtomicInteger limit = new AtomicInteger(origlimit);

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

	/**
	 * Suffix
	 */
	static String suffix = "/catalogue";

	/**
	 * Method to check readability of JSON created files
	 *
	 * @param args
	 * @throws IOException
	 */
	public static void checkJsonReadable(final String[] args) throws IOException {
		if (args.length == 0 || args[0] == null) {
			System.err.println("You need to pass an AliEn path to a file as first argument.");
			System.exit(-3);
		}

		final JSONParser parser = new JSONParser();

		try (FileReader reader = new FileReader(args[0])) {
			final Object obj = parser.parse(reader);

			final JSONObject jsonObject = (JSONObject) obj;

			final String guid = (String) jsonObject.get("guid");
			System.out.println(guid);

			final String ctime = (String) jsonObject.get("ctime");
			System.out.println(ctime);

			// loop array
			// JSONArray mem = (JSONArray) jsonObject.get("zip_members");
			//
			// for (int i = 0; i < mem.size(); i++) {
			// JSONObject zipmem = (JSONObject) mem.get(i);
			//
			// String lfn = (String) zipmem.get("lfn");
			// System.out.println(lfn);
			//
			// String md5 = (String) zipmem.get("md5");
			// System.out.println(md5);
			//
			// String size = (String) zipmem.get("size");
			// System.out.println(size);
			// }

			//
			// JSONArray colmembers = (JSONArray) jsonObject.get("lfns");
			// if (colmembers != null && colmembers.size()>0) {
			// for (int i = 0; i < colmembers.size(); i++) {
			// String lfn = (String) colmembers.get(i);
			// System.out.println(lfn);
			// }
			// }

		}
		catch (final Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(final String[] args) throws IOException {
		final int nargs = args.length;

		if (nargs < 1) {
			System.err.println("Usage: ./run.sh alien/src/test/CatalogueToJsonThreads <alien_path> [<pool_size>] [<logs_suffix>]");
			System.err.println("E.g. <alien_path> -> /alice/sim/2016/");
			System.err.println("E.g. <pool_size> -> 8");
			System.err.println("E.g. <logs-suffix> -> alice-sim-2016");
			System.exit(-3);
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

		System.out.println("Printing threads to used_threads" + logs_suffix);
		used_threads = new PrintWriter(new FileOutputStream("used_threads" + logs_suffix));
		used_threads.println(logs_suffix + " - " + pool_size);
		used_threads.close();

		System.out.println("Going to create " + args[0] + " hierarchy. Time: " + new Date());
		System.out.println(SystemCommand.bash("df -h .").stdout);
		System.out.println(SystemCommand.bash("df -hi .").stdout);

		try (Scanner reader = new Scanner(System.in)) {
			// create directories
			final File f = new File(suffix + args[0]);
			if (f.exists()) {
				System.out.println("!!!!! Base folder already exists. Continue? (default yes) !!!!!");
				final String cont = reader.next();
				if (cont.equals("no")) {
					System.err.println("User stopping on base directory");
					System.exit(-1);
				}
			}
			else if (!f.mkdirs()) {
				System.err.println("Error creating base directory");
				System.exit(-1);
			}
		}

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
			}
		}
		catch (final InterruptedException e) {
			System.err.println("Something went wrong!: " + e);
		}

		System.out.println("Final count: " + global_count.toString());

		out.println("Final count: " + global_count.toString() + " - " + new Date());
		out.close();
		pw.close();
		failed_folders.close();
		failed_collections.close();
		failed_files.close();
		failed_ses.close();

		// Utils.getOutput("tar -cvzf ~/alien_folder.tar.gz folders alice");
		// Utils.getOutput("rm -rf folders alice");
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
		@SuppressWarnings({ "unchecked", "null" })
		public void run() {
			if (dir == null) {
				final String msg = "LFN DIR is null!";
				failed_folders.println(msg);
				failed_folders.flush();
				return;
			}

			final DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

			final int counted = global_count.get();
			if (global_count.get() >= limit.get()) {
				out.println("LFN: " + dir.getCanonicalName() + " - Count: " + counted + " Time: " + new Date());
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

				// if(Thread.currentThread().isInterrupted()){
				// String msg = "Thead "+threadId+" interrupted on: "+dir.lfn;
				// System.out.println(msg);
				// failed_folders.println(msg);
				// failed_folders.flush();
				// }

				if (l.isDirectory()) {
					// create the dir
					final File f = new File(suffix + l.getCanonicalName());
					if (!f.mkdirs()) {
						final String msg = "Error creating directory: " + l.getCanonicalName() + " Time: " + new Date();
						System.err.println(msg);
						failed_folders.println(msg);
						failed_folders.flush();
						continue;
					}

					pw.println(l.getCanonicalName() + "\t" + l.getOwner() + ":" + l.getGroup() + "\t" + l.getPermissions() + "\t" + df.format(l.ctime));
					pw.flush();

					try {
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
					final JSONObject lfnfile = new JSONObject();
					lfnfile.put("size", String.valueOf(l.size));
					lfnfile.put("owner", l.getOwner());
					lfnfile.put("gowner", l.getGroup());
					lfnfile.put("perm", l.getPermissions());
					lfnfile.put("ctime", df.format(l.ctime));
					lfnfile.put("jobid", String.valueOf(l.jobid));
					lfnfile.put("guid", l.guid.toString());

					final JSONArray filesjson = new JSONArray();
					for (final String s : l.listCollection())
						filesjson.add(s);
					lfnfile.put("lfns", filesjson);

					try (FileWriter file = new FileWriter(suffix + l.getCanonicalName())) {
						String myJsonString = lfnfile.toJSONString();
						myJsonString = myJsonString.replaceAll("\\\\", "");
						myJsonString = myJsonString.replaceAll(",", ",\n");
						file.write(myJsonString);
					}
					catch (final IOException e) {
						final String msg = "Can't create LFN collection: " + l.getCanonicalName() + " Time: " + new Date() + " Message: " + e.getMessage();
						failed_collections.println(msg);
						failed_collections.flush();
					}
				}
				else if (l.isFile()) {

					if (members_of_archives.contains(l))
						continue;

					final List<LFN> zip_members = getZipMembers(whereis, l);
					final boolean isArchive = zip_members != null && !zip_members.isEmpty();

					// create json file in the hierarchy
					final JSONObject lfnfile = new JSONObject();
					lfnfile.put("size", String.valueOf(l.size));
					lfnfile.put("owner", l.getOwner());
					lfnfile.put("gowner", l.getGroup());
					lfnfile.put("perm", String.valueOf(l.getPermissions()));
					lfnfile.put("ctime", df.format(l.ctime));
					lfnfile.put("jobid", String.valueOf(l.jobid));
					lfnfile.put("guid", String.valueOf(l.guid));
					lfnfile.put("md5", l.md5);

					Set<PFN> pfns = null;
					// we have the pfns in the map
					for (final Map.Entry<GUID, LFN> entry : whereis.entrySet())
						if (entry.getValue().equals(l))
							pfns = entry.getKey().getPFNs();

					if (pfns != null) {
						final JSONArray pfnsjson = new JSONArray();
						for (final PFN p : pfns) {
							final JSONObject pfn = new JSONObject();
							final String se = p.getSE().getName();
							if (se == null) {
								failed_ses.println("SE null: " + p.seNumber + " - " + p.pfn);
								failed_ses.flush();
								continue;
							}
							pfn.put("se", se);
							pfn.put("pfn", p.pfn);
							pfnsjson.add(pfn);
						}
						lfnfile.put("pfns", pfnsjson);
					}

					if (isArchive) {
						final JSONArray members = new JSONArray();
						// we have an archive, we create ln per member
						for (final LFN lfn_in_zip : zip_members) {
							members_of_archives.add(lfn_in_zip);
							final JSONObject entry = new JSONObject();
							entry.put("lfn", lfn_in_zip.getFileName());
							entry.put("size", String.valueOf(lfn_in_zip.size));
							entry.put("md5", lfn_in_zip.md5);
							members.add(entry);
						}
						lfnfile.put("zip_members", members);
					}

					try (FileWriter file = new FileWriter(suffix + l.getCanonicalName())) {
						String myJsonString = lfnfile.toJSONString();
						myJsonString = myJsonString.replaceAll("\\\\", "");
						myJsonString = myJsonString.replaceAll(",", ",\n");
						file.write(myJsonString);

						if (isArchive)
							for (final LFN lfn_in_zip : zip_members)
								Files.createSymbolicLink(Paths.get(suffix + lfn_in_zip.getCanonicalName()), Paths.get(l.getFileName()));
					}
					catch (final IOException e) {
						final String msg = "Can't create LFN: " + l.getCanonicalName() + " Time: " + new Date() + " Exception: " + e;
						failed_files.println(msg);
						failed_files.flush();
					}
				}
			}
			// Remove from list
			activeThreadFolders.remove(Long.valueOf(threadId));
		}
	}

}
