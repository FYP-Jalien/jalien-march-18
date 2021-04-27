package alien.test.cassandra;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.UserDefinedFileAttributeView;
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

import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.PFN;
import alien.test.JobDiscoverer;
import lazyj.commands.SystemCommand;
import lia.util.Utils;

/**
 *
 */
public class CatalogueToCVMFSThreads {

	/**
	 * Array of thread-dir
	 */
	static final HashMap<Long, LFN> activeThreadFolders = new HashMap<>();

	/**
	 * Thread pool
	 */
	static ThreadPoolExecutor tPool = null;

	/**
	 * Entries processed
	 */
	static final int origlimit = 500000;
	/**
	 * Progress
	 */
	static AtomicInteger global_count = new AtomicInteger();
	/**
	 * Limit
	 */
	static AtomicInteger limit = new AtomicInteger(origlimit);

	/**
	 * File for tracking created folders
	 */
	static PrintWriter out = null;

	/**
	 * Writer
	 */
	static PrintWriter pw = null;
	/**
	 * Where to log failed folders
	 */
	static PrintWriter failed_folders = null;
	/**
	 * And failed files
	 */
	static PrintWriter failed_files = null;
	/**
	 * ... collections
	 */
	static PrintWriter failed_collections = null;
	/**
	 * ... ses
	 */
	static PrintWriter failed_ses = null;
	/**
	 * ... threads
	 */
	static PrintWriter used_threads = null;
	/**
	 * ?
	 */
	static String logs_suffix = "";

	/**
	 * ?
	 */
	static String suffix = "/cvmfs/alien-catalogue.cern.ch";

	/*
	 * private static void readAttributess(final String[] args) throws IOException {
	 * Path path = Paths.get("/cvmfs/alien-catalogue.cern.ch/alice/cern.ch/user/g/grigoras/bin/cpugather.sh");
	 * UserDefinedFileAttributeView view = Files.getFileAttributeView(path, UserDefinedFileAttributeView.class);
	 * String pfns = "pfns";
	 * ByteBuffer buffer = ByteBuffer.allocate(view.size(pfns));
	 * view.read(pfns, buffer);
	 * buffer.flip();
	 * String value = Charset.defaultCharset().decode(buffer).toString();
	 * System.out.println(value);
	 * }
	 */

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(final String[] args) throws IOException {
		final int nargs = args.length;

		if (nargs < 1) {
			System.err.println("Usage: ./run.sh alien/src/test/CatalogueToCVMFSThreads <alien_path> [<pool_size>] [<logs_suffix>]");
			System.err.println("E.g. <alien_path> -> /alice/sim/2016/");
			System.err.println("E.g. <pool_size> -> 8");
			System.err.println("E.g. <logs-suffix> -> alice-sim-2016");
			System.exit(-3);
		}

		int pool_size = 8;
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
		// System.out.println(Utils.getOutput("df -h ."));
		// System.out.println(Utils.getOutput("df -hi ."));

		// CVMFS transaction start
		System.out.println("CVMFS transaction: " + new Date());
		System.out.println(" " + SystemCommand.bash("cvmfs_server transaction").stdout);
		System.out.println("CVMFS transaction done: " + new Date());

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

				// CVMFS transaction abort
				Utils.getOutput("cvmfs_server abort");
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

		// CVMFS transaction end
		// System.out.println("CVMFS publish: "+new Date());
		// System.out.println(Utils.getOutput("cvmfs_server publish"));
		// System.out.println("CVMFS publish done: "+new Date());
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
					// Touch file
					try {
						Files.createFile(Paths.get(suffix + l.getCanonicalName()));
					}
					catch (final IOException e) {
						final String msg = "Can't create LFN collection: " + l.getCanonicalName() + " Time: " + new Date() + " Exception: " + e;
						failed_collections.println(msg);
						failed_collections.flush();
						return;
					}

					// Set xattrs
					final Path path = Paths.get(suffix + l.getCanonicalName());
					final UserDefinedFileAttributeView view = Files.getFileAttributeView(path, UserDefinedFileAttributeView.class);

					try (FileWriter file = new FileWriter(suffix + l.getCanonicalName())) {
						view.write("lfn", Charset.defaultCharset().encode(l.getCanonicalName()));
						view.write("size", Charset.defaultCharset().encode(String.valueOf(l.size)));
						view.write("owner", Charset.defaultCharset().encode(l.getOwner()));
						view.write("gowner", Charset.defaultCharset().encode(l.getGroup()));
						view.write("perm", Charset.defaultCharset().encode(l.getPermissions()));
						view.write("ctime", Charset.defaultCharset().encode(df.format(l.ctime)));
						view.write("jobid", Charset.defaultCharset().encode(String.valueOf(l.jobid)));
						view.write("guid", Charset.defaultCharset().encode(l.guid.toString()));

						final JSONArray filesjson = new JSONArray();
						for (final String s : l.listCollection())
							filesjson.add(s);

						String myJsonString = filesjson.toJSONString();
						myJsonString = myJsonString.replaceAll("\\\\", "");
						myJsonString = myJsonString.replaceAll(",", ",\n");
						file.write(myJsonString);
						// view.write("lfns", Charset.defaultCharset().encode(myJsonString));
					}
					catch (final IOException e) {
						final String msg1 = "Can't set attr or write lfns LFN collection: " + l.getCanonicalName() + " Time: " + new Date() + " Message: " + e.getMessage();
						failed_collections.println(msg1);
						failed_collections.flush();
						try {
							Files.delete(path);
						}
						catch (final IOException e1) {
							final String msg2 = "Can't delete failed LFN collection: " + l.getCanonicalName() + " Time: " + new Date() + " Exception: " + e1;
							failed_collections.println(msg2);
							failed_collections.flush();
						}
						return;
					}
				}
				else if (l.isFile()) {

					if (members_of_archives.contains(l))
						continue;

					final List<LFN> zip_members = getZipMembers(whereis, l);
					final boolean isArchive = zip_members != null && !zip_members.isEmpty();

					// Touch file
					try {
						Files.createFile(Paths.get(suffix + l.getCanonicalName()));
					}
					catch (final IOException e) {
						final String msg = "Can't create LFN: " + l.getCanonicalName() + " Time: " + new Date() + " Exception: " + e;
						failed_files.println(msg);
						failed_files.flush();
						return;
					}

					// Set xattrs
					final Path path = Paths.get(suffix + l.getCanonicalName());
					final UserDefinedFileAttributeView view = Files.getFileAttributeView(path, UserDefinedFileAttributeView.class);

					try {
						view.write("lfn", Charset.defaultCharset().encode(l.getCanonicalName()));
						view.write("size", Charset.defaultCharset().encode(String.valueOf(l.size)));
						view.write("owner", Charset.defaultCharset().encode(l.getOwner()));
						view.write("gowner", Charset.defaultCharset().encode(l.getGroup()));
						view.write("perm", Charset.defaultCharset().encode(String.valueOf(l.getPermissions())));
						view.write("ctime", Charset.defaultCharset().encode(df.format(l.ctime)));
						view.write("jobid", Charset.defaultCharset().encode(String.valueOf(l.jobid)));
						view.write("guid", Charset.defaultCharset().encode(String.valueOf(l.guid)));
						view.write("md5", Charset.defaultCharset().encode(l.md5));

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

							String myJsonString = pfnsjson.toJSONString();
							myJsonString = myJsonString.replaceAll("\\\\", "");
							myJsonString = myJsonString.replaceAll(",", ",\n");
							view.write("pfns", Charset.defaultCharset().encode(myJsonString));
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

							String myJsonString = members.toJSONString();
							myJsonString = myJsonString.replaceAll("\\\\", "");
							myJsonString = myJsonString.replaceAll(",", ",\n");
							view.write("zip_members", Charset.defaultCharset().encode(myJsonString));

							for (final LFN lfn_in_zip : zip_members)
								Files.createSymbolicLink(Paths.get(suffix + lfn_in_zip.getCanonicalName()), Paths.get(l.getFileName()));
						}
					}
					catch (final IOException e) {
						final String msg = "Can't set attr LFN: " + l.getCanonicalName() + " Time: " + new Date() + " Exception: " + e;
						failed_files.println(msg);
						failed_files.flush();
						try {
							Files.delete(path);
						}
						catch (final IOException e1) {
							final String msg2 = "Can't delete failed LFN: " + l.getCanonicalName() + " Time: " + new Date() + " Exception: " + e1;
							failed_files.println(msg2);
							failed_files.flush();
						}
					}
				}
			}
			// Remove from list
			activeThreadFolders.remove(Long.valueOf(threadId));
		}
	}

}
