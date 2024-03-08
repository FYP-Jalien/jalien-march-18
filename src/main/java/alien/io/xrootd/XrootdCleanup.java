package alien.io.xrootd;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.PFN;
import alien.catalogue.access.AccessTicket;
import alien.catalogue.access.AccessType;
import alien.catalogue.access.XrootDEnvelope;
import alien.io.protocols.Xrootd;
import alien.io.xrootd.envelopes.XrootDEnvelopeSigner;
import alien.se.SE;
import alien.se.SEUtils;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import lazyj.Format;

/**
 * @author costing
 *
 */
public class XrootdCleanup {
	/**
	 * Storage element we are working on
	 */
	final SE se;

	private final String server;

	private final AtomicLong sizeRemoved = new AtomicLong();
	private final AtomicLong sizeKept = new AtomicLong();
	private final AtomicLong sizeFailed = new AtomicLong();
	private final AtomicLong filesRemoved = new AtomicLong();
	private final AtomicLong filesKept = new AtomicLong();
	private final AtomicLong filesFailed = new AtomicLong();
	private final AtomicLong dirsSeen = new AtomicLong();

	/**
	 * How many items are currently in progress
	 */
	final AtomicInteger inProgress = new AtomicInteger(0);

	/**
	 * how many files were processed so far
	 */
	final AtomicInteger processed = new AtomicInteger(0);

	private final boolean dryRun;

	/**
	 * Check all GUID files in this storage by listing recursively its contents.
	 *
	 * @param sSE
	 * @param dryRun
	 * @param threads
	 */
	public XrootdCleanup(final String sSE, final boolean dryRun, final int threads) {
		this.dryRun = dryRun;
		this.XROOTD_THREADS = threads;

		se = SEUtils.getSE(sSE);

		if (se == null) {
			server = null;

			System.err.println("No such SE " + sSE);

			return;
		}

		String sBase = se.seioDaemons;

		if (sBase.startsWith("root://"))
			sBase = sBase.substring(7);

		server = sBase;

		pushDir("/");

		int progressCounter = 0;

		while (inProgress.intValue() > 0) {
			try {
				Thread.sleep(1000);
			}
			catch (@SuppressWarnings("unused") final InterruptedException ie) {
				// ignore
			}

			if ((++progressCounter) % 10 == 0)
				System.err.println("*** " + se.seName + " *** processed so far : " + processed.get() + ", " + inProgress.get() + " are queued, " + toString());
		}

		for (final CleanupThread t : workers) {
			t.signalStop();
			t.interrupt();
		}
	}

	/**
	 * processing queue
	 */
	LinkedBlockingQueue<String> processingQueue = null;

	private static final int XROOTD_THREADS_DEFAULT = 16;

	private int XROOTD_THREADS = XROOTD_THREADS_DEFAULT;

	/**
	 * parallel xrootd listing threads
	 */
	private List<CleanupThread> workers = null;

	/**
	 * Directories under parsing
	 */
	Set<String> activeSet = new HashSet<>();

	private class CleanupThread extends Thread {
		private boolean shouldRun = true;

		public CleanupThread() {
			setDaemon(true);
		}

		@Override
		public void run() {
			while (shouldRun)
				try {
					final String dir = processingQueue.take();

					if (dir != null) {
						setName(se.seName + dir);

						synchronized (activeSet) {
							activeSet.add(dir);
						}

						try {
							storageCleanup(dir);
						}
						finally {
							inProgress.decrementAndGet();

							synchronized (activeSet) {
								activeSet.remove(dir);
							}
						}
					}
				}
				catch (@SuppressWarnings("unused") final InterruptedException ie) {
					// ignore
				}
		}

		/**
		 * Tell it to stop
		 */
		public void signalStop() {
			shouldRun = false;
		}
	}

	private synchronized void pushDir(final String dir) {
		if (processingQueue == null) {
			processingQueue = new LinkedBlockingQueue<>();

			workers = new ArrayList<>(XROOTD_THREADS);

			for (int i = 0; i < XROOTD_THREADS; i++) {
				final CleanupThread t = new CleanupThread();
				t.start();

				workers.add(t);
			}
		}

		inProgress.incrementAndGet();

		if (!processingQueue.contains(dir)) {
			synchronized (activeSet) {
				if (activeSet.contains(dir))
					return;
			}

			processingQueue.offer(dir);
		}
	}

	/**
	 * @param path
	 */
	void storageCleanup(final String path) {
		System.err.println("storageCleanup: " + path);

		dirsSeen.incrementAndGet();

		try {
			final boolean setSE = se.getName().toLowerCase().contains("dcache");

			final XrootdListing listing = new XrootdListing(server, path, setSE ? se : null);

			for (final XrootdFile file : listing.getFiles())
				fileCheck(file);

			for (final XrootdFile dir : listing.getDirs())
				if (dir.path.matches("^/\\d{2}(/\\d{5})?$"))
					pushDir(dir.path);
		}
		catch (final IOException ioe) {
			System.err.println(ioe.getMessage());
			ioe.printStackTrace();
		}
	}

	private boolean removeFile(final XrootdFile file) {
		if (!dryRun)
			return removeFile(file, se);

		System.err.println("WOULD RM " + file);

		return true;
	}

	// B6B6EF58-4000-11E0-9CE5-001F29EB8B98
	private static final Pattern UUID_PATTERN = Pattern.compile(".*([0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}).*");

	/**
	 * @param file
	 * @param se
	 * @return true if the file was actually removed
	 */
	public static boolean removeFile(final XrootdFile file, final SE se) {
		System.err.println("RM " + file);

		final Matcher m = UUID_PATTERN.matcher(file.getName());

		final UUID uuid;

		if (m.matches())
			uuid = UUID.fromString(m.group(1));
		else
			uuid = GUIDUtils.generateTimeUUID();

		final GUID guid = GUIDUtils.getGUID(uuid, true);

		guid.size = file.size;
		guid.md5 = "130254d9540d6903fa6f0ab41a132361";

		final PFN pfn = new PFN(guid, se);

		pfn.pfn = se.generateProtocol() + file.path;

		final XrootDEnvelope env = new XrootDEnvelope(AccessType.DELETE, pfn);

		try {
			XrootDEnvelopeSigner.sealEnvelope(env);
		}
		catch (final GeneralSecurityException e) {
			e.printStackTrace();
			return false;
		}

		pfn.ticket = new AccessTicket(AccessType.DELETE, env);

		final Xrootd xrootd = new Xrootd();

		try {
			if (!xrootd.delete(pfn)) {
				System.err.println("Could not delete : " + pfn);

				return false;
			}

			return true;
		}
		catch (final IOException e) {
			e.printStackTrace();
		}

		return false;
	}

	private void fileCheck(final XrootdFile file) {
		try {
			if (System.currentTimeMillis() - file.date.getTime() < 1000 * 60 * 60 * 24)
				// ignore very recent files
				return;

			final UUID uuid;

			try {
				uuid = UUID.fromString(file.getName());
			}
			catch (@SuppressWarnings("unused") final Exception e) {
				// not an alien file name, ignore
				return;
			}

			final GUID guid = GUIDUtils.getGUID(uuid);

			boolean remove = false;

			if (guid == null)
				remove = true;
			else {
				final Set<PFN> pfns = guid.getPFNs();

				if (pfns == null || pfns.size() == 0)
					remove = true;
				else {
					boolean found = false;

					for (final PFN pfn : pfns)
						if (se.equals(pfn.getSE())) {
							found = true;
							break;
						}

					remove = !found;
				}
			}

			if (remove) {
				if (removeFile(file)) {
					sizeRemoved.addAndGet(file.size);
					filesRemoved.incrementAndGet();
				}
				else {
					sizeFailed.addAndGet(file.size);
					filesFailed.incrementAndGet();
				}
			}
			else {
				sizeKept.addAndGet(file.size);
				filesKept.incrementAndGet();
			}
		}
		catch (final Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		}

		processed.incrementAndGet();
	}

	@Override
	public String toString() {
		return "Removed " + filesRemoved + " files (" + Format.size(sizeRemoved.longValue()) + "), " + "failed to remove " + filesFailed + " (" + Format.size(sizeFailed.longValue()) + "), " + "kept "
				+ filesKept + " (" + Format.size(sizeKept.longValue()) + "), " + dirsSeen + " directories";
	}

	/**
	 * @param args
	 *            the only argument taken by this class is the name of the storage to be cleaned
	 * @throws IOException
	 */
	public static void main(final String[] args) throws IOException {
		final OptionParser parser = new OptionParser();

		parser.accepts("n", "Do not take any action (dry run)");
		parser.accepts("?", "Print this help");
		parser.accepts("t").withRequiredArg().describedAs("Parallel threads, default " + XROOTD_THREADS_DEFAULT).ofType(Integer.class);
		parser.accepts("a", "Run on all known SEs");

		final OptionSet options = parser.parse(args);

		if ((options.nonOptionArguments().size() == 0 && !options.has("a")) || options.has("?")) {
			parser.printHelpOn(System.out);
			return;
		}

		final boolean dryRun = options.has("n");

		System.err.println("Dry run : " + dryRun);

		final long lStart = System.currentTimeMillis();

		int threads = XROOTD_THREADS_DEFAULT;

		if (options.has("t") && options.hasArgument("t"))
			threads = ((Integer) options.valueOf("t")).intValue();

		System.err.println("Parallel threads per SE : " + threads);

		final List<String> ses = new LinkedList<>();

		if (options.has("a"))
			for (final SE se : SEUtils.getSEs(null))
				ses.add(se.getName());
		else
			for (final Object o : options.nonOptionArguments())
				ses.add(o.toString());

		for (final String se : ses) {
			final XrootdCleanup cleanup = new XrootdCleanup(se, dryRun, threads);
			System.err.println(cleanup + ", took " + Format.toInterval(System.currentTimeMillis() - lStart));
		}
	}

}
