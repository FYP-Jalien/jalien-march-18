/**
 *
 */
package utils.benchmark;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import alien.catalogue.LFN;
import alien.catalogue.access.AuthorizationFactory;
import alien.config.ConfigUtils;
import alien.io.IOUtils;
import alien.monitoring.Timing;
import alien.shell.commands.JAliEnCOMMander;
import alien.user.AliEnPrincipal;
import alien.user.UsersHelper;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import lazyj.Format;

/**
 * @author costing
 * @since May 6, 2021
 */
public class SEBenchmark {

	private static final int DEFAULT_THREADS = 8;
	private static final float DEFAULT_SIZE = 500;

	private static final AtomicInteger sequence = new AtomicInteger();

	private static File localFile;

	private static final AliEnPrincipal account = AuthorizationFactory.getDefaultUser();

	private static final AtomicLong uploadedSoFar = new AtomicLong();

	private static final AtomicLong downloadedSoFar = new AtomicLong();

	private static final AtomicInteger completed = new AtomicInteger();

	private static final AtomicInteger failed = new AtomicInteger();

	private static final AtomicInteger downloadedOk = new AtomicInteger();

	private static final AtomicInteger downloadedErr = new AtomicInteger();

	private static long startup;

	private static final Object lock = new Object();

	private static String fileNamePrefix = ConfigUtils.getLocalHostname();

	private static LinkedBlockingQueue<String> readBackFiles = null;

	private static int readBackThreads = 0;

	private static boolean keepWrittenFiles = false;

	private static volatile boolean shutdown = false;

	private static final Thread monitoringThread = new Thread() {
		{
			setDaemon(true);
		}

		@Override
		public void run() {
			while (true) {
				synchronized (lock) {
					try {
						lock.wait(1000 * 60);
					}
					catch (@SuppressWarnings("unused") final InterruptedException e) {
						return;
					}
				}

				final long dTime = System.currentTimeMillis() - startup;

				final double rate = (uploadedSoFar.longValue() * 1000. / dTime) / 1024 / 1024;

				System.err.print("So far " + completed + " files (" + Format.size(uploadedSoFar.longValue()) + ") have completed in " + Format.toInterval(dTime) + ", for an average rate of "
						+ (Format.point(rate) + " MB/s") + "; " + failed + " failures");

				if (downloadedSoFar.get() > 0) {
					final double dRate = downloadedSoFar.longValue() * 1000. / dTime / 1024 / 1024;

					System.err.println(
							", download stats: " + downloadedOk + " ok (" + Format.size(downloadedSoFar.longValue()) + "), " + downloadedErr + " errors, dw rate " + Format.point(dRate) + " MB/s");
				}

				System.err.println();
			}
		}
	};

	private static final boolean cleanupCatalogueFile(final String fileName) {
		if (fileName == null)
			return true;

		try {
			final JAliEnCOMMander cmd = JAliEnCOMMander.getInstance();

			final LFN l = cmd.c_api.getLFN(fileName, true);

			if (l.exists && !cmd.c_api.removeLFN(fileName, false, true)) {
				System.err.println("Cannot remove the previously existing file: " + fileName);
				return false;
			}

			return true;
		}
		catch (final Throwable t) {
			System.err.println("Caught exception cleaning up " + fileName + ": " + t.getMessage());
			t.printStackTrace();
			return false;
		}
	}

	private static final class ReadThread extends Thread {
		@Override
		public void run() {
			final JAliEnCOMMander commander = JAliEnCOMMander.getInstance();

			while (true) {
				try {
					final String s = readBackFiles.take();

					if (s.isBlank())
						break;

					final File fTemp = File.createTempFile("SEBenchmark", "download");

					fTemp.delete();

					try (Timing t = new Timing()) {
						if (commander.c_api.downloadFile(s, fTemp) != null) {
							System.err.println("Downloaded " + s + " in " + t);
							downloadedSoFar.addAndGet(fTemp.length());
							downloadedOk.incrementAndGet();
						}
						else {
							System.err.println("Failed to download " + s);
							downloadedErr.incrementAndGet();
						}
					}
					finally {
						fTemp.delete();

						if ((shutdown || !readBackFiles.offer(s)) && !keepWrittenFiles)
							cleanupCatalogueFile(s);
					}
				}
				catch (@SuppressWarnings("unused") final InterruptedException ie) {
					break;
				}
				catch (final IOException ioe) {
					downloadedErr.incrementAndGet();
					System.err.println("Exception downloading a file: " + ioe.getMessage());
				}
			}
		}
	}

	private static final class UploadThread extends Thread {
		private int iterations;
		private final int tNo;
		private final String seName;

		public UploadThread(final int iterations, final String seName) {
			this.iterations = iterations;
			this.seName = seName;
			tNo = sequence.incrementAndGet();

			setName("Upload #" + tNo);
		}

		@Override
		public void run() {
			final String basePath = UsersHelper.getHomeDir(account.getDefaultUser()) + "se_test_" + fileNamePrefix + "/" + tNo;

			int count = 0;

			JAliEnCOMMander.getInstance().c_api.createCatalogueDirectory(basePath, true);

			String testPath = null;

			do {
				if (testPath == null) {
					testPath = basePath + "/" + (++count) + "." + UUID.randomUUID().toString();
				}

				if (!cleanupCatalogueFile(testPath)) {
					try {
						sleep(1000);
					}
					catch (@SuppressWarnings("unused") final InterruptedException e) {
						break;
					}

					continue;
				}

				try (Timing timing = new Timing()) {
					final LFN target = IOUtils.upload(localFile, testPath, account, System.err, "-S", seName);

					if (target != null) {
						System.err.println("Thread " + tNo + " completed one upload in " + timing + " (" + Format.point(localFile.length() / timing.getSeconds() / 1024 / 1024) + " MB/s): "
								+ target.getCanonicalName());

						uploadedSoFar.addAndGet(localFile.length());
						completed.incrementAndGet();

						if (readBackThreads > 0) {
							if (!readBackFiles.offer(testPath) && !keepWrittenFiles) {
								System.err.println("Removing just uploaded file " + testPath);
								cleanupCatalogueFile(testPath);
							}

							testPath = null;
						}

						if (keepWrittenFiles)
							testPath = null;
					}
					else {
						System.err.println("Failed to upload to " + testPath);
						failed.incrementAndGet();
					}
				}
				catch (final IOException e) {
					System.err.println("Thread " + tNo + " failed to upload a file: " + e.getMessage());
					failed.incrementAndGet();
				}
				catch (final Throwable t) {
					System.err.println("Thread " + tNo + " caught an exception in the upload part: " + t.getMessage());
					failed.incrementAndGet();
				}
			} while (--iterations > 0);

			cleanupCatalogueFile(testPath);
		}
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(final String[] args) throws IOException {
		// System.setProperty("alien.io.protocols.TempFileManager.persistent.entries", "0");
		// System.setProperty("alien.io.protocols.TempFileManager.temp.entries", "0");

		final OptionParser parser = new OptionParser();

		parser.accepts("s").withRequiredArg();
		parser.accepts("j").withRequiredArg().ofType(Integer.class);
		parser.accepts("b").withRequiredArg().ofType(Float.class);
		parser.accepts("f").withRequiredArg();
		parser.accepts("i").withRequiredArg().ofType(Integer.class);
		parser.accepts("n").withRequiredArg();
		parser.accepts("r").withRequiredArg().ofType(Integer.class);
		parser.accepts("k");

		final OptionSet options = parser.parse(args);

		if (args.length == 0 || !options.has("s")) {
			System.err.println("Run it with: java " + SEBenchmark.class.getCanonicalName() + " [options]");
			System.err.println("\t-s <SE name>\t\t\t(required)");
			System.err.println("\t-j <threads>\t\t\t(optional, default " + DEFAULT_THREADS + ")");
			System.err.println("\t-i <iterations>\t\t\t(optional, default 1)");
			System.err.println("\t-b <file size, in MB>\t\t(optional, default " + DEFAULT_SIZE + ")");
			System.err.println("\t-f <file name>\t\t\t(optional, file to use, size to be extracted from it)");
			System.err.println("\t-n <catalogue name>\t\t(optional, default '" + fileNamePrefix + "')");
			System.err.println("\t-r <threads>\t\t(number of read back threads, optional, default 0)");
			System.err.println("\t-k\t\t(pass this flag to keep written files on disk)");

			return;
		}

		final int threads = options.has("j") ? ((Integer) options.valueOf("j")).intValue() : DEFAULT_THREADS;

		final boolean tempFile;

		if (options.has("f")) {
			localFile = new File((String) options.valueOf("f"));

			if (!localFile.exists() || !localFile.isFile() || !localFile.canRead()) {
				System.err.println("Cannot use the indicated file: " + localFile.getAbsolutePath());
				return;
			}

			tempFile = false;
		}
		else {
			long size = (long) ((options.has("b") ? ((Float) options.valueOf("b")).floatValue() : DEFAULT_SIZE) * 1024 * 1024);

			localFile = File.createTempFile("sebenchmark-", ".tmp");
			localFile.deleteOnExit();

			try (FileOutputStream fos = new FileOutputStream(localFile)) {
				final byte[] buffer = new byte[8192];

				while (size > 0) {
					ThreadLocalRandom.current().nextBytes(buffer);

					fos.write(buffer, 0, size < buffer.length ? (int) size : buffer.length);

					size -= buffer.length;
				}
			}

			tempFile = true;
		}

		final int iterations = options.has("i") ? ((Integer) options.valueOf("i")).intValue() : 1;

		final String seName = (String) options.valueOf("s");

		if (options.has("n"))
			fileNamePrefix = (String) options.valueOf("n");

		keepWrittenFiles = options.has("k");

		final List<UploadThread> tList = new ArrayList<>(threads);

		startup = System.currentTimeMillis();

		ConfigUtils.setApplicationName(SEBenchmark.class.getCanonicalName());

		for (int i = 0; i < threads; i++) {
			final UploadThread ut = new UploadThread(iterations, seName);
			ut.start();
			tList.add(ut);
		}

		final List<ReadThread> rList = new ArrayList<>();

		if (options.has("r")) {
			readBackThreads = ((Integer) options.valueOf("r")).intValue();

			for (int i = 0; i < readBackThreads; i++) {
				final ReadThread rt = new ReadThread();
				rt.start();
				rList.add(rt);
			}
		}

		readBackFiles = new LinkedBlockingQueue<>(readBackThreads);

		monitoringThread.start();

		for (final UploadThread ut : tList)
			try {
				ut.join();
			}
			catch (@SuppressWarnings("unused") final InterruptedException e) {
				// ignore
			}

		shutdown = true;

		for (int i = 0; i < readBackThreads; i++)
			try {
				readBackFiles.put("");
			}
			catch (@SuppressWarnings("unused") final InterruptedException e) {
				// ignore
			}

		for (final ReadThread rt : rList) {
			try {
				rt.join();
			}
			catch (@SuppressWarnings("unused") final InterruptedException ie) {
				// ignore
			}
		}

		synchronized (lock) {
			lock.notifyAll();
		}

		if (tempFile)
			localFile.delete();
	}

}
