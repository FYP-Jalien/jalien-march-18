/**
 *
 */
package utils.benchmark;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import alien.catalogue.LFN;
import alien.catalogue.access.AuthorizationFactory;
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

	private static final AtomicInteger completed = new AtomicInteger();

	private static long startup;

	private static final Object lock = new Object();

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

				System.err.println("So far " + completed + " files (" + Format.size(uploadedSoFar.longValue()) + ") have completed in " + Format.toInterval(dTime) + ", for an average rate of "
						+ (Format.point(rate) + " MB/s"));
			}
		}
	};

	private static final boolean cleanupCatalogueFile(final String fileName) {
		final JAliEnCOMMander cmd = JAliEnCOMMander.getInstance();

		final LFN l = cmd.c_api.getLFN(fileName, true);

		if (l.exists && !cmd.c_api.removeLFN(fileName, false, true)) {
			System.err.println("Cannot remove the previously existing file: " + fileName);
			return false;
		}

		return true;
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
			final String testPath = UsersHelper.getHomeDir(account.getDefaultUser()) + "/se_test_" + tNo;

			do {
				if (!cleanupCatalogueFile(testPath))
					break;

				try (Timing timing = new Timing()) {
					LFN target = IOUtils.upload(localFile, testPath, account, null, "-S", seName);

					if (target != null) {
						System.err.println("Thread " + tNo + " completed one upload in " + timing + " (" + Format.point(localFile.length() / timing.getSeconds() / 1024 / 1024) + " MB/s)");

						uploadedSoFar.addAndGet(localFile.length());
						completed.incrementAndGet();
					}
					else
						System.err.println("Failed to upload to " + testPath);
				}
				catch (final IOException e) {
					System.err.println("Thread " + tNo + " failed to upload a file: " + e.getMessage());
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
		final OptionParser parser = new OptionParser();

		parser.accepts("s").withRequiredArg();
		parser.accepts("j").withRequiredArg().ofType(Integer.class);
		parser.accepts("b").withRequiredArg().ofType(Float.class);
		parser.accepts("f").withRequiredArg();
		parser.accepts("i").withRequiredArg().ofType(Integer.class);

		final OptionSet options = parser.parse(args);

		if (args.length == 0 || !options.has("s")) {
			System.err.println("Run it with: java " + SEBenchmark.class.getCanonicalName() + " [options]");
			System.err.println("\t-s <SE name>\t\t\t(required)");
			System.err.println("\t-j <threads>\t\t\t(optional, default " + DEFAULT_THREADS + ")");
			System.err.println("\t-i <iterations>\t\t\t(optional, default 1)");
			System.err.println("\t-b <file size, in MB>\t\t(optional, default " + DEFAULT_SIZE + ")");
			System.err.println("\t-f <file name>\t\t\t(optional, file to use, size to be extracted from it)");

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

		final List<UploadThread> tList = new ArrayList<>(threads);

		startup = System.currentTimeMillis();

		for (int i = 0; i < threads; i++) {
			final UploadThread ut = new UploadThread(iterations, seName);
			ut.start();
			tList.add(ut);
		}

		monitoringThread.start();

		for (final UploadThread ut : tList)
			try {
				ut.join();
			}
			catch (@SuppressWarnings("unused") final InterruptedException e) {
				// ignore
			}

		synchronized (lock) {
			lock.notifyAll();
		}

		if (tempFile)
			localFile.delete();
	}

}
