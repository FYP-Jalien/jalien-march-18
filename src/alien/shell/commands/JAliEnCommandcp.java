package alien.shell.commands;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOError;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import alien.api.Dispatcher;
import alien.api.ServerException;
import alien.api.catalogue.PFNforWrite;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.config.ConfigUtils;
import alien.io.Transfer;
import alien.io.protocols.Protocol;
import alien.io.protocols.TempFileManager;
import alien.se.SE;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import utils.CachedThreadPool;

/**
 * @author ron
 * @since June 4, 2011
 */
public class JAliEnCommandcp extends JAliEnBaseCommand {

	/**
	 * If <code>true</code> then force a download operation
	 */
	private boolean bT = false;

	/**
	 * If <code>true</code> then wait for all uploads to finish before returning. Otherwise return after the first successful one, waiting for the remaining copies to be uploaded in background and
	 * committed asynchronously to the catalogue.
	 */
	private boolean bW = ConfigUtils.getConfig().getb("alien.shell.commands.cp.wait_for_all_uploads.default", false);

	/**
	 * If <code>true</code> then the source file is to be deleted after a successful upload (for temporary files)
	 */
	private boolean bD = false;

	/**
	 * Number of concurrent operations to do
	 */
	private int concurrentOperations = 1;

	private int referenceCount = 0;

	private final List<String> ses = new ArrayList<>();
	private final List<String> exses = new ArrayList<>();

	private final HashMap<String, Integer> qos = new HashMap<>();

	private String source = null;
	private String target = null;

	private File localFile = null;

	private long jobId = -1;

	// public long timingChallenge = 0;

	// public boolean isATimeChallenge = false;

	@Override
	public void run() {

		if (bT)
			localFile = copyGridToLocal(source, null);
		else
			if (out != null && out.isRootPrinter()) {
				out.nextResult();
				if (!localFileSpec(source) && localFileSpec(target)) {

					localFile = new File(getLocalFileSpec(target));

					if (!localFile.exists() || localFile.isDirectory())
						copyGridToLocal(source, localFile);
					else
						if (!isSilent())
							out.setField("message", "A local file already exists with this name.");
						else {
							final IOException ex = new IOException("A local file already exists with this name: " + target);

							throw new IOError(ex);
						}
				}
				else
					if (localFileSpec(source) && !localFileSpec(target)) {

						final File sourceFile = new File(getLocalFileSpec(source));
						if (!targetLFNExists(target))
							if (sourceFile.exists())
								copyLocalToGrid(sourceFile, target);
							else
								if (!isSilent())
									out.setField("message", "A local file with this name does not exists.");
								else {
									final IOException ex = new IOException("Local file " + target + " doesn't exist");

									throw new IOError(ex);
								}
					}
					else
						if (!targetLFNExists(target)) {

							localFile = copyGridToLocal(source, null);
							if (localFile != null && localFile.exists() && localFile.length() > 0)
								if (copyLocalToGrid(localFile, target))
									if (!isSilent())
										out.setField("message", "Copy successful.");
									else
										if (!isSilent())
											out.setField("message", "Could not copy to the target.");
										else {
											final IOException ex = new IOException("Could not copy to the target: " + target);

											throw new IOError(ex);
										}

								else
									if (!isSilent())
										out.setField("message", "Could not get the source.");
									else {
										final IOException ex = new IOException("Could not get the source: " + source);

										throw new IOError(ex);
									}
						}
			}
			else
				if (!localFileSpec(source) && localFileSpec(target)) {

					localFile = new File(getLocalFileSpec(target));

					if (!localFile.exists() || localFile.isDirectory())
						copyGridToLocal(source, localFile);
					else
						if (!isSilent())
							out.printErrln("A local file already exists with this name.");
						else {
							final IOException ex = new IOException("A local file already exists with this name: " + target);

							throw new IOError(ex);
						}

				}
				else
					if (localFileSpec(source) && !localFileSpec(target)) {

						final File sourceFile = new File(getLocalFileSpec(source));
						if (!targetLFNExists(target))
							if (sourceFile.exists())
								copyLocalToGrid(sourceFile, target);
							else
								if (!isSilent())
									out.printErrln("A local file with this name does not exists.");
								else {
									final IOException ex = new IOException("Local file " + target + " doesn't exist");

									throw new IOError(ex);
								}

					}
					else
						if (!targetLFNExists(target)) {

							localFile = copyGridToLocal(source, null);
							if (localFile != null && localFile.exists() && localFile.length() > 0)
								if (copyLocalToGrid(localFile, target))
									if (!isSilent())
										out.printOutln("Copy successful.");
									else
										if (!isSilent())
											out.printErrln("Could not copy to the target.");
										else {
											final IOException ex = new IOException("Could not copy to the target: " + target);

											throw new IOError(ex);
										}

								else
									if (!isSilent())
										out.printErrln("Could not get the source.");
									else {
										final IOException ex = new IOException("Could not get the source: " + source);

										throw new IOError(ex);
									}

						}
	}

	/**
	 * @return local File after get/pull
	 */
	protected File getOutputFile() {
		return localFile;
	}

	/**
	 * @author ron
	 * @since October 5, 2011
	 */
	private static final class ProtocolAction extends Thread {
		private final Protocol proto;
		private final File file;
		private final PFN pfn;
		private File output = null;

		private Exception lastException = null;

		/**
		 * @param protocol
		 * @param source
		 * @param target
		 */
		ProtocolAction(final Protocol protocol, final PFN source, final File target) {
			proto = protocol;
			file = target;
			pfn = source;
		}

		@Override
		public void run() {
			try {
				if (pfn.ticket != null && pfn.ticket.envelope != null && pfn.ticket.envelope.getArchiveAnchor() != null) {
					final File tempLocalFile = proto.get(pfn, null);

					if (tempLocalFile != null) {
						final LFN archiveMember = pfn.ticket.envelope.getArchiveAnchor();

						final String archiveFileName = archiveMember.getFileName();

						try (ZipInputStream zi = new ZipInputStream(new FileInputStream(tempLocalFile))) {
							ZipEntry zipentry;

							while ((zipentry = zi.getNextEntry()) != null) {
								if (zipentry.getName().equals(archiveFileName)) {
									final FileOutputStream fos = new FileOutputStream(file);

									final byte[] buf = new byte[8192];

									int n;

									while ((n = zi.read(buf, 0, buf.length)) > -1)
										fos.write(buf, 0, n);

									fos.close();
									zi.closeEntry();

									output = file;

									TempFileManager.putPersistent(pfn.getGuid(), output);

									break;
								}
							}
						} finally {
							TempFileManager.release(tempLocalFile);
						}
					}
				}
				else
					output = proto.get(pfn, file);
			} catch (final IOException e) {
				lastException = e;
				output = null;
			}
		}

		/**
		 * @return local output file
		 */
		public File getFile() {
			return output;
		}

		/**
		 * @return last execution exception, if any
		 */
		public Exception getLastException() {
			return lastException;
		}
	}

	private boolean targetLFNExists(final String targetLFN) {
		final LFN currentDir = commander.getCurrentDir();

		final LFN tLFN = commander.c_api.getLFN(FileSystemUtils.getAbsolutePath(commander.user.getName(), currentDir != null ? currentDir.getCanonicalName() : null, targetLFN));

		if (tLFN != null) {
			if (!isSilent())
				out.printErrln("The target LFN already exists.");

			return true;
		}
		return false;
	}

	private class GridToLocal implements Runnable {
		private final String sourcelfn;
		private final String longestMatchingPath;
		private final File targetLocalFile;
		private File resultFile = null;

		public GridToLocal(final String sourcelfn, final String longestMatchingPath, final File targetLocalFile) {
			this.sourcelfn = sourcelfn;
			this.longestMatchingPath = longestMatchingPath;
			this.targetLocalFile = targetLocalFile;
		}

		@SuppressWarnings("synthetic-access")
		@Override
		public void run() {
			final LFN lfn = commander.c_api.getLFN(sourcelfn);

			if (lfn == null) {
				if (!isSilent())
					out.printErrln("Could not get the file's LFN: " + sourcelfn);

				return;
			}

			if (!lfn.isFile()) {
				// ignoring anything else but files
				return;
			}

			File writeToLocalFile = targetLocalFile;

			if (targetLocalFile.exists() && targetLocalFile.isDirectory()) {
				final String fileName = sourcelfn.substring(longestMatchingPath.length());

				final int idx = fileName.indexOf('/');

				if (idx >= 0) {
					final String dirName = fileName.substring(0, idx);

					final File fDir = new File(targetLocalFile, dirName);

					if (fDir.exists()) {
						if (!fDir.isDirectory()) {
							out.printErrln("This file exists and I cannot create the same directory here: " + fDir.getAbsolutePath());
							return;
						}
					}
					else
						if (!fDir.mkdirs()) {
							out.printErrln("Could not create the directory: " + fDir.getAbsolutePath());
							return;
						}
				}

				writeToLocalFile = new File(targetLocalFile, fileName);
			}

			if (writeToLocalFile.exists()) {
				out.printErrln("Local copy target " + writeToLocalFile + " exists, skipping it");
				return;
			}

			final List<PFN> pfns = commander.c_api.getPFNsToRead(lfn, ses, exses);

			File transferAttempt = null;

			Exception lastException = null;

			if (pfns != null && pfns.size() > 0) {
				for (final PFN pfn : pfns) {
					logger.log(Level.INFO, "Trying " + pfn.pfn);

					final List<Protocol> protocols = Transfer.getAccessProtocols(pfn);
					for (final Protocol protocol : protocols) {
						final ProtocolAction pA = new ProtocolAction(protocol, pfn, writeToLocalFile);
						try {
							pA.start();
							while (pA.isAlive()) {
								Thread.sleep(500);
								if (!isSilent())
									out.pending();
							}

							if (pA.getFile() != null && pA.getFile().exists() && pA.getFile().length() > 0) {
								transferAttempt = pA.getFile();

								if (!isSilent())
									out.printOutln("Downloaded file to " + transferAttempt.getCanonicalPath());

								try {
									if (!transferAttempt.setLastModified(lfn.ctime.getTime())) {
										// alternative method of setting file times:
										final BasicFileAttributeView attributes = Files.getFileAttributeView(Paths.get(transferAttempt.getAbsolutePath()), BasicFileAttributeView.class);
										final FileTime time = FileTime.fromMillis(lfn.ctime.getTime());
										attributes.setTimes(time, time, time);
									}
								} catch (final Throwable t) {
									// this is not worth reporting to the user
									logger.log(Level.WARNING, "Exception setting file last modified timestamp", t);
								}

								break;
							}

							if ((lastException = pA.getLastException()) != null)
								logger.log(Level.WARNING, "Attempt to fetch " + pfn + " failed", lastException);

						} catch (final Exception e) {
							e.printStackTrace();
						}

					}
					if (transferAttempt != null && transferAttempt.exists() && transferAttempt.length() > 0) {
						resultFile = transferAttempt;
						break;
					}
				}
			}
			else
				out.printErrln("No replicas for this LFN: " + lfn.getCanonicalName());

			if (resultFile == null && !isSilent())
				out.printErrln("Could not get the file: " + sourcelfn + " to " + writeToLocalFile.getAbsolutePath() + (lastException != null ? ", error was: " + lastException.getMessage() : ""));
		}

		/**
		 * @return the local file
		 */
		public File getResult() {
			return resultFile;
		}
	}

	/**
	 * Copy a Grid file to a local file
	 *
	 * @param sourceLFN
	 *            Grid filename
	 * @param toLocalFile
	 *            local file
	 * @return local target file (one of them if multiple files were copied), or <code>null</code> if any problem
	 */
	public File copyGridToLocal(final String sourceLFN, final File toLocalFile) {
		final File targetLocalFile = toLocalFile;

		final LFN currentDir = commander.getCurrentDir();

		final String absolutePath = FileSystemUtils.getAbsolutePath(commander.user.getName(), currentDir != null ? currentDir.getCanonicalName() : null, sourceLFN);

		final List<String> sources = FileSystemUtils.expandPathWildCards(absolutePath, commander.user, commander.role);

		if (sources.size() == 0) {
			if (!isSilent())
				out.printErrln("No such file: " + sourceLFN);

			return null;
		}

		if (sources.size() > 1) {
			if (logger.isLoggable(Level.FINE))
				logger.log(Level.FINE, "Expanded path yielded multiple files: " + sources);

			// the target cannot be a file
			if (targetLocalFile.exists()) {
				if (!targetLocalFile.isDirectory()) {
					out.printErrln("Local target must be a directory when copying multiple files");
					return null;
				}

				if (!targetLocalFile.canWrite()) {
					out.printErrln("Cannot write into " + targetLocalFile.getAbsolutePath());
					return null;
				}
			}
			else
				if (!targetLocalFile.mkdirs()) {
					out.printErrln("Could not create the output target directory: " + targetLocalFile.getAbsolutePath());
					return null;
				}
		}

		// String longestMatchingPath = currentDir != null ? currentDir.getCanonicalName() : absolutePath;
		String longestMatchingPath = absolutePath;

		for (final String sourcelfn : sources)
			if (!sourcelfn.startsWith(longestMatchingPath)) {
				final char[] s1 = sourcelfn.toCharArray();
				final char[] s2 = longestMatchingPath.toCharArray();

				final int minLen = Math.min(s1.length, s2.length);
				int commonLength = 0;

				for (; commonLength < minLen; commonLength++)
					if (s1[commonLength] != s2[commonLength])
						break;

				longestMatchingPath = longestMatchingPath.substring(0, commonLength);
			}

		if (!longestMatchingPath.endsWith("/"))
			longestMatchingPath = longestMatchingPath.substring(0, longestMatchingPath.lastIndexOf('/') + 1);

		logger.log(Level.FINE, "Longest matching path: " + longestMatchingPath);

		File oneFileToReturn = null;

		if (sources.size() <= 1 || concurrentOperations <= 1) {
			for (final String sourcelfn : sources) {
				final GridToLocal oneFile = new GridToLocal(sourcelfn, longestMatchingPath, targetLocalFile);
				oneFile.run();

				if (oneFileToReturn == null)
					oneFileToReturn = oneFile.getResult();
			}
		}
		else {
			final ThreadPoolExecutor downloader = new CachedThreadPool(concurrentOperations, 1, TimeUnit.SECONDS, (r) -> new Thread(r, "cpGridToLocal"));
			downloader.allowCoreThreadTimeOut(true);

			final List<Future<GridToLocal>> futures = new LinkedList<>();

			for (final String sourcelfn : sources) {
				final GridToLocal oneFile = new GridToLocal(sourcelfn, longestMatchingPath, targetLocalFile);
				final Future<GridToLocal> future = downloader.submit(oneFile, oneFile);
				futures.add(future);
			}

			for (final Future<GridToLocal> future : futures) {
				final GridToLocal result;

				try {
					result = future.get();

					if (oneFileToReturn == null)
						oneFileToReturn = result.getResult();
				} catch (InterruptedException | ExecutionException e) {
					logger.log(Level.WARNING, "Exception waiting for a future", e);
				}
			}
		}

		return oneFileToReturn;
	}

	private static final ExecutorService UPLOAD_THREAD_POOL = new CachedThreadPool(Integer.MAX_VALUE,
			ConfigUtils.getConfig().getl("alien.shell.commands.JAliEnCommandcp.UPLOAD_THREAD_POOL.keepAliveTime", 2), TimeUnit.SECONDS, new ThreadFactory() {
				@Override
				public Thread newThread(Runnable r) {
					final Thread t = new Thread(r, "JAliEnCommandcp.UPLOAD_THREAD_POOL");

					return t;
				}
			});

	/**
	 * Upload one file in a separate thread
	 *
	 * @author costing
	 */
	private final class UploadWork implements Runnable {
		private final LFN lfn;
		private final GUID guid;
		private final File sourceFile;
		private final PFN pfn;
		private final Object lock;

		private String envelope;

		/**
		 * @param lfn
		 * @param guid
		 * @param sourceFile
		 * @param pfn
		 * @param lock
		 */
		public UploadWork(final LFN lfn, final GUID guid, final File sourceFile, final PFN pfn, final Object lock) {
			this.lfn = lfn;
			this.guid = guid;
			this.sourceFile = sourceFile;
			this.pfn = pfn;
			this.lock = lock;
		}

		@Override
		public void run() {
			envelope = uploadPFN(lfn, guid, sourceFile, pfn);

			synchronized (lock) {
				lock.notifyAll();
			}
		}

		public String getEnvelope() {
			return envelope;
		}
	}

	/**
	 * Copy a local file to the Grid
	 *
	 * @param sourceFile
	 *            local filename
	 * @param targetLFN
	 *            Grid filename
	 * @return status of the upload
	 */
	public boolean copyLocalToGrid(final File sourceFile, final String targetLFN) {
		if (!sourceFile.exists() || !sourceFile.isFile() || !sourceFile.canRead()) {
			if (!isSilent())
				out.printErrln("Could not get the local file: " + sourceFile.getAbsolutePath());
			else {
				final IOException ex = new IOException("Could not get the local file: " + sourceFile.getAbsolutePath());

				throw new IOError(ex);
			}

			return false;
		}

		List<PFN> pfns = null;

		final LFN currentDir = commander.getCurrentDir();

		final LFN lfn = commander.c_api.getLFN(FileSystemUtils.getAbsolutePath(commander.user.getName(), currentDir != null ? currentDir.getCanonicalName() : null, targetLFN), true);

		if (lfn.exists)
			if (lfn.isFile()) {
				if (!commander.c_api.removeLFN(lfn.getCanonicalName()))
					if (!isSilent())
						out.printErrln("Cannot remove the previously existing file: " + lfn.getCanonicalName());
					else
						throw new IOError(new IOException("Cannot remove the previously existing file: " + lfn.getCanonicalName()));
			}
			else
				if (!isSilent())
					out.printErrln("Target existing and is not a file: " + lfn.getCanonicalName());
				else
					throw new IOError(new IOException("Target existing and is not a file: " + lfn.getCanonicalName()));

		final GUID guid;

		try {
			guid = GUIDUtils.createGuid(sourceFile, commander.user);
		} catch (final IOException e) {
			if (!isSilent())
				out.printErrln("Couldn't create the GUID : " + e.getMessage());
			else {
				final IOException ex = new IOException("Couldn't create the GUID based on " + sourceFile, e);

				throw new IOError(ex);
			}

			return false;
		}

		lfn.guid = guid.guid;
		lfn.size = guid.size;
		lfn.md5 = guid.md5;
		lfn.jobid = jobId;
		lfn.ctime = guid.ctime;
		guid.lfnCache = new LinkedHashSet<>(1);
		guid.lfnCache.add(lfn);

		try {
			final PFNforWrite pfw = Dispatcher.execute(new PFNforWrite(commander.getUser(), commander.getRole(), commander.getSite(), lfn, guid, ses, exses, qos));

			pfns = pfw.getPFNs();

			if (pfns == null || pfns.size() == 0) {
				final String err = pfw.getErrorMessage();

				if (isSilent())
					throw new IOError(new IOException(err != null ? err : "No write access tickets were returned for " + lfn.getCanonicalName()));

				out.printErr(err != null ? err : "Could not get any write access tickets for " + lfn.getCanonicalName());

				return false;
			}
		} catch (final ServerException e) {
			if (!isSilent()) {
				out.printErrln("Couldn't get any access ticket.");

				return false;
			}

			throw new IOError(new IOException("Call for write PFNs for " + lfn.getCanonicalName() + " failed", e.getCause()));
		}

		qos.clear();

		for (final PFN p : pfns) {
			final SE se = commander.c_api.getSE(p.seNumber);

			if (se != null)
				exses.add(se.getName());
		}

		if (referenceCount == 0)
			referenceCount = pfns.size();

		final Vector<String> envelopes = new Vector<>(pfns.size());
		final Vector<String> registerPFNs = new Vector<>(pfns.size());

		final ArrayList<Future<UploadWork>> futures = new ArrayList<>(pfns.size());

		final Object lock = new Object();

		for (final PFN pfn : pfns) {
			final UploadWork work = new UploadWork(lfn, guid, sourceFile, pfn, lock);

			final Future<UploadWork> f = UPLOAD_THREAD_POOL.submit(work, work);

			futures.add(f);
		}

		long lastReport = System.currentTimeMillis();

		do {
			final Iterator<Future<UploadWork>> it = futures.iterator();

			while (it.hasNext()) {
				final Future<UploadWork> f = it.next();

				if (f.isDone())
					try {
						final UploadWork uw = f.get();

						final String envelope = uw.getEnvelope();

						if (envelope != null) {
							envelopes.add(envelope);

							if (!bW)
								break;
						}
					} catch (final InterruptedException e) {
						logger.log(Level.WARNING, "Interrupted operation", e);
					} catch (final ExecutionException e) {
						logger.log(Level.WARNING, "Execution exception", e);
					} finally {
						it.remove();
					}
			}

			if (!bW && pfns.size() > 1 && envelopes.size() > 0) {
				if (commit(envelopes, registerPFNs, guid, bD ? null : sourceFile, 1 + registerPFNs.size(), true))
					break;

				envelopes.clear();
				registerPFNs.clear();
			}

			if (futures.size() > 0) {
				if (System.currentTimeMillis() - lastReport > 500 && !isSilent()) {
					out.pending();

					lastReport = System.currentTimeMillis();
				}

				synchronized (lock) {
					try {
						lock.wait(100);
					} catch (@SuppressWarnings("unused") final InterruptedException e) {
						return false;
					}
				}
			}
		} while (futures.size() > 0);

		if (futures.size() > 0) {
			// there was a successfully registered upload so far, we can return true

			new BackgroundUpload(guid, futures, bD ? sourceFile : null).start();

			return true;
		}

		if (bD)
			sourceFile.delete();

		return commit(envelopes, registerPFNs, guid, bD ? null : sourceFile, referenceCount, true);
	}

	private final class BackgroundUpload extends Thread {
		private final GUID guid;
		private final List<Future<UploadWork>> futures;
		private final File fileToDeleteOnComplete;

		public BackgroundUpload(final GUID guid, final List<Future<UploadWork>> futures, final File fileToDeleteOnComplete) {
			super("alien.shell.commands.JAliEnCommandcp.BackgroundUpload (" + guid.guidId + ")");

			this.guid = guid;
			this.futures = futures;
			this.fileToDeleteOnComplete = fileToDeleteOnComplete;
		}

		@Override
		public void run() {
			final Vector<String> envelopes = new Vector<>(futures.size());

			while (futures.size() > 0) {
				final Iterator<Future<UploadWork>> it = futures.iterator();

				while (it.hasNext()) {
					final Future<UploadWork> f = it.next();

					if (f.isDone()) {
						logger.log(Level.FINER, "Got back one more copy of " + guid.guid);

						try {
							final UploadWork uw = f.get();

							final String envelope = uw.getEnvelope();

							if (envelope != null)
								envelopes.add(envelope);
						} catch (final InterruptedException e) {
							// Interrupted upload
							logger.log(Level.FINE, "Interrupted upload of " + guid.guid, e);
						} catch (final ExecutionException e) {
							// Error executing
							logger.log(Level.FINE, "Error getting the upload result of " + guid.guid, e);
						} finally {
							it.remove();
						}
					}
				}
			}

			if (envelopes.size() > 0)
				commit(envelopes, null, guid, null, futures.size(), false);

			if (fileToDeleteOnComplete != null)
				fileToDeleteOnComplete.delete();
		}
	}

	/**
	 * @param envelopes
	 * @param registerPFNs
	 * @param guid
	 * @param sourceFile
	 * @param desiredCount
	 * @param report
	 * @return <code>true</code> if the request was successful
	 */
	boolean commit(final Vector<String> envelopes, final Vector<String> registerPFNs, final GUID guid, final File sourceFile, final int desiredCount, final boolean report) {
		if (envelopes.size() != 0) {
			final List<PFN> registeredPFNs = commander.c_api.registerEnvelopes(envelopes);

			if (report && !isSilent() && (registeredPFNs == null || registeredPFNs.size() != envelopes.size()))
				out.printErrln("From the " + envelopes.size() + " replica with tickets only " + (registeredPFNs != null ? String.valueOf(registeredPFNs.size()) : "null") + " were registered");
		}

		int registeredPFNsCount = 0;

		if (registerPFNs != null && registerPFNs.size() > 0) {
			final List<PFN> registeredPFNs = commander.c_api.registerEnvelopes(registerPFNs);

			registeredPFNsCount = registeredPFNs != null ? registeredPFNs.size() : 0;

			if (report && !isSilent() && registeredPFNsCount != registerPFNs.size())
				out.printErrln("From the " + registerPFNs.size() + " pfns only " + registeredPFNsCount + " were registered");
		}

		if (sourceFile != null && envelopes.size() + registeredPFNsCount > 0)
			TempFileManager.putPersistent(guid, sourceFile);

		if (desiredCount == envelopes.size() + registeredPFNsCount) {
			if (report && !isSilent())
				out.printOutln("File successfully uploaded to " + desiredCount + " SEs");

			return true;
		}
		else
			if (envelopes.size() + registeredPFNsCount > 0) {
				if (report && !isSilent())
					out.printErrln("Only " + (envelopes.size() + registeredPFNsCount) + " out of " + desiredCount + " requested replicas could be uploaded");

				return true;
			}
			else
				if (report)
					if (!isSilent())
						out.printOutln("Upload failed, sorry!");
					else {
						final IOException ex = new IOException("Upload failed");

						throw new IOError(ex);
					}

		return false;
	}

	/**
	 * @param lfn
	 * @param guid
	 * @param sourceFile
	 * @param initialPFN
	 * @return the return envelope, if any
	 */
	String uploadPFN(final LFN lfn, final GUID guid, final File sourceFile, final PFN initialPFN) {
		boolean failOver;

		PFN pfn = initialPFN;

		String returnEnvelope = null;

		do {
			failOver = false;

			final List<Protocol> protocols = Transfer.getAccessProtocols(pfn);

			String targetPFNResult = null;

			for (final Protocol protocol : protocols) {
				try {
					if (!isSilent())
						out.printOutln("Uploading file " + sourceFile.getCanonicalPath() + " to " + pfn.getPFN());

					try {
						targetPFNResult = protocol.put(pfn, sourceFile);
					} catch (@SuppressWarnings("unused") final IOException ioe) {
						// ignore, will try next protocol or fetch another
						// replica to replace this one
					}
				} catch (@SuppressWarnings("unused") final Exception e) {
					// e.printStackTrace();
				}

				if (targetPFNResult != null)
					break;
			}

			if (targetPFNResult != null) {
				// if (!isSilent()){
				// out.printOutln("Successfully uploaded " + sourceFile.getAbsolutePath() + " to " + pfn.getPFN()+"\n"+targetLFNResult);
				// }

				if (pfn.ticket != null && pfn.ticket.envelope != null)
					if (pfn.ticket.envelope.getSignedEnvelope() != null)
						if (pfn.ticket.envelope.getEncryptedEnvelope() == null) {
							// signed envelopes were passed to the storage, it should have replied in kind
							returnEnvelope = targetPFNResult;
						}
						else {
							// give back to the central services the signed envelopes
							returnEnvelope = pfn.ticket.envelope.getSignedEnvelope();
						}
					else {
						// no signed envelopes, return the encrypted one, if any
						if (pfn.ticket.envelope.getEncryptedEnvelope() != null) {
							returnEnvelope = pfn.ticket.envelope.getEncryptedEnvelope();
						}
						else {
							// what kind of ticket was this?
							returnEnvelope = targetPFNResult;
						}
					}
				else {
					// if no ticket at all...
					returnEnvelope = targetPFNResult;
				}
			}
			else {
				failOver = true;

				if (!isSilent())
					out.printErrln("Error uploading file to SE: " + commander.c_api.getSE(pfn.seNumber).getName());

				synchronized (exses) {
					SE se = commander.c_api.getSE(pfn.seNumber);

					final HashMap<String, Integer> replacementQoS = new HashMap<>();

					String qosType = "disk";

					if (se.qos.size() > 0)
						qosType = se.qos.iterator().next();

					replacementQoS.put(qosType, Integer.valueOf(1));

					final List<PFN> newPFNtoTry = commander.c_api.getPFNsToWrite(lfn, guid, ses, exses, replacementQoS);

					if (newPFNtoTry != null && newPFNtoTry.size() > 0) {
						pfn = newPFNtoTry.get(0);

						se = commander.c_api.getSE(pfn.seNumber);

						exses.add(se.getName());
					}
					else
						pfn = null;
				}
			}
		} while (failOver && pfn != null);

		return returnEnvelope;
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		if (!isSilent()) {
			out.printOutln();
			out.printOutln(helpUsage("cp", "[-options] < file:///localfile /gridfile >  |  < /gridfile file:///localfile >  |  < -t /gridfile >"));
			out.printOutln(helpStartOptions());
			out.printOutln(helpOption("-g", "get by GUID"));
			out.printOutln(helpOption("-S", "[se[,se2[,!se3[,qos:count]]]]"));
			out.printOutln(helpOption("-t", "create a local temp file"));
			out.printOutln(helpOption("-silent", "execute command silently"));
			out.printOutln(helpOption("-T", "Use this many concurrent threads (where possible) - default 1"));
			out.printOutln();
		}
	}

	/**
	 * cp cannot run without arguments
	 *
	 * @return <code>false</code>
	 */
	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}

	private static boolean localFileSpec(final String file) {
		return file.startsWith("file:");
	}

	private static String getLocalFileSpec(final String file) {
		if (file.startsWith("file://"))
			return file.substring(7);

		if (file.startsWith("file:"))
			return file.substring(5);

		return file;
	}

	/**
	 * Constructor needed for the command factory in commander
	 *
	 * @param commander
	 * @param out
	 * @param alArguments
	 *            the arguments of the command
	 * @throws OptionException
	 */
	public JAliEnCommandcp(final JAliEnCOMMander commander, final UIPrintWriter out, final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);
		try {
			final OptionParser parser = new OptionParser();

			parser.accepts("S").withRequiredArg();
			parser.accepts("g");
			parser.accepts("t");
			parser.accepts("w");
			parser.accepts("W");
			parser.accepts("d");
			parser.accepts("j").withRequiredArg();
			parser.accepts("T").withRequiredArg();

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			if (options.nonOptionArguments().size() == 0)
				return;

			if (options.nonOptionArguments().size() != 2 && !(options.nonOptionArguments().size() == 1 && options.has("t"))) {
				printHelp();
				return;
			}

			if (options.has("w"))
				bW = true;

			if (options.has("W"))
				bW = false;

			if (options.has("d"))
				bD = true;

			if (options.has("j"))
				jobId = Long.parseLong((String) options.valueOf("j"));

			if (options.has("S") && options.hasArgument("S"))
				if ((String) options.valueOf("S") != null) {
					final StringTokenizer st = new StringTokenizer((String) options.valueOf("S"), ",");
					while (st.hasMoreElements()) {
						final String spec = st.nextToken();
						if (spec.contains("::")) {
							if (spec.indexOf("::") != spec.lastIndexOf("::"))
								if (spec.startsWith("!")) // an exSE spec
									exses.add(spec.toUpperCase());
								else {// an SE spec
									ses.add(spec.toUpperCase());
									referenceCount++;
								}
						}
						else
							if (spec.contains(":"))
								try {
									final int c = Integer.parseInt(spec.substring(spec.indexOf(':') + 1));
									if (c > 0) {
										qos.put(spec.substring(0, spec.indexOf(':')), Integer.valueOf(c));
										referenceCount = referenceCount + c;
									}
									else
										throw new JAliEnCommandException("Number of replicas has to be stricly positive, in " + spec);

								} catch (final Exception e) {
									throw new JAliEnCommandException("Could not parse the QoS string " + spec, e);
								}
							else
								if (!spec.equals(""))
									throw new JAliEnCommandException();
					}
				}

			bT = options.has("t");

			if (options.has("t") && options.hasArgument("t"))
				out.printOutln("t has val: " + (String) options.valueOf("t"));

			final List<String> nonOptionArguments = optionToString(options.nonOptionArguments());

			source = nonOptionArguments.get(0);
			if (!(options.nonOptionArguments().size() == 1 && options.has("t")))
				target = nonOptionArguments.get(1);

			if (options.has("T"))
				concurrentOperations = Integer.parseInt(options.valueOf("T").toString());

		} catch (final OptionException e) {
			printHelp();
			throw e;
		}
	}

}
