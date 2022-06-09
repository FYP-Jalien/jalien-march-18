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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import alien.api.Dispatcher;
import alien.api.ServerException;
import alien.api.catalogue.PFNforWrite;
import alien.catalogue.BookingTable.BOOKING_STATE;
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
import alien.shell.ErrNo;
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
	 * If <code>true</code> then wait for all uploads to finish before returning.
	 * Otherwise return after the first successful one, waiting for the remaining
	 * copies to be uploaded in background and committed asynchronously to the
	 * catalogue.
	 */
	private boolean bW = ConfigUtils.getConfig().getb("alien.shell.commands.cp.wait_for_all_uploads.default", false);

	/**
	 * If <code>true</code> then the source file is to be deleted after a successful
	 * upload (for temporary files)
	 */
	private boolean bD = false;

	/**
	 * If <code>true</code> then replicas failing immediate upload are asynchronously mirror-ed to the rest of the locations.
	 * It would only apply for partially successful uploads, at least one physical replica must exist at that point.
	 */
	private boolean bM = false;

	/**
	 * Number of concurrent operations to do
	 */
	private int concurrentOperations = 1;

	private int referenceCount = 0;

	private final List<String> ses = new ArrayList<>();
	private final List<String> exses = new ArrayList<>();

	private final Map<String, AtomicInteger> failedUploads = new ConcurrentHashMap<>();

	private final HashMap<String, Integer> qos = new HashMap<>();

	private String source = null;
	private String target = null;

	private File localFile = null;

	private long jobId = -1;

	private boolean noCommit = false;

	private String protocolUploadErrors = "";

	// public long timingChallenge = 0;

	// public boolean isATimeChallenge = false;

	@Override
	public void run() {
		if (bT) {
			try {
				final LFN currentDir = commander.getCurrentDir();

				final String absolutePath = FileSystemUtils.getAbsolutePath(commander.user.getName(), currentDir != null ? currentDir.getCanonicalName() : null, source);

				final LFN lSource = commander.c_api.getLFN(absolutePath);

				if (lSource == null) {
					commander.setReturnCode(1, "File doesn't exist: " + absolutePath);
				}
				else if (!lSource.isFile()) {
					commander.setReturnCode(2, "This entry is not a file: " + absolutePath);
				}
				else {
					final GUID g = commander.c_api.getGUID(lSource.guid.toString());

					if (g == null) {
						commander.setReturnCode(2, "Cannot get the GUID of " + absolutePath);
					}
					else {
						localFile = TempFileManager.getAny(g);

						if (localFile == null) {
							final File fTemp = File.createTempFile("jalien.get.", ".temp");

							fTemp.delete();

							localFile = copyGridToLocal(absolutePath, fTemp);

							if (localFile != null)
								TempFileManager.putTemp(g, localFile);
						}
						else {
							commander.printOutln("Downloaded file to " + localFile.getAbsolutePath());
						}
					}
				}
			}
			catch (@SuppressWarnings("unused") final IOException ioe) {
				commander.printErrln("Cannot create temporary file");
			}
		}
		else {
			commander.outNextResult();
			if (!localFileSpec(source) && localFileSpec(target)) {
				// download a file
				localFile = new File(getLocalFileSpec(target));

				if (!localFile.exists() || localFile.isDirectory())
					copyGridToLocal(source, localFile);
				else {
					commander.setReturnCode(1, "A local file already exists with this name: " + target);

					if (isSilent()) {
						final IOException ex = new IOException("A local file already exists with this name: " + target);

						throw new IOError(ex);
					}
					return;
				}
			}
			else if (localFileSpec(source) && !localFileSpec(target)) {
				// from the local disk to the Grid

				final File sourceFile = new File(getLocalFileSpec(source));

				if (!targetLFNExists(target))
					if (sourceFile.exists())
						copyLocalToGrid(sourceFile, target);
					else {
						commander.setReturnCode(2, "A local file with this name does not exists.");
						if (isSilent()) {
							final IOException ex = new IOException("Local file " + target + " doesn't exist");

							throw new IOError(ex);
						}
						return;
					}
			}
			else {
				// copying between two Grid endpoints

				final LFN currentDir = commander.getCurrentDir();

				final LFN tLFN = commander.c_api.getLFN(FileSystemUtils.getAbsolutePath(commander.user.getName(), currentDir != null ? currentDir.getCanonicalName() : null, target));

				if (tLFN != null && tLFN.exists && !tLFN.isDirectory()) {
					commander.setReturnCode(3, "Target file already exists: " + tLFN.getCanonicalName());
					if (isSilent()) {
						final IOException ex = new IOException("Target file already exists: " + tLFN.getCanonicalName());

						throw new IOError(ex);
					}
					return;
				}

				final String absolutePath = FileSystemUtils.getAbsolutePath(commander.user.getName(), currentDir != null ? currentDir.getCanonicalName() : null, source);

				final List<String> expandedPaths = FileSystemUtils.expandPathWildCards(absolutePath, commander.user);

				if (expandedPaths == null || expandedPaths.isEmpty()) {
					commander.setReturnCode(4, "No such file: " + source);
					return;
				}

				if (expandedPaths.size() > 1) {
					// if more than one file then the target must be an existing directory

					if (tLFN == null || !tLFN.exists || !tLFN.isDirectory()) {
						commander.setReturnCode(5, "Multiple sources can only be copied to a target directory");
						if (isSilent()) {
							final IOException ex = new IOException("Multiple sources can only be copied to a target directory");

							throw new IOError(ex);
						}
						return;
					}
				}

				String lastError = null;

				for (final String sFile : expandedPaths) {
					String actualTarget = target;

					LFN actualLFN = null;

					if (tLFN != null) {
						// target is a directory, have to create the same file name inside
						String fileName = sFile;

						if (fileName.contains("/"))
							fileName = fileName.substring(fileName.lastIndexOf('/') + 1);

						actualTarget = tLFN.getCanonicalName() + fileName;

						actualLFN = commander.c_api.getLFN(actualTarget);
					}

					if (actualLFN != null && actualLFN.exists) {
						lastError = "Target already exists: " + actualTarget;
						commander.setReturnCode(3, lastError);
						continue;
					}

					localFile = copyGridToLocal(sFile, null);
					if (localFile != null && localFile.exists() && localFile.length() > 0) {
						if (copyLocalToGrid(localFile, actualTarget) != null) {
							commander.printOutln("Copied " + sFile + " -> " + actualTarget);
						}
						else {
							lastError = "Could not copy " + sFile + " -> " + actualTarget;
							commander.setReturnCode(6, lastError);
							continue;
						}
					}
					else {
						lastError = "Could not get the contents of " + sFile;
						commander.setReturnCode(7, lastError);
						continue;
					}
				}

				if (lastError != null && isSilent())
					throw new IOError(new IOException(lastError));
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

							while ((zipentry = zi.getNextEntry()) != null)
								if (zipentry.getName().equals(archiveFileName)) {
									final FileOutputStream fos = new FileOutputStream(file);

									final byte[] buf = new byte[8192];

									int n;

									while ((n = zi.read(buf, 0, buf.length)) > -1)
										fos.write(buf, 0, n);

									fos.close();
									zi.closeEntry();

									output = file;

									break;
								}
						}
						finally {
							TempFileManager.release(tempLocalFile);
						}
					}
				}
				else
					output = proto.get(pfn, file);
			}
			catch (final IOException e) {
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
			commander.setReturnCode(3, "The target LFN already exists: " + targetLFN);

			return true;
		}
		return false;
	}

	private class GridToLocal implements Runnable {
		private final String sourcelfn;
		private LFN lfn;
		private final String longestMatchingPath;
		private final File targetLocalFile;
		private File resultFile = null;

		public GridToLocal(final String sourcelfn, final String longestMatchingPath, final File targetLocalFile) {
			this.sourcelfn = sourcelfn;
			this.longestMatchingPath = longestMatchingPath;
			this.targetLocalFile = targetLocalFile;
		}

		public GridToLocal(final LFN lfn, final File targetLocalFile) {
			this.sourcelfn = null;
			this.lfn = lfn;
			this.longestMatchingPath = null;
			this.targetLocalFile = targetLocalFile;
		}

		@Override
		public void run() {
			if (lfn == null)
				lfn = commander.c_api.getLFN(sourcelfn);

			if (lfn == null) {
				commander.setReturnCode(101, "Could not get the file's LFN: " + sourcelfn);
				return;
			}

			if (!lfn.isFile())
				// ignoring anything else but files
				return;

			File writeToLocalFile = targetLocalFile;

			if (targetLocalFile != null && targetLocalFile.exists() && targetLocalFile.isDirectory()) {
				if (sourcelfn == null || longestMatchingPath == null) {
					commander.setReturnCode(102, "Target file already exists and on this code path I should not save it in an alternate location");
					return;
				}

				final String fileName = sourcelfn.substring(longestMatchingPath.length());

				final int idx = fileName.lastIndexOf('/');

				if (idx >= 0) {
					final String dirName = fileName.substring(0, idx);

					final File fDir = new File(targetLocalFile, dirName);

					if (fDir.exists()) {
						if (!fDir.isDirectory()) {
							commander.setReturnCode(102, "This file exists and I cannot create the same directory here: " + fDir.getAbsolutePath());
							return;
						}
					}
					else if (!fDir.mkdirs()) {
						commander.setReturnCode(103, "Could not create the directory: " + fDir.getAbsolutePath());
						return;
					}
				}

				writeToLocalFile = new File(targetLocalFile, fileName);
			}

			if (writeToLocalFile != null && writeToLocalFile.exists()) {
				commander.setReturnCode(104, "Local copy target " + writeToLocalFile + " exists, skipping it");
				return;
			}

			final List<PFN> pfns = commander.c_api.getPFNsToRead(lfn, ses, exses);

			File transferAttempt = null;

			Exception lastException = null;

			if (pfns != null && pfns.size() > 0)
				for (final PFN pfn : pfns) {
					logger.log(Level.INFO, "Trying " + pfn.pfn);

					final List<Protocol> protocols = Transfer.getAccessProtocols(pfn);
					for (final Protocol protocol : protocols) {
						final ProtocolAction pA = new ProtocolAction(protocol, pfn, writeToLocalFile);
						try {
							pA.start();
							while (pA.isAlive()) {
								Thread.sleep(500);
								commander.pending();
							}

							if (pA.getFile() != null && pA.getFile().exists() && pA.getFile().length() > 0) {
								transferAttempt = pA.getFile();

								commander.printOutln("Downloaded file to " + transferAttempt.getCanonicalPath());

								try {
									if (!transferAttempt.setLastModified(lfn.ctime.getTime())) {
										// alternative method of setting file times:
										final BasicFileAttributeView attributes = Files.getFileAttributeView(Paths.get(transferAttempt.getAbsolutePath()), BasicFileAttributeView.class);
										final FileTime time = FileTime.fromMillis(lfn.ctime.getTime());
										attributes.setTimes(time, time, time);
									}
								}
								catch (final Throwable t) {
									// this is not worth reporting to the user
									logger.log(Level.WARNING, "Exception setting file last modified timestamp", t);
								}

								break;
							}

							if ((lastException = pA.getLastException()) != null)
								logger.log(Level.WARNING, "Attempt to fetch " + pfn + " failed", lastException);

						}
						catch (final Exception e) {
							e.printStackTrace();
						}

					}
					if (transferAttempt != null && transferAttempt.exists() && transferAttempt.length() > 0) {
						resultFile = transferAttempt;
						break;
					}
				}
			else
				commander.setReturnCode(105, "No replicas for this LFN: " + lfn.getCanonicalName());

			if (resultFile == null)
				commander.setReturnCode(106, "Could not get the file: " + lfn.getCanonicalName() + " to " + (writeToLocalFile != null ? writeToLocalFile.getAbsolutePath() : " a temporary file")
						+ (lastException != null ? ", error was: " + lastException.getMessage() : ""));
		}

		/**
		 * @return the local file
		 */
		public File getResult() {
			return resultFile;
		}
	}

	/**
	 * Copy one LFN to one local file
	 *
	 * @param lfn the LFN object pointing to a file
	 * @param toLocalFile the target file to write to
	 * @return the result of the download
	 */
	public File copyGridToLocal(final LFN lfn, final File toLocalFile) {
		final GridToLocal cpOperation = new GridToLocal(lfn, toLocalFile);
		cpOperation.run();
		return cpOperation.getResult();
	}

	/**
	 * Copy a Grid file to a local file
	 *
	 * @param sourceLFN
	 *            Grid filename
	 * @param toLocalFile
	 *            local file
	 * @return local target file (one of them if multiple files were copied), or
	 *         <code>null</code> if any problem
	 */
	public File copyGridToLocal(final String sourceLFN, final File toLocalFile) {
		final File targetLocalFile = toLocalFile;

		final LFN currentDir = commander.getCurrentDir();

		final String absolutePath = FileSystemUtils.getAbsolutePath(commander.user.getName(), currentDir != null ? currentDir.getCanonicalName() : null, sourceLFN);

		final List<String> expandedPaths = FileSystemUtils.expandPathWildCards(absolutePath, commander.user);

		if (expandedPaths == null || expandedPaths.isEmpty()) {
			commander.setReturnCode(107, "No such file: " + sourceLFN);
			return null;
		}

		final List<String> sources = new ArrayList<>(expandedPaths.size());

		for (final String expandedPath : expandedPaths) {
			final LFN l = commander.c_api.getLFN(expandedPath);

			if (l != null)
				if (l.isFile())
					sources.add(expandedPath);
				else if (l.isDirectory()) {
					final Collection<LFN> findresult = commander.c_api.find(expandedPath, "*", 0);

					logger.log(Level.FINER, "`find " + expandedPath + " *` produced " + findresult.size() + " results");

					for (final LFN file : findresult)
						if (file.isFile())
							sources.add(file.getCanonicalName());
				}
				else if (l.isCollection())
					sources.addAll(l.listCollection());
		}

		if (sources.size() > 1) {
			if (logger.isLoggable(Level.FINE))
				logger.log(Level.FINE, "Expanded path yielded multiple files: " + sources);

			// the target cannot be a file
			if (targetLocalFile.exists()) {
				if (!targetLocalFile.isDirectory()) {
					commander.setReturnCode(108, "Local target must be a directory when copying multiple files");
					return null;
				}

				if (!targetLocalFile.canWrite()) {
					commander.setReturnCode(109, "Cannot write into " + targetLocalFile.getAbsolutePath());
					return null;
				}
			}
			else if (!targetLocalFile.mkdirs()) {
				commander.setReturnCode(110, "Could not create the output target directory: " + targetLocalFile.getAbsolutePath());
				return null;
			}
		}

		// String longestMatchingPath = currentDir != null ?
		// currentDir.getCanonicalName() : absolutePath;
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

		if (sources.size() <= 1 || concurrentOperations <= 1)
			for (final String sourcelfn : sources) {
				final GridToLocal oneFile = new GridToLocal(sourcelfn, longestMatchingPath, targetLocalFile);
				oneFile.run();

				if (oneFileToReturn == null)
					oneFileToReturn = oneFile.getResult();
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
				}
				catch (InterruptedException | ExecutionException e) {
					logger.log(Level.WARNING, "Exception waiting for a future", e);
				}
			}
		}

		return oneFileToReturn;
	}

	private static final ExecutorService UPLOAD_THREAD_POOL = new CachedThreadPool(Integer.MAX_VALUE,
			ConfigUtils.getConfig().getl("alien.shell.commands.JAliEnCommandcp.UPLOAD_THREAD_POOL.timeOutSeconds", 2), TimeUnit.SECONDS, new ThreadFactory() {
				@Override
				public Thread newThread(final Runnable r) {
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
	public LFN copyLocalToGrid(final File sourceFile, final String targetLFN) {
		if (!sourceFile.exists() || !sourceFile.isFile() || !sourceFile.canRead()) {
			commander.setReturnCode(201, "Could not get the local file: " + sourceFile.getAbsolutePath());
			if (isSilent()) {
				final IOException ex = new IOException("Could not get the local file: " + sourceFile.getAbsolutePath());

				throw new IOError(ex);
			}

			return null;
		}

		List<PFN> pfns = null;

		final LFN currentDir = commander.getCurrentDir();

		final LFN lfn = commander.c_api.getLFN(FileSystemUtils.getAbsolutePath(commander.user.getName(), currentDir != null ? currentDir.getCanonicalName() : null, targetLFN), true);

		if (lfn.exists)
			if (lfn.isFile()) {
				if (!commander.c_api.removeLFN(lfn.getCanonicalName())) {
					commander.setReturnCode(202, "Cannot remove the previously existing file: " + lfn.getCanonicalName());
					if (isSilent())
						throw new IOError(new IOException("Cannot remove the previously existing file: " + lfn.getCanonicalName()));

					return null;
				}
			}
			else {
				commander.setReturnCode(203, "Target existing and is not a file: " + lfn.getCanonicalName());

				if (isSilent())
					throw new IOError(new IOException("Target existing and is not a file: " + lfn.getCanonicalName()));

				return null;
			}

		final GUID guid;

		try {
			guid = GUIDUtils.createGuid(sourceFile, commander.user);
		}
		catch (final IOException e) {
			commander.setReturnCode(204, "Couldn't create the GUID : " + e.getMessage());
			if (isSilent()) {
				final IOException ex = new IOException("Couldn't create the GUID based on " + sourceFile, e);

				throw new IOError(ex);
			}

			return null;
		}

		lfn.guid = guid.guid;
		lfn.size = guid.size;
		lfn.md5 = guid.md5;
		lfn.jobid = jobId;
		lfn.ctime = guid.ctime;
		guid.lfnCache = new LinkedHashSet<>(1);
		guid.lfnCache.add(lfn);

		try {
			final PFNforWrite pfw = Dispatcher.execute(new PFNforWrite(commander.getUser(), commander.getSite(), lfn, guid, ses, exses, qos));

			pfns = pfw.getPFNs();

			if (pfns == null || pfns.size() == 0) {
				final String err = pfw.getErrorMessage();

				commander.setReturnCode(205, err != null ? err : "Could not get any write access tickets for " + lfn.getCanonicalName());

				if (isSilent())
					throw new IOError(new IOException(err != null ? err : "No write access tickets were returned for " + lfn.getCanonicalName()));

				return null;
			}
		}
		catch (final ServerException e) {
			commander.setReturnCode(206, "Couldn't get any access ticket.");

			if (isSilent())
				throw new IOError(new IOException("Call for write PFNs for " + lfn.getCanonicalName() + " failed", e.getCause()));

			return null;
		}

		for (final PFN p : pfns) {
			final SE se = commander.c_api.getSE(p.seNumber);

			if (se != null)
				exses.add(se.getName());
		}

		if (referenceCount == 0)
			referenceCount = pfns.size();

		final Vector<String> envelopes = new Vector<>(pfns.size());

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
					}
					catch (final InterruptedException e) {
						logger.log(Level.WARNING, "Interrupted operation", e);
					}
					catch (final ExecutionException e) {
						logger.log(Level.WARNING, "Execution exception", e);
					}
					finally {
						it.remove();
					}
			}

			if (!bW && pfns.size() > 1 && envelopes.size() > 0) {
				if (commit(lfn.getCanonicalName(), envelopes, guid, bD ? null : sourceFile, envelopes.size(), true)) {
					envelopes.clear();
					break;
				}
			}

			if (futures.size() > 0) {
				if (System.currentTimeMillis() - lastReport > 500 && !isSilent()) {
					commander.pending();

					lastReport = System.currentTimeMillis();
				}

				synchronized (lock) {
					try {
						lock.wait(100);
					}
					catch (@SuppressWarnings("unused") final InterruptedException e) {
						return null;
					}
				}
			}
		} while (futures.size() > 0);

		if (futures.size() > 0) {
			// there was a successfully registered upload so far, we can return true

			new BackgroundUpload(lfn, guid, futures, bD ? sourceFile : null).start();

			return lfn;
		}
		else if (bD && envelopes.size() > 0)
			sourceFile.delete();

		if (commit(lfn.getCanonicalName(), envelopes, guid, bD ? null : sourceFile, referenceCount, true))
			return lfn;

		return null;
	}

	private final class BackgroundUpload extends Thread {
		private final GUID guid;
		private final List<Future<UploadWork>> futures;
		private final int originalNoOfCopies;
		private final File fileToDeleteOnComplete;
		private final LFN lfn;

		public BackgroundUpload(final LFN lfn, final GUID guid, final List<Future<UploadWork>> futures, final File fileToDeleteOnComplete) {
			super("alien.shell.commands.JAliEnCommandcp.BackgroundUpload (" + futures.size() + " x " + guid.guid + " )");

			this.lfn = lfn;
			this.guid = guid;
			this.futures = futures;
			this.fileToDeleteOnComplete = fileToDeleteOnComplete;
			this.originalNoOfCopies = futures.size();
		}

		@Override
		public void run() {
			final Vector<String> envelopes = new Vector<>(futures.size());

			while (futures.size() > 0) {
				final Iterator<Future<UploadWork>> it = futures.iterator();

				boolean anyChange = false;

				while (it.hasNext()) {
					final Future<UploadWork> f = it.next();

					if (f.isDone()) {
						logger.log(Level.FINER, "Got back one more copy of " + guid.guid);

						try {
							final UploadWork uw = f.get();

							final String envelope = uw.getEnvelope();

							if (envelope != null)
								envelopes.add(envelope);
						}
						catch (final InterruptedException e) {
							// Interrupted upload
							logger.log(Level.FINE, "Interrupted upload of " + guid.guid, e);
						}
						catch (final ExecutionException e) {
							// Error executing
							logger.log(Level.FINE, "Error getting the upload result of " + guid.guid, e);
						}
						finally {
							it.remove();
							anyChange = true;
						}
					}
				}

				if (futures.size() > 0) {
					if (anyChange)
						setName("alien.shell.commands.JAliEnCommandcp.BackgroundUpload (" + futures.size() + " / " + originalNoOfCopies + " x " + guid.guid + ")");

					try {
						Thread.sleep(100);
					}
					catch (@SuppressWarnings("unused") final InterruptedException ie) {
						break;
					}
				}
			}

			if (envelopes.size() > 0) {
				commit(lfn.getCanonicalName(), envelopes, guid, null, futures.size(), false);

				if (fileToDeleteOnComplete != null)
					fileToDeleteOnComplete.delete();
			}
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
	boolean commit(final String lfnToCommit, final Vector<String> envelopes, final GUID guid, final File sourceFile, final int desiredCount, final boolean report) {
		if (envelopes.size() != 0) {
			final List<PFN> registeredPFNs = commander.c_api.registerEnvelopes(envelopes, noCommit ? BOOKING_STATE.KEPT : BOOKING_STATE.COMMITED);

			if (report && (registeredPFNs == null || registeredPFNs.size() != envelopes.size()))
				commander.setReturnCode(301,
						"From the " + envelopes.size() + " replica with tickets only " + (registeredPFNs != null ? String.valueOf(registeredPFNs.size()) : "null") +
								" were registered. We had the following errors: " + protocolUploadErrors);
		}

		if (sourceFile != null && envelopes.size() > 0)
			TempFileManager.putPersistent(guid, sourceFile);

		if (desiredCount <= envelopes.size()) {
			if (report)
				commander.printOutln("File successfully uploaded to " + desiredCount + " SEs");

			return true;
		}
		else if (envelopes.size() > 0) {
			if (bM && failedUploads.size() > 0) {
				// scheduling a background mirror to the missing replicas
				final List<String> sesToRecover = new ArrayList<>();
				final Map<String, Integer> qosToRecover = new HashMap<>();

				for (final Map.Entry<String, AtomicInteger> entry : failedUploads.entrySet()) {
					final String key = entry.getKey();

					if (key.contains("::"))
						sesToRecover.add(key);
					else
						qosToRecover.put(key, Integer.valueOf(entry.getValue().intValue()));
				}

				final Map<String, Long> scheduledTransfers = commander.c_api.mirrorLFN(lfnToCommit, sesToRecover, null, qosToRecover, false, Integer.valueOf(10), null);

				if (scheduledTransfers != null && scheduledTransfers.size() > 0) {
					commander.printOutln("Scheduled the following transfers to make up for the failed uploads: " + scheduledTransfers);

					// TODO: decide whether or not to return here, avoiding to send the jobs to DONE_WARN if the file can be recovered
				}
			}

			// Warning: do NOT touch this message, it is used in particular by JobWrapper to decide upon the job status
			if (report)
				commander.printOutln("Only " + envelopes.size() + " out of " + desiredCount + " requested replicas could be uploaded");

			return true;
		}
		else if (report) {
			commander.setReturnCode(302, "Upload failed, sorry! We had the following errors: " + protocolUploadErrors);
			if (isSilent()) {
				final IOException ex = new IOException("Upload failed");

				throw new IOError(ex);
			}
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
					commander.printOutln("Uploading file " + sourceFile.getCanonicalPath() + " to " + pfn.getPFN());

					try {
						targetPFNResult = protocol.put(pfn, sourceFile);
					}
					catch (final IOException ioe) {
						// ignore, will try next protocol or fetch another replica to replace this one
						protocolUploadErrors += System.lineSeparator() + ioe.toString();
					}
				}
				catch (@SuppressWarnings("unused") final Exception e) {
					// e.printStackTrace();
				}

				if (targetPFNResult != null)
					break;
			}

			if (targetPFNResult != null) {
				// commander.printOutln("Successfully uploaded " + sourceFile.getAbsolutePath()
				// + " to " + pfn.getPFN());

				if (pfn.ticket != null && pfn.ticket.envelope != null)
					if (pfn.ticket.envelope.getSignedEnvelope() != null)
						if (pfn.ticket.envelope.getEncryptedEnvelope() == null)
							// signed envelopes were passed to the storage, it should have replied in kind
							returnEnvelope = targetPFNResult;
						else
							// give back to the central services the signed envelopes
							returnEnvelope = pfn.ticket.envelope.getSignedEnvelope();
					else
					// no signed envelopes, return the encrypted one, if any
					if (pfn.ticket.envelope.getEncryptedEnvelope() != null)
						returnEnvelope = pfn.ticket.envelope.getEncryptedEnvelope();
					else
						// what kind of ticket was this?
						returnEnvelope = targetPFNResult;
				else
					// if no ticket at all...
					returnEnvelope = targetPFNResult;
			}
			else {
				// release the file for immediate collection
				if (pfn.ticket != null && pfn.ticket.envelope != null) {
					String rejectEnvelope = pfn.ticket.envelope.getEncryptedEnvelope();

					if (rejectEnvelope == null)
						rejectEnvelope = pfn.ticket.envelope.getSignedEnvelope();

					// blind call to the central services to reject this failed copy
					if (rejectEnvelope != null)
						commander.c_api.registerEnvelopes(Arrays.asList(rejectEnvelope), BOOKING_STATE.REJECTED);
				}

				SE se = commander.c_api.getSE(pfn.seNumber);

				final String failedSEName = se.getName();

				boolean wasExplicitSE = false;

				for (final String s : ses) {
					if (failedSEName.equalsIgnoreCase(s)) {
						wasExplicitSE = true;
						break;
					}
				}

				if (wasExplicitSE) {
					commander.printErrln("Error uploading file to explicit SE target: " + failedSEName);

					failedUploads.put(failedSEName, new AtomicInteger(1));
				}
				else {
					failOver = true;

					final HashMap<String, Integer> replacementQoS = new HashMap<>();

					String qosType = "disk";

					if (se.qos.size() > 0) {
						// keep the order from LDAP, match in the same order
						final Set<String> targetSEQoS = new LinkedHashSet<>(se.qos);

						if (qos.size() > 0) {
							// try to match the original QoS constraints
							targetSEQoS.retainAll(qos.keySet());
						}

						if (targetSEQoS.size() > 0)
							qosType = targetSEQoS.iterator().next();
					}

					replacementQoS.put(qosType, Integer.valueOf(1));

					synchronized (exses) {
						final List<PFN> newPFNtoTry = commander.c_api.getPFNsToWrite(lfn, guid, ses, new ArrayList<>(exses), replacementQoS);

						if (newPFNtoTry != null && newPFNtoTry.size() > 0) {
							pfn = newPFNtoTry.get(0);

							se = commander.c_api.getSE(pfn.seNumber);

							exses.add(se.getName());
						}
						else {
							pfn = null;

							failedUploads.computeIfAbsent(qosType, (k) -> new AtomicInteger(0)).incrementAndGet();
						}
					}

					if (pfn != null)
						commander.printErrln("Error uploading file to SE: " + failedSEName + " of type " + qosType + ", trying instead to upload to " + se.getName());
					else
						commander.printErrln("Error uploading file to SE: " + failedSEName + " of type " + qosType + ", didn't get an alternative location of the same type to upload to");
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
		commander.printOutln();
		commander.printOutln(helpUsage("cp", "[-options] <source> [<target>]"));
		commander.printOutln("Source and target paths can be one of the following formats:");
		commander.printOutln("\tfile://<local file on disk>");
		commander.printOutln("\talien://<path in the Grid catalogue>");
		commander.printOutln("\tor without a prefix, in which case it's treated as a Grid path");
		commander.printOutln(helpStartOptions());
		commander.printOutln(helpOption("-g", "treat the Grid path as a GUID"));
		commander.printOutln(helpOption("-S <SEs|QoS:count>", "Storage target specifications for uploading, default is 'disk:2'"));
		commander.printOutln(helpOption("-t", "no target needed, create a local temporary file instead, print out the local path at the end"));
		commander.printOutln(helpOption("-silent", "execute command silently"));
		commander.printOutln(helpOption("-w", "wait for all replicas to complete upload before returning (default " + bW + ")"));
		commander.printOutln(helpOption("-W", "do _not_ wait for all replicas to complete upload, return as soon as the first replica is available"));
		commander.printOutln(helpOption("-T", "Use this many concurrent download threads (where possible) - default 1"));
		commander.printOutln(helpOption("-d", "delete local file after a successful upload (i.e. move local to Grid)"));
		commander.printOutln(helpOption("-j <job ID>", "the job ID that has created the file"));
		commander.printOutln(helpOption("-m", "queue mirror operations to the missing SEs, in case of partial success. Forces '-w'"));
		commander.printOutln();
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
	 * @param alArguments
	 *            the arguments of the command
	 * @throws OptionException
	 */
	public JAliEnCommandcp(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);
		try {
			final OptionParser parser = new OptionParser();

			parser.accepts("S").withRequiredArg();
			parser.accepts("g");
			parser.accepts("t");
			parser.accepts("w");
			parser.accepts("W");
			parser.accepts("d");
			parser.accepts("v");
			parser.accepts("j").withRequiredArg().ofType(Long.class);
			parser.accepts("T").withRequiredArg().ofType(Integer.class);
			parser.accepts("silent");
			parser.accepts("nc");
			parser.accepts("m");

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

			if (options.has("m")) {
				bM = true;
				bW = true;
			}

			if (options.has("silent"))
				this.silent();

			if (options.has("j"))
				jobId = ((Long) options.valueOf("j")).longValue();
			else
				jobId = ConfigUtils.getConfig().getl("ALIEN_PROC_ID", jobId);

			if (options.has("nc")) {
				if (jobId > 0)
					noCommit = true;
				else {
					commander.printErrln("no commit flag can only be set for job outputs");
					return;
				}
			}

			if (options.has("S") && options.hasArgument("S"))
				if ((String) options.valueOf("S") != null) {
					final StringTokenizer st = new StringTokenizer((String) options.valueOf("S"), ",");
					while (st.hasMoreElements()) {
						final String spec = st.nextToken().replace('=', ':');
						if (spec.contains("::")) {
							if (spec.indexOf("::") != spec.lastIndexOf("::"))
								if (spec.startsWith("!")) // an exSE spec
									exses.add(spec.toUpperCase().substring(1));
								else {// an SE spec
									ses.add(spec.toUpperCase());
									referenceCount++;
								}
						}
						else if (spec.contains(":"))
							try {
								final int c = Integer.parseInt(spec.substring(spec.indexOf(':') + 1));
								if (c > 0) {
									qos.put(spec.substring(0, spec.indexOf(':')), Integer.valueOf(c));
									referenceCount = referenceCount + c;
								}
								else
									throw new JAliEnCommandException("Number of replicas has to be stricly positive, in " + spec);

							}
							catch (final Exception e) {
								throw new JAliEnCommandException("Could not parse the QoS string " + spec, e);
							}
						else if (!spec.equals(""))
							throw new JAliEnCommandException();
					}
				}

			bT = options.has("t");

			if (options.has("t") && options.hasArgument("t"))
				commander.printOutln("t has val: " + (String) options.valueOf("t"));

			final List<String> nonOptionArguments = optionToString(options.nonOptionArguments());

			source = nonOptionArguments.get(0);
			if (!(options.nonOptionArguments().size() == 1 && options.has("t")))
				target = nonOptionArguments.get(1);

			if (options.has("T"))
				concurrentOperations = ((Integer) options.valueOf("T")).intValue();
		}
		catch (final OptionException e) {
			commander.setReturnCode(ErrNo.EINVAL, e.getMessage());
			setArgumentsOk(false);
		}
	}

}
