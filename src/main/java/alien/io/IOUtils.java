package alien.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;

import alien.catalogue.FileSystemUtils;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.PFN;
import alien.catalogue.access.AccessType;
import alien.catalogue.access.AuthorizationFactory;
import alien.config.ConfigUtils;
import alien.io.protocols.Protocol;
import alien.io.protocols.TempFileManager;
import alien.se.SEUtils;
import alien.shell.commands.JAliEnCOMMander;
import alien.shell.commands.JAliEnCommandcp;
import alien.shell.commands.PlainWriter;
import alien.shell.commands.UIPrintWriter;
import alien.user.AliEnPrincipal;
import alien.user.AuthorizationChecker;
import lazyj.Utils;
import utils.CachedThreadPool;

/**
 * Helper functions for IO
 *
 * @author costing
 */
public class IOUtils {

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(IOUtils.class.getCanonicalName());

	/**
	 * @param f
	 * @return the MD5 checksum of the entire file
	 * @throws IOException
	 */
	public static String getMD5(final File f) throws IOException {
		if (f == null || !f.isFile() || !f.canRead())
			throw new IOException("Cannot read from this file: " + f);

		MessageDigest md;
		try {
			md = MessageDigest.getInstance("MD5");
		} catch (final NoSuchAlgorithmException e1) {
			throw new IOException("Could not initialize MD5 digester", e1);
		}

		try (DigestInputStream dis = new DigestInputStream(new FileInputStream(f), md)) {
			final byte[] buff = new byte[10240];

			int cnt;

			do
				cnt = dis.read(buff);
			while (cnt == buff.length);

			final byte[] digest = md.digest();

			return String.format("%032x", new BigInteger(1, digest));
		} catch (final IOException ioe) {
			throw ioe;
		}
	}

	/**
	 * Download the file in a temporary location. The GUID should be filled with authorization tokens before calling this method.
	 *
	 * @param guid
	 * @return the temporary file name. You should handle the deletion of this temporary file!
	 * @see TempFileManager#release(File)
	 * @see #get(GUID, File)
	 * @see AuthorizationFactory#fillAccess(GUID, AccessType)
	 */
	public static File get(final GUID guid) {
		return get(guid, null);
	}

	private static final CachedThreadPool PARALLEL_DW_THREAD_POOL = new CachedThreadPool(Integer.MAX_VALUE, ConfigUtils.getConfig().getl("alien.io.IOUtils.PARALLEL_DW_THREAD_POOL.keepAliveTime", 2),
			TimeUnit.SECONDS, r -> {
				final Thread t = new Thread(r, "IOUtils.PARALLEL_DW_THREAD_POOL");
				t.setDaemon(true);

				return t;
			});

	/**
	 * Download the file in a specified location. The GUID should be filled with authorization tokens before calling this method.
	 *
	 * @param guid
	 * @param localFile
	 *            path where the file should be downloaded. Can be <code>null</code> in which case a temporary location will be used, but then you should handle the temporary files.
	 * @return the downloaded file, or <code>null</code> if the file could not be retrieved
	 * @see TempFileManager#release(File)
	 * @see AuthorizationFactory#fillAccess(GUID, AccessType)
	 */
	public static File get(final GUID guid, final File localFile) {
		final File cachedContent = TempFileManager.getAny(guid);

		if (cachedContent != null) {
			if (localFile == null)
				return cachedContent;

			try {
				if (!Utils.copyFile(cachedContent.getAbsolutePath(), localFile.getAbsolutePath())) {
					if (logger.isLoggable(Level.WARNING))
						logger.log(Level.WARNING, "Cannot copy " + cachedContent.getAbsolutePath() + " to " + localFile.getAbsolutePath());

					return null;
				}
			} finally {
				TempFileManager.release(cachedContent);
			}

			TempFileManager.putPersistent(guid, localFile);

			return localFile;
		}

		final Set<PFN> pfns = guid.getPFNs();

		if (pfns == null || pfns.size() == 0)
			return null;

		final Set<PFN> realPFNsSet = new HashSet<>();

		boolean zipArchive = false;

		for (final PFN pfn : pfns) {
			if (pfn.pfn.startsWith("guid:/") && pfn.pfn.indexOf("?ZIP=") >= 0)
				zipArchive = true;

			final Set<PFN> realPfnsTemp = pfn.getRealPFNs();

			if (realPfnsTemp == null || realPfnsTemp.size() == 0)
				continue;

			for (final PFN realPFN : realPfnsTemp)
				realPFNsSet.add(realPFN);
		}

		final String site = ConfigUtils.getCloseSite();

		File f = null;

		if (realPFNsSet.size() > 1 && guid.size < ConfigUtils.getConfig().getl("alien.io.IOUtils.parallel_downloads.size_limit", 10 * 1024 * 1024)
				&& PARALLEL_DW_THREAD_POOL.getActiveCount() < ConfigUtils.getConfig().geti("alien.io.IOUtils.parallel_downloads.threads", 100))
			f = parallelDownload(guid, realPFNsSet, zipArchive ? null : localFile);
		else {
			final List<PFN> sortedRealPFNs = SEUtils.sortBySite(realPFNsSet, site, false, false);

			for (final PFN realPfn : sortedRealPFNs) {
				if (realPfn.ticket == null)
					logger.log(Level.WARNING, "Missing ticket for " + realPfn.pfn);
				// try even if there is no read ticket since read is normally allowed without an access token

				final List<Protocol> protocols = Transfer.getAccessProtocols(realPfn);

				if (protocols == null || protocols.size() == 0)
					continue;

				for (final Protocol protocol : protocols)
					try {
						f = protocol.get(realPfn, zipArchive ? null : localFile);

						if (f != null)
							break;
					} catch (final IOException e) {
						logger.log(Level.INFO, "Failed to fetch " + realPfn.pfn + " by " + protocol, e);
					}

				if (f != null)
					break;
			}
		}

		if (f == null || !zipArchive)
			return f;

		try {
			for (final PFN p : pfns)
				if (p.pfn.startsWith("guid:/") && p.pfn.indexOf("?ZIP=") >= 0) {
					// this was actually an archive

					final String archiveFileName = p.pfn.substring(p.pfn.lastIndexOf('=') + 1);

					try (ZipInputStream zi = new ZipInputStream(new FileInputStream(f))) {
						ZipEntry zipentry = zi.getNextEntry();

						File target = null;

						while (zipentry != null) {
							if (zipentry.getName().equals(archiveFileName)) {
								if (localFile != null)
									target = localFile;
								else
									target = File.createTempFile(guid.guid + "#" + archiveFileName + ".", null, getTemporaryDirectory());

								final FileOutputStream fos = new FileOutputStream(target);

								final byte[] buf = new byte[8192];

								int n;

								while ((n = zi.read(buf, 0, buf.length)) > -1)
									fos.write(buf, 0, n);

								fos.close();
								zi.closeEntry();
								break;
							}

							zipentry = zi.getNextEntry();
						}

						if (target != null)
							if (localFile == null)
								TempFileManager.putTemp(guid, target);
							else
								TempFileManager.putPersistent(guid, localFile);

						return target;
					} catch (final ZipException e) {
						logger.log(Level.WARNING, "ZipException parsing the content of " + f.getAbsolutePath() + " of GUID " + guid.guid, e);
					} catch (final IOException e) {
						logger.log(Level.WARNING, "IOException extracting " + archiveFileName + " from " + f.getAbsolutePath() + " of GUID " + guid.guid + " to parse as ZIP", e);
					}

					return null;
				}
		} finally {
			TempFileManager.release(f);
		}

		return null;
	}

	private static final class DownloadWork implements Runnable {
		private final PFN realPfn;
		private final Object lock;
		private volatile File f;

		public DownloadWork(final PFN realPfn, final Object lock) {
			this.realPfn = realPfn;
			this.lock = lock;
		}

		@Override
		public void run() {
			final List<Protocol> protocols = Transfer.getAccessProtocols(realPfn);

			if (protocols == null || protocols.size() == 0)
				return;

			try {
				for (final Protocol protocol : protocols)
					try {
						f = protocol.get(realPfn, null);

						if (f != null)
							break;
					} catch (final IOException e) {
						logger.log(Level.FINE, "Failed to fetch " + realPfn.pfn + " by " + protocol, e);
					}

				if (f != null)
					synchronized (lock) {
						lock.notifyAll();
					}
			} catch (@SuppressWarnings("unused") final Throwable t) {
				if (f != null)
					TempFileManager.release(f);
			}
		}

		public File getLocalFile() {
			return f;
		}

		public PFN getPFN() {
			return realPfn;
		}
	}

	private static File parallelDownload(final GUID guid, final Set<PFN> realPFNsSet, final File localFile) {
		final List<Future<DownloadWork>> parallelDownloads = new ArrayList<>(realPFNsSet.size());

		final Object lock = new Object();

		File f = null;

		final List<DownloadWork> tasks = new ArrayList<>(realPFNsSet.size());

		for (final PFN realPfn : realPFNsSet) {
			if (realPfn.ticket == null) {
				logger.log(Level.WARNING, "Missing ticket for " + realPfn.pfn);
				continue; // no access to this guy
			}

			final DownloadWork dw = new DownloadWork(realPfn, lock);

			tasks.add(dw);

			final Future<DownloadWork> future = PARALLEL_DW_THREAD_POOL.submit(dw, dw);

			parallelDownloads.add(future);
		}

		while (f == null && parallelDownloads.size() > 0) {
			final Iterator<Future<DownloadWork>> it = parallelDownloads.iterator();

			while (it.hasNext()) {
				final Future<DownloadWork> future = it.next();

				if (future.isDone()) {
					try {
						final DownloadWork dw = future.get();

						tasks.remove(dw);

						f = dw.getLocalFile();

						if (logger.isLoggable(Level.FINER))
							if (f != null)
								logger.log(Level.FINER, "The first replica to reply was: " + dw.getPFN().pfn);
							else
								logger.log(Level.FINER, "This replica was not accessible: " + dw.getPFN().pfn);
					} catch (final InterruptedException e) {
						e.printStackTrace();
					} catch (final ExecutionException e) {
						e.printStackTrace();
					} finally {
						it.remove();
					}

					if (f != null)
						break;
				}
			}

			if (f == null)
				synchronized (lock) {
					try {
						lock.wait(100);
					} catch (@SuppressWarnings("unused") final InterruptedException e) {
						break;
					}
				}
		}

		for (final Future<DownloadWork> future : parallelDownloads)
			future.cancel(true);

		Thread.yield();

		for (final DownloadWork dw : tasks) {
			final File tempFile = dw.getLocalFile();

			if (logger.isLoggable(Level.FINEST))
				logger.log(Level.FINEST, "Got one file to test:" + tempFile);

			if (tempFile != null)
				if (f == null) {
					if (logger.isLoggable(Level.FINEST))
						logger.log(Level.FINEST, "Keeping this as the main instance:" + tempFile);

					f = tempFile;
				}
				else
					TempFileManager.release(tempFile);
		}

		if (localFile != null && f != null)
			if (lazyj.Utils.copyFile(f.getAbsolutePath(), localFile.getAbsolutePath())) {
				TempFileManager.release(f);

				TempFileManager.putPersistent(guid, localFile);

				return localFile;
			}

		return f;
	}

	/**
	 * @param guid
	 * @return the contents of the file, or <code>null</code> if there was a problem getting it
	 */
	public static String getContents(final GUID guid) {
		final String reason = AuthorizationFactory.fillAccess(guid, AccessType.READ);

		if (reason != null) {
			logger.log(Level.WARNING, "Access denied: " + reason);

			return null;
		}

		final File f = get(guid);

		if (f != null)
			try {
				return Utils.readFile(f.getCanonicalPath());
			} catch (@SuppressWarnings("unused") final IOException ioe) {
				// ignore, shouldn't be ...
			} finally {
				TempFileManager.release(f);
			}

		return null;
	}

	/**
	 * @param lfn
	 * @return the contents of the file, or <code>null</code> if there was a problem getting it
	 */
	public static String getContents(final LFN lfn) {
		if (lfn == null)
			return null;

		final GUID g = GUIDUtils.getGUID(lfn);

		if (g == null)
			return null;

		return getContents(g);
	}

	/**
	 * @param lfn
	 * @return the contents of the file, or <code>null</code> if there was a problem getting it
	 */
	public static String getContents(final String lfn) {
		return getContents(JAliEnCOMMander.getInstance().c_api.getLFN(lfn));
	}

	/**
	 * @param lfn
	 *            relative paths are allowed
	 * @param owner
	 * @return <code>true</code> if the indicated LFN doesn't exist (any more) in the catalogue and can be created again
	 */
	public static boolean backupFile(final String lfn, final AliEnPrincipal owner) {
		final String absolutePath = FileSystemUtils.getAbsolutePath(owner.getName(), null, lfn);

		final LFN l = JAliEnCOMMander.getInstance().c_api.getLFN(absolutePath, true);

		if (!l.exists)
			return true;

		final LFN backupLFN = JAliEnCOMMander.getInstance().c_api.getLFN(absolutePath + "~", true);

		if (backupLFN.exists && AuthorizationChecker.canWrite(backupLFN.getParentDir(), owner))
			if (!backupLFN.delete(true, false))
				return false;

		return LFNUtils.mvLFN(owner, l, absolutePath + "~") != null;
	}

	/**
	 * Upload a local file to the Grid
	 *
	 * @param localFile
	 * @param toLFN
	 * @param owner
	 * @throws IOException
	 */
	public static void upload(final File localFile, final String toLFN, final AliEnPrincipal owner) throws IOException {
		upload(localFile, toLFN, owner, 2, null, false);
	}

	/**
	 * Upload a local file to the Grid
	 *
	 * @param localFile
	 * @param toLFN
	 * @param owner
	 * @param replicaCount
	 * @throws IOException
	 */
	public static void upload(final File localFile, final String toLFN, final AliEnPrincipal owner, final int replicaCount) throws IOException {
		upload(localFile, toLFN, owner, replicaCount, null, false);
	}

	/**
	 * Upload a local file to the Grid
	 *
	 * @param localFile
	 * @param toLFN
	 * @param owner
	 * @param replicaCount
	 * @param progressReport
	 * @throws IOException
	 */
	public static void upload(final File localFile, final String toLFN, final AliEnPrincipal owner, final int replicaCount, final OutputStream progressReport) throws IOException {
		upload(localFile, toLFN, owner, replicaCount, progressReport, false);
	}

	/**
	 * Upload a local file to the Grid
	 *
	 * @param localFile
	 * @param toLFN
	 * @param owner
	 * @param replicaCount
	 * @param progressReport
	 * @param deleteSourceAfterUpload
	 *            if <code>true</code> then the local file (the source) is to be deleted after the operation completes
	 * @throws IOException
	 */
	public static void upload(final File localFile, final String toLFN, final AliEnPrincipal owner, final int replicaCount, final OutputStream progressReport, final boolean deleteSourceAfterUpload)
			throws IOException {
		final ArrayList<String> cpArgs = new ArrayList<>();

		if (deleteSourceAfterUpload)
			cpArgs.add("-d");

		cpArgs.add("-S");
		cpArgs.add("disk:" + replicaCount);

		upload(localFile, toLFN, owner, progressReport, cpArgs.toArray(new String[0]));
	}

	/**
	 * Upload a local file to the Grid
	 *
	 * @param localFile
	 *            local file to upload
	 * @param toLFN
	 *            catalogue entry name
	 * @param owner
	 *            owner of the new file
	 * @param progressReport
	 *            if you want progress report displayed (for user interface)
	 * @param args
	 *            other `cp` command parameters to pass
	 * @throws IOException
	 */
	public static void upload(final File localFile, final String toLFN, final AliEnPrincipal owner, final OutputStream progressReport, final String... args) throws IOException {
		final UIPrintWriter out = progressReport != null ? new PlainWriter(progressReport) : null;

		final JAliEnCOMMander cmd = new JAliEnCOMMander(owner, null, ConfigUtils.getCloseSite(), out);

		final LFN l = cmd.c_api.getLFN(toLFN, true);

		if (l.exists)
			throw new IOException("LFN already exists: " + toLFN);

		final String absolutePath = FileSystemUtils.getAbsolutePath(owner.getName(), null, toLFN);

		final ArrayList<String> cpArgs = new ArrayList<>();
		cpArgs.add("file:" + localFile.getAbsolutePath());
		cpArgs.add(absolutePath);

		if (args != null)
			for (final String arg : args)
				cpArgs.add(arg);

		final JAliEnCommandcp cp = new JAliEnCommandcp(cmd, cpArgs);

		cp.copyLocalToGrid(localFile, absolutePath);
	}

	/**
	 * @return the temporary directory where downloaded files are put by default
	 */
	public static final File getTemporaryDirectory() {
		final String sDir = ConfigUtils.getConfig().gets("alien.io.IOUtils.tempDownloadDir");

		if (sDir == null || sDir.length() == 0)
			return null;

		final File f = new File(sDir);

		if (f.exists() && f.isDirectory() && f.canWrite())
			return f;

		return null;
	}
}
