package alien.catalogue;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;
import alien.io.IOUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.taskQueue.JDL;
import alien.user.UserFactory;
import lazyj.DBFunctions;
import lazyj.Utils;
import lia.util.process.ExternalProcess.ExitStatus;
import utils.ProcessWithTimeout;

/**
 * @author ron
 * @since Nov 23, 2011
 */
public class PackageUtils {
	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(IndexTableEntry.class.getCanonicalName());

	/**
	 * Monitoring component
	 */
	static final Monitor monitor = MonitorFactory.getMonitor(IndexTableEntry.class.getCanonicalName());

	private static long lastCacheCheck = 0;

	private static Map<String, Package> packages = null;

	private static Set<String> cvmfsPackages = null;

	private static final class BackgroundPackageRefresher extends Thread {
		private final Object wakeupObject = new Object();

		public BackgroundPackageRefresher() {
			setName("alien.catalogue.PackageUtils.BackgroundPackageRefresher");
			setDaemon(true);
		}

		@Override
		public void run() {
			while (true) {
				synchronized (wakeupObject) {
					try {
						wakeupObject.wait(1000 * 60 * 60);
					}
					catch (@SuppressWarnings("unused") final InterruptedException e) {
						// ignore
					}
				}

				cacheCheck();
			}
		}

		public void wakeup() {
			synchronized (wakeupObject) {
				wakeupObject.notifyAll();
			}
		}
	}

	private static final BackgroundPackageRefresher BACKGROUND_REFRESHER = new BackgroundPackageRefresher();

	static {
		BACKGROUND_REFRESHER.start();
	}

	private static synchronized void cacheCheck() {
		if ((System.currentTimeMillis() - lastCacheCheck) > 1000 * 60) {
			final Map<String, Package> newPackages = new ConcurrentHashMap<>();

			try (DBFunctions db = ConfigUtils.getDB("alice_users")) {
				if (db != null) {
					if (monitor != null)
						monitor.incrementCounter("Package_db_lookup");

					final String q = "SELECT DISTINCT packageVersion, packageName, username, platform, lfn FROM PACKAGES ORDER BY 3,2,1,4,5;";

					db.setReadOnly(true);
					db.setQueryTimeout(60);

					if (!db.query(q))
						return;

					Package prev = null;

					while (db.moveNext()) {
						final Package next = new Package(db);

						if (prev != null && next.equals(prev))
							prev.setLFN(db.gets("platform"), db.gets("lfn"));
						else {
							next.setLFN(db.gets("platform"), db.gets("lfn"));
							prev = next;

							newPackages.put(next.getFullName(), next);
						}
					}
				}
			}

			final Set<String> newCvmfsPackages = new HashSet<>();

			try {
				final ProcessBuilder pBuilder = new ProcessBuilder("/cvmfs/alice.cern.ch/bin/alienv", "q");

				pBuilder.environment().put("LD_LIBRARY_PATH", "");
				pBuilder.environment().put("DYLD_LIBRARY_PATH", "");
				pBuilder.redirectErrorStream(false);

				final Process p = pBuilder.start();

				final ProcessWithTimeout pTimeout = new ProcessWithTimeout(p, pBuilder);

				pTimeout.waitFor(60, TimeUnit.SECONDS);

				final ExitStatus exitStatus = pTimeout.getExitStatus();

				if (exitStatus.getExtProcExitStatus() == 0) {
					final BufferedReader br = new BufferedReader(new StringReader(exitStatus.getStdOut()));

					String line;

					while ((line = br.readLine()) != null)
						newCvmfsPackages.add(line.trim());
				}
			}
			catch (final Throwable t) {
				logger.log(Level.WARNING, "Exception getting the CVMFS package list", t);
			}

			lastCacheCheck = System.currentTimeMillis();

			if (newPackages.size() > 0 || packages == null)
				packages = newPackages;

			if (newCvmfsPackages.size() > 0 || cvmfsPackages == null)
				cvmfsPackages = newCvmfsPackages;
		}
	}

	/**
	 * Force a reload of package list from the database.
	 */
	public static void refresh() {
		lastCacheCheck = 0;

		BACKGROUND_REFRESHER.wakeup();
	}

	/**
	 * @return list of defined packages
	 */
	public static List<Package> getPackages() {
		if (packages == null || packages.size() == 0)
			cacheCheck();
		else
			BACKGROUND_REFRESHER.wakeup();

		if (packages != null)
			return new ArrayList<>(packages.values());

		return null;
	}

	/**
	 * @return the set of known package names
	 */
	public static Set<String> getPackageNames() {
		if (packages == null || packages.size() == 0)
			cacheCheck();
		else
			BACKGROUND_REFRESHER.wakeup();

		if (packages != null)
			return packages.keySet();

		return null;
	}

	/**
	 * Get the Package object corresponding to the given textual description.
	 *
	 * @param name
	 *            package name, eg. "VO_ALICE@AliRoot::vAN-20140917"
	 * @return the corresponding Package object, if it exists
	 */
	public static Package getPackage(final String name) {
		if (packages == null || packages.size() == 0)
			cacheCheck();
		else
			BACKGROUND_REFRESHER.wakeup();

		if (packages != null)
			return packages.get(name);

		return null;
	}

	/**
	 * Get the set of packages registered in CVMFS (should be a subset of the AliEn packages)
	 *
	 * @return set of packages
	 */
	public static Set<String> getCvmfsPackages() {
		if (cvmfsPackages == null)
			cacheCheck();
		else
			BACKGROUND_REFRESHER.wakeup();

		return cvmfsPackages;
	}

	/**
	 * @param j
	 *            JDL to check
	 * @return <code>null</code> if the requirements are met and the JDL can be submitted, or a String object with the message detailing what condition was not met.
	 */
	public static String checkPackageRequirements(final JDL j) {
		if (j == null)
			return "JDL is null";

		if (packages == null)
			cacheCheck();
		else
			BACKGROUND_REFRESHER.wakeup();

		if (packages == null)
			return "Package list could not be fetched from the database";

		final Collection<String> packageVersions = j.getList("Packages");

		if (packageVersions == null || packageVersions.size() == 0)
			return null;

		for (final String requiredPackage : packageVersions) {
			if (!packages.containsKey(requiredPackage))
				return "Package not defined: " + requiredPackage;

			if (cvmfsPackages != null && cvmfsPackages.size() > 0 && !cvmfsPackages.contains(requiredPackage))
				return "Package not seen yet in CVMFS: " + requiredPackage;
		}

		return null;
	}

	/**
	 * @param user <code>null</code> to use the default, "VO_ALICE", or pass it explicitly
	 * @param name package name
	 * @param version package version
	 * @param platform reference platform
	 * @param dependencies comma-separated list of direct dependencies
	 * @param tarballURL where to take the package tarball from. Optional, can be <code>null</code> as we take everything from CVMFS
	 * @return the new (or existing) package
	 * @throws IOException in case the new package cannot be defined
	 */
	public static Package definePackage(final String user, final String name, final String version, final String platform, final String dependencies, final String tarballURL) throws IOException {
		final String packageBaseDir = "/alice/packages/" + name;

		final String packageWithVersionDir = packageBaseDir + "/" + version;

		final String platformArchive = packageWithVersionDir + "/" + platform;

		final String packageUser = user != null && !user.isBlank() ? user : "VO_ALICE";

		Package existing = getUncached(packageUser, name, version);

		if (existing != null && existing.isAvailable(platform))
			return existing;

		Set<String> tagTables = LFNUtils.getTagTableNames(packageWithVersionDir, "PackageDef", true);

		if (tagTables == null || tagTables.size() == 0) {
			try (DBFunctions db = ConfigUtils.getDB("alice_data")) {
				if (db != null) {
					if (!db.query("INSERT INTO TAG0 (tagName, path, tableName, user) VALUES ('PackageDef', ?, 'TadminVPackageDef', 'admin');", false, packageBaseDir)) {
						logger.log(Level.WARNING, "Failed to create TAG0 entry for " + packageBaseDir + ": " + db.getLastError());

						throw new IOException("Failed to create TAG0 entry for " + packageBaseDir);
					}

					tagTables = LFNUtils.getTagTableNames(packageWithVersionDir, "PackageDef", true);
				}
				else {
					logger.log(Level.WARNING, "No access to the alice_data db");
					throw new IOException("This is not a central service, cannot perform this operation");
				}
			}
		}

		if (tagTables == null || tagTables.size() == 0) {
			logger.log(Level.WARNING, "Cannot continue without a TAG0 entry for " + packageBaseDir);
			throw new IOException("No TAG0 entry for " + packageBaseDir);
		}

		if (existing == null) {
			// make sure the tag directory exists and insert an entry for this version

			final LFN dir = LFNUtils.mkdirs(UserFactory.getByUsername("admin"), packageWithVersionDir);

			if (dir == null) {
				logger.log(Level.SEVERE, "Cannot create base directory " + packageWithVersionDir);
				throw new IOException("Cannot create package version directory: " + packageWithVersionDir);
			}
		}

		LFN archiveCheck = LFNUtils.getLFN(platformArchive, true);

		if (!archiveCheck.exists) {
			if (tarballURL == null || tarballURL.isBlank()) {
				if (!LFNUtils.touchLFN(UserFactory.getByUsername("admin"), archiveCheck)) {
					logger.log(Level.WARNING, "Could not create empty LFN " + archiveCheck);
					throw new IOException("Could not create empty LFN " + archiveCheck);
				}
			}
			else {
				// download and register the file in the catalogue
				archiveCheck = fetchAndRegister(tarballURL, platformArchive);

				logger.log(Level.INFO, "Fetching " + tarballURL + " to " + platformArchive + " returned " + archiveCheck);
			}
		}

		if (archiveCheck != null && archiveCheck.exists && archiveCheck.isFile()) {
			// file exists, let's take it into account
			try (DBFunctions db = ConfigUtils.getDB("alice_users")) {
				if (db != null) {
					if (db.query("INSERT INTO PACKAGES (packageVersion, packageName, username, platform, lfn) VALUES (?, ?, ?, ?, ?);", false, version, name, packageUser, platform, platformArchive)) {
						existing = getUncached(packageUser, name, version);

						if (existing == null) {
							logger.log(Level.WARNING, "Could not read back the row I have just inserted: " + packageUser + ", " + name + ", " + version + ", " + platform + ", " + platformArchive);
							throw new IOException("Could not read back the row I have just inserted: " + packageUser + ", " + name + ", " + version + ", " + platform + ", " + platformArchive);
						}
					}
					else {
						logger.log(Level.WARNING, "Could not add the existing archive LFN to the existing package: " + platformArchive + ": " + db.getLastError());
						throw new IOException("Could not add " + platformArchive + " to the PACKAGES table: " + db.getLastError());
					}
				}
				else {
					logger.log(Level.WARNING, "I can't talk to the database for this operation");
					throw new IOException("No database configured");
				}
			}
		}

		if (existing != null) {
			final String table = tagTables.iterator().next();

			System.err.println(table);

			try (DBFunctions db = ConfigUtils.getDB("alice_users")) {
				if (db != null) {
					if (!db.query("REPLACE INTO " + table + " (file, dependencies) VALUES (?, ?);", false, packageWithVersionDir, dependencies)) {
						logger.log(Level.WARNING, "Failed to insert the dependencies for " + packageWithVersionDir + ": " + db.getLastError());
						throw new IOException("Failed to insert the dependencies for " + packageWithVersionDir + ": " + db.getLastError());
					}
				}
				else
					throw new IOException("No catalogue db");
			}

			if (packages != null)
				packages.put(existing.getFullName(), existing);
		}

		return existing;
	}

	private static Package getUncached(final String user, final String name, final String version) {
		try (DBFunctions db = ConfigUtils.getDB("alice_users")) {
			if (db != null) {
				final String q = "SELECT DISTINCT packageVersion, packageName, username, platform, lfn FROM PACKAGES WHERE username=? and packageName=? and packageVersion=? ORDER BY 3,2,1,4,5;";

				db.setReadOnly(true);
				db.setQueryTimeout(60);

				if (!db.query(q, false, user, name, version))
					return null;

				Package ret = null;

				while (db.moveNext()) {
					if (ret == null)
						ret = new Package(db);

					ret.setLFN(db.gets("platform"), db.gets("lfn"));
				}

				return ret;
			}
		}

		return null;
	}

	private static LFN fetchAndRegister(final String url, final String lfn) {
		File f;

		if (url.startsWith("file:")) {
			String fname = url.substring(5);

			if (fname.startsWith("//"))
				fname = fname.substring(2);

			f = new File(fname);
		}
		else if (url.startsWith("http://") || url.startsWith("https://")) {
			try {
				f = File.createTempFile("packages", "download");

				final String fname = Utils.download(url, f.getAbsolutePath());

				if (fname == null) {
					logger.log(Level.WARNING, "Could not download the content of " + url);
					return null;
				}
			}
			catch (final IOException ioe) {
				logger.log(Level.WARNING, "Exception downloading the content of " + url, ioe);
				return null;
			}
		}
		else {
			logger.log(Level.WARNING, "Unknown protocol in this URL: " + url);
			return null;
		}

		if (f.exists() && f.isFile() && f.canRead() && f.length() > 0)
			try {
				IOUtils.upload(f, lfn, UserFactory.getByUsername("admin"));
			}
			catch (final IOException e) {
				logger.log(Level.WARNING, "Could not upload " + f.getAbsolutePath() + " to " + lfn, e);
				return null;
			}

		return LFNUtils.getLFN(lfn);
	}
}
