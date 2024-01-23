package alien.catalogue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import alien.config.ConfigUtils;
import lazyj.DBFunctions;
import lazyj.StringFactory;

/**
 * @author ron
 * @since Nov 23, 2011
 */
public class Package implements Comparable<Package>, Serializable {
	/**
	 *
	 */
	private static final long serialVersionUID = 1858434456566977987L;

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(Package.class.getCanonicalName());

	/**
	 * Whether or not this entry really exists in the catalogue
	 */
	public boolean exists = false;

	/**
	 * packageVersion
	 */
	public String packageVersion;

	/**
	 * packageName
	 */
	public String packageName;

	/**
	 * user
	 */
	public String user;

	/**
	 * Platform - to - LFN mapping
	 */
	private final Map<String, String> platforms = new HashMap<>(2);

	private final Map<String, Set<String>> deps = new HashMap<>(2);

	/**
	 * Comment extracted from the metadata JSON file. Can be <code>null</code> if not defined. Keeping any one comment from all defined platforms.
	 */
	public String packageComment;

	/**
	 * @param db
	 */
	public Package(final DBFunctions db) {
		init(db);

		hashCodeValue = getFullName().hashCode();
	}

	private final int hashCodeValue;

	@Override
	public int hashCode() {
		return hashCodeValue;
	}

	private void init(final DBFunctions db) {
		exists = true;

		packageVersion = StringFactory.get(db.gets("packageVersion"));

		packageName = StringFactory.get(db.gets("packageName"));

		user = StringFactory.get(db.gets("username"));

		packageComment = db.gets("packageComment", null);
	}

	@Override
	public String toString() {
		return getFullName() + ": " + platforms;
	}

	/**
	 * @return the full package name
	 */
	public String getFullName() {
		return user + "@" + packageName + "::" + packageVersion;
	}

	/**
	 * @return the package version
	 */
	public String getVersion() {
		return packageVersion;
	}

	/**
	 * @return the package (short) name
	 */
	public String getName() {
		return packageName;
	}

	/**
	 * @return the user/owner of the package
	 */
	public String getUser() {
		return user;
	}

	/**
	 * @return comment propagated from the user via the build system and a JSON file in CVMFS
	 */
	public String getComment() {
		return packageComment;
	}

	/**
	 * @param platform
	 * @return the LFN name of the package for the given platform
	 */
	public String getLFN(final String platform) {
		return platforms.get(platform);
	}

	/**
	 * @return the available platforms
	 */
	public Set<String> getPlatforms() {
		return platforms.keySet();
	}

	/**
	 * @param platform
	 * @return <code>true</code> if this package is available for the given platform
	 */
	public boolean isAvailable(final String platform) {
		return platforms.containsKey(platform);
	}

	/**
	 * Set the known package locations
	 *
	 * @param platform
	 * @param lfn
	 */
	void setLFN(final String platform, final String lfn) {
		platforms.put(platform, lfn);
	}

	@Override
	public boolean equals(final Object obj) {
		if (obj == null || !(obj instanceof Package))
			return false;

		if (this == obj)
			return true;

		final Package other = (Package) obj;

		return compareTo(other) == 0;
	}

	@Override
	public int compareTo(final Package arg0) {
		return getFullName().compareTo(arg0.getFullName());
	}

	/**
	 * Get the package names that are required by this package.
	 *
	 * @return the set of packages, for one of the available platforms
	 */
	public Set<String> getDependencies() {
		return getDependencies(null);
	}

	/**
	 * Get the package names that are required by this package.
	 *
	 * @param desiredPlatform from the available plaforms for this package, use this one if available. If not (or <code>null</code>) the first available value would be used.
	 *
	 * @return the set of packages, if possible for the indicated platform, otherwise an arbitrary one from the available ones
	 */
	public Set<String> getDependencies(final String desiredPlatform) {
		if (deps != null && deps.size() > 0) {
			if (desiredPlatform == null)
				return deps.values().iterator().next();

			final Set<String> platformDeps = deps.get(desiredPlatform);
			if (platformDeps != null)
				return platformDeps;
		}

		final Set<String> platformDeps = new HashSet<>();

		if (platforms.size() == 0)
			return platformDeps;

		final ArrayList<Map.Entry<String, String>> files = new ArrayList<>(platforms.size());

		for (final Map.Entry<String, String> entry : platforms.entrySet())
			if (entry.getKey().equals(desiredPlatform))
				files.add(0, entry);
			else
				files.add(entry);

		try (DBFunctions dbDeps = ConfigUtils.getDB("alice_data")) {
			dbDeps.setReadOnly(true);
			dbDeps.setQueryTimeout(60);

			for (final Map.Entry<String, String> entry : files) {
				final String dir = entry.getValue();

				for (final String tableName : LFNUtils.getTagTableNames(dir, "PackageDef", true)) {
					dbDeps.query("SELECT dependencies FROM " + tableName + " WHERE ? like concat(file,'%')", false, dir);

					while (dbDeps.moveNext()) {
						final StringTokenizer st = new StringTokenizer(dbDeps.gets(1), ", ");

						while (st.hasMoreTokens())
							platformDeps.add(st.nextToken());
					}

					if (platformDeps.size() > 0) {
						deps.put(entry.getKey(), platformDeps);
						return platformDeps;
					}
				}
			}
		}

		deps.put("<unknown>", platformDeps);
		return platformDeps;
	}
}
