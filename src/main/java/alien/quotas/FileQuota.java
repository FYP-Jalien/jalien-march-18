package alien.quotas;

import java.io.Serializable;
import java.util.Set;

import lazyj.DBFunctions;
import lazyj.Format;
import lazyj.StringFactory;

/**
 * @author costing
 * @since 2012-03-30
 */
public final class FileQuota implements Serializable, Comparable<FileQuota> {

	/*
	 * user | varchar(64) totalSize | bigint(20) maxNbFiles | int(11) nbFiles | int(11) tmpIncreasedTotalSize | bigint(20) maxTotalSize | bigint(20) tmpIncreasedNbFiles | int(11)
	 */

	/**
	 *
	 */
	private static final long serialVersionUID = 7587668615003121402L;

	/**
	 * AliEn account name
	 */
	public final String user;

	/**
	 * Total size of the stored files
	 */
	public final long totalSize;

	/**
	 * Max number of files allowed
	 */
	public final long maxNbFiles;

	/**
	 * Current number of files stored by this user
	 */
	public final long nbFiles;

	/**
	 * Temp increase
	 */
	public final long tmpIncreasedTotalSize;

	/**
	 * Max allowed total size of this users' files
	 */
	public final long maxTotalSize;

	/**
	 * Temp increase
	 */
	public final int tmpIncreasedNbFiles;

	/**
	 * Fields allowed to modify via fquota set command
	 */
	private final static Set<String> allowed_to_update = Set.of("maxNbFiles", "maxTotalSize");

	/**
	 * @param db
	 */
	FileQuota(final DBFunctions db) {
		this.user = StringFactory.get(db.gets("user").toLowerCase());
		this.totalSize = db.getl("totalSize");
		this.maxNbFiles = db.getl("maxNbFiles");
		this.nbFiles = db.getl("nbFiles");
		this.tmpIncreasedTotalSize = db.getl("tmpIncreasedTotalSize");
		this.maxTotalSize = db.getl("maxTotalSize");
		this.tmpIncreasedNbFiles = db.geti("tmpIncreasedNbFiles");
	}

	@Override
	public int compareTo(final FileQuota o) {
		return this.user.compareTo(o.user);
	}

	@Override
	public String toString() {
		return "FQuota: user: " + user + "\n" + "totalSize\t: " + Format.size(totalSize) + "\n" + "maxTotalSize\t: " + Format.size(maxTotalSize) + "\n" + "tmpIncreasedTotalSize\t: "
				+ Format.size(tmpIncreasedTotalSize) + "\n" + "nbFiles\t: " + nbFiles + "\n" + "maxNbFiles\t: " + maxNbFiles + "\n" + "tmpIncreasedNbFiles\t: " + tmpIncreasedNbFiles;
	}

	@Override
	public boolean equals(final Object obj) {
		if (!(obj instanceof FileQuota))
			return false;

		return compareTo((FileQuota) obj) == 0;
	}

	@Override
	public int hashCode() {
		return user.hashCode();
	}

	/**
	 * @param noFiles
	 * @param size
	 * @return <code>true</code> if the user is allowed to upload this number of files with the given total size
	 */
	public boolean canUpload(final long noFiles, final long size) {
		if (totalSize + size <= (maxTotalSize + tmpIncreasedTotalSize) && nbFiles + noFiles <= (maxNbFiles + tmpIncreasedNbFiles))
			return true;

		return false;
	}

	/**
	 * @param fieldname
	 * @return <code>true</code> if the field is update-able
	 */
	public static boolean canUpdateField(final String fieldname) {
		return allowed_to_update.contains(fieldname);
	}
}
