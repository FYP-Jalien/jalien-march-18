package alien.io.protocols;

import lazyj.Format;

/**
 * @author costing
 * @since 2016-09-02
 */
public class SpaceInfo {
	/**
	 * Software vendor, if known (set to <code>null</code> if unknown)
	 */
	public String vendor = null;
	/**
	 * Software version
	 */
	public String version = null;

	/**
	 * Field set to <code>true</code> if the version information is available
	 */
	public boolean versionInfoSet = false;

	/**
	 * Path for space information
	 */
	public String path = null;
	/**
	 * Total storage space, in bytes
	 */
	public long totalSpace = 0;
	/**
	 * Free storage space, in bytes
	 */
	public long freeSpace = 0;
	/**
	 * Used space, in bytes
	 */
	public long usedSpace = 0;
	/**
	 * Largest free chunk, in bytes
	 */
	public long largestFreeChunk = 0;

	/**
	 * Field set to <code>true</code> if the space information fields are set
	 */
	public boolean spaceInfoSet = false;

	/**
	 * @param path
	 * @param totalSpace
	 * @param freeSpace
	 * @param usedSpace
	 * @param largestFreeChunk
	 */
	public void setSpaceInfo(final String path, final long totalSpace, final long freeSpace, final long usedSpace, final long largestFreeChunk) {
		this.path = path;
		this.totalSpace = totalSpace;
		this.freeSpace = freeSpace;
		this.usedSpace = usedSpace;
		this.largestFreeChunk = largestFreeChunk;
		this.spaceInfoSet = true;
	}

	/**
	 * @param vendor
	 * @param version
	 */
	public void setVersion(final String vendor, final String version) {
		this.vendor = vendor;
		this.version = version;
		this.versionInfoSet = true;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();

		if (spaceInfoSet) {
			sb.append("Path:  ").append(path);
			sb.append("\nTotal: ").append(Format.size(totalSpace)).append(" (LDAP setting: ").append(totalSpace / 1024).append(")");
			sb.append("\nFree:  ").append(Format.size(freeSpace));
			sb.append("\nUsed:  ").append(Format.size(usedSpace));
			sb.append("\nChunk: ").append(Format.size(largestFreeChunk)).append("\n");
		}

		if (versionInfoSet) {
			sb.append("Version: ");
			if (vendor != null)
				sb.append(vendor).append(" ");
			else
				sb.append("Unknown ");

			sb.append(version);
		}

		return sb.toString();
	}

	/**
	 * @param factor Internal replication factor to apply to the apparent size
	 */
	public void applyReplicaCorrection(double factor) {
		if (spaceInfoSet) {
			totalSpace = Math.round(totalSpace / factor);
			freeSpace = Math.round(freeSpace / factor);
			usedSpace = Math.round(usedSpace / factor);
		}
	}
}
