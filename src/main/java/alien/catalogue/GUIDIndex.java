package alien.catalogue;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import lazyj.DBFunctions;

/**
 * One row from alice_users.GUIDINDEX
 *
 * @author costing
 * @since Nov 3, 2010
 */
public class GUIDIndex implements Serializable, Comparable<GUIDIndex> {

	/**
	 *
	 */
	private static final long serialVersionUID = -6989985910318299235L;

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(GUIDIndex.class.getCanonicalName());

	/**
	 * Monitoring component
	 */
	static transient final Monitor monitor = MonitorFactory.getMonitor(GUIDIndex.class.getCanonicalName());

	/**
	 * Host index ID
	 */
	public final int indexId;

	/**
	 * Host index
	 */
	public final int hostIndex;

	/**
	 * Table name
	 */
	public final int tableName;

	/**
	 * GUID time, in milliseconds
	 */
	public final long guidTime;

	/**
	 * Initialize from a DB query from alice_users.GUIDINDEX
	 *
	 * @param db
	 * @see CatalogueUtils#getGUIDIndex(long)
	 */
	GUIDIndex(final DBFunctions db) {
		indexId = db.geti("indexId");
		hostIndex = db.geti("hostIndex");
		tableName = db.geti("tableName");

		final String s = db.gets("guidTime");

		if (s.length() >= 8)
			guidTime = Long.parseLong(s.substring(0, 8), 16);
		else
			guidTime = 0;
	}

	@Override
	public int compareTo(final GUIDIndex o) {
		int diff = hostIndex - o.hostIndex;

		if (diff != 0)
			return diff;

		diff = tableName - o.tableName;

		if (diff != 0)
			return diff;

		diff = indexId - o.indexId;

		return diff;
	}

	@Override
	public boolean equals(final Object obj) {
		if (!(obj instanceof GUIDIndex))
			return false;

		return compareTo((GUIDIndex) obj) == 0;
	}

	@Override
	public int hashCode() {
		return 13 * hostIndex + 29 * tableName + 43 * indexId;
	}

	@Override
	public String toString() {
		return "GUIDIndex : hostIndex : " + hostIndex + "\n" + "tableName\t: " + tableName + "\n" + "indexId\t: " + indexId + "\n" + "guidTime\t: " + Long.toHexString(guidTime);
	}

	/**
	 * SE usage statistics for a SE (usage and file count)
	 *
	 * @author costing
	 * @since Nov 19, 2014
	 */
	public static class SEUsageStats {
		/**
		 * total amount of space allocated on the SE (as the catalogue sees it)
		 */
		public long usedSpace = 0;

		/**
		 * number of files on that SE
		 */
		public long fileCount = 0;

		/**
		 * Initial value
		 *
		 * @param usedSpace
		 * @param fileCount
		 */
		public SEUsageStats(final long usedSpace, final long fileCount) {
			this.usedSpace = usedSpace;
			this.fileCount = fileCount;
		}

		/**
		 * Merge with the results coming from the next G*L_PFN table
		 *
		 * @param other
		 */
		public void merge(final SEUsageStats other) {
			this.usedSpace += other.usedSpace;
			this.fileCount += other.fileCount;
		}
	}

	/**
	 * @return SE usage statistics for every (found) seNumber in the G*L_PFN tables
	 */
	public Map<Integer, SEUsageStats> getSEUsageStats() {
		final Map<Integer, SEUsageStats> ret = new HashMap<>();

		final Host h = CatalogueUtils.getHost(this.hostIndex);

		if (h == null)
			return ret;

		if (monitor != null)
			monitor.incrementCounter("GUID_seUsageStats");

		try (DBFunctions db = h.getDB()) {
			db.setReadOnly(true);

			db.query("select seNumber, sum(size),count(1) from G" + tableName + "L INNER JOIN G" + tableName + "L_PFN USING(guidId) GROUP BY seNumber;");

			while (db.moveNext()) {
				final Integer key = Integer.valueOf(db.geti(1));

				final SEUsageStats se = new SEUsageStats(db.getl(2), db.getl(3));

				ret.put(key, se);
			}
		}

		return ret;
	}
}