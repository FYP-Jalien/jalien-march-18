package alien.catalogue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import lazyj.DBFunctions;
import lazyj.Format;
import lazyj.StringFactory;

/**
 * Wrapper around a row in INDEXTABLE
 *
 * @author costing
 */
public class IndexTableEntry implements Serializable, Comparable<IndexTableEntry> {

	/**
	 *
	 */
	private static final long serialVersionUID = -2978796807690712492L;

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(IndexTableEntry.class.getCanonicalName());

	/**
	 * Monitoring component
	 */
	static transient final Monitor monitor = MonitorFactory.getMonitor(IndexTableEntry.class.getCanonicalName());

	private static transient final boolean forceLFNIndexUsage = ConfigUtils.getConfig().getb("alien.catalogue.IndexTableEntry.forceLFNIndexUsage", true);

	/**
	 * Index id
	 */
	public final int indexId;

	/**
	 * Host and database where this table is located
	 */
	public final int hostIndex;

	/**
	 * Table name
	 */
	public final int tableName;

	/**
	 * LFN prefix
	 */
	public final String lfn;

	private final int hashCode;

	/**
	 * Initialize from one entry in INDEXTABLE
	 *
	 * @param db
	 */
	public IndexTableEntry(final DBFunctions db) {
		indexId = db.geti("indexId");
		hostIndex = db.geti("hostIndex");
		tableName = db.geti("tableName");
		lfn = StringFactory.get(db.gets("lfn"));

		hashCode = hostIndex * 13 + tableName * 29 + indexId * 43;
	}

	@Override
	public String toString() {
		return "IndexTableEntry indexId: " + indexId + "\n" + "hostIndex\t\t: " + hostIndex + "\n" + "tableName\t\t: " + tableName + "\n" + "lfn\t\t\t: " + lfn + "\n";
	}

	/**
	 * @return the database connection to this host/database
	 */
	public DBFunctions getDB() {
		final Host h = CatalogueUtils.getHost(hostIndex);

		if (h == null)
			return null;

		if (logger.isLoggable(Level.FINEST))
			logger.log(Level.FINEST, "Host is : " + h);

		return h.getDB();
	}

	/**
	 * Get the LFN from this table
	 *
	 * @param sPath
	 * @return the LFN, or <code>null</code> if it doesn't exist
	 */
	public LFN getLFN(final String sPath) {
		return getLFN(sPath, false);
	}

	/**
	 * Restrict how many LFNs can be queried in a single request
	 */
	static final int MAX_QUERY_LENGTH = 100;

	/**
	 * Get the LFN from this table
	 *
	 * @param ignoreFolders
	 * @param path
	 * @return the LFNs for the given paths, bulk extraction where possible
	 */
	public List<LFN> getLFNs(final boolean ignoreFolders, final List<String> path) {
		if (path == null || path.size() == 0)
			return null;

		List<LFN> retList = null;

		try (DBFunctions db = getDB()) {
			if (db == null)
				return null;

			if (monitor != null)
				monitor.incrementCounter("LFN_db_lookup");

			db.setReadOnly(true);

			final int queries = ((path.size() - 1) / MAX_QUERY_LENGTH) + 1;

			for (int chunk = 0; chunk < queries; chunk++) {
				final List<String> sublist = path.subList(chunk * MAX_QUERY_LENGTH, Math.min((chunk + 1) * MAX_QUERY_LENGTH, path.size()));

				final StringBuilder q = new StringBuilder("SELECT ");

				if (sublist.size() > 1)
					q.append("SQL_NO_CACHE ");

				q.append("* FROM L" + tableName + "L WHERE ");

				boolean first = true;

				for (String sSearch : sublist) {
					if (sSearch.startsWith("/"))
						sSearch = sSearch.substring(lfn.length());

					sSearch = Format.escSQL(sSearch);

					if (!first)
						q.append(" OR ");
					else
						first = false;

					q.append("lfn='").append(sSearch).append("'");

					if (!ignoreFolders && !sSearch.endsWith("/"))
						q.append(" OR lfn='").append(sSearch).append("/'");
				}

				if (!db.query(q.toString()))
					return null;

				if (retList == null)
					retList = new ArrayList<>(path.size());

				while (db.moveNext())
					retList.add(new LFN(db, this));
			}
		}

		return retList;
	}

	/**
	 * Get the LFN having the indicated GUID
	 *
	 * @param guid
	 * @return the LFN, if it exists in this table, or <code>null</code> if not
	 */
	public LFN getLFN(final UUID guid) {
		try (DBFunctions db = getDB()) {
			if (db == null)
				return null;

			if (monitor != null)
				monitor.incrementCounter("LFN_db_lookup");

			final String q = "SELECT * from L" + tableName + "L WHERE guid=string2binary(?);";

			db.setReadOnly(true);

			if (!db.query(q, false, guid.toString()))
				return null;

			if (!db.moveNext()) {
				if (logger.isLoggable(Level.FINE))
					logger.log(Level.FINE, "Empty result set for " + q + " and " + guid);

				return null;
			}

			return new LFN(db, this);
		}
	}

	/**
	 * Bulk operation to retrieve known LFNs for many UUIDs
	 *
	 * @param uuids
	 *            the GUIDs to search for
	 * @return the LFNs for which the GUID was in the given set. The number of the returned objects and the order of them does not reflect the input collection.
	 */
	public List<LFN> getLFNs(final Collection<UUID> uuids) {
		if (uuids == null || uuids.size() == 0)
			return null;

		List<LFN> ret = null;

		try (DBFunctions db = getDB()) {
			if (db == null)
				return null;

			if (monitor != null)
				monitor.incrementCounter("LFN_db_lookup");

			final List<UUID> uuidsAsList = (uuids instanceof List) ? (List<UUID>) uuids : new ArrayList<>(uuids);

			final int queries = ((uuidsAsList.size() - 1) / MAX_QUERY_LENGTH) + 1;

			for (int chunk = 0; chunk < queries; chunk++) {
				final List<UUID> sublist = uuidsAsList.subList(chunk * MAX_QUERY_LENGTH, Math.min((chunk + 1) * MAX_QUERY_LENGTH, uuidsAsList.size()));

				final StringBuilder sb = new StringBuilder("SELECT ");

				if (sublist.size() > 1)
					sb.append("SQL_NO_CACHE ");

				sb.append("* FROM L" + tableName + "L WHERE guid IN (");

				boolean first = true;

				for (final UUID u : sublist) {
					if (first)
						first = false;
					else
						sb.append(',');

					sb.append("string2binary('").append(u.toString()).append("')");
				}

				sb.append(");");

				db.setReadOnly(true);

				if (!db.query(sb.toString()))
					return null;

				if (ret == null)
					ret = new ArrayList<>();

				while (db.moveNext()) {
					ret.add(new LFN(db, this));
				}
			}
		}

		return ret;
	}

	/**
	 * Get the LFN from this table
	 *
	 * @param sPath
	 * @param evenIfDoesntExist
	 * @return the LFN, either the existing entry, or if <code>evenIfDoesntExist</code> is <code>true</code> then a bogus entry is returned
	 */
	public LFN getLFN(final String sPath, final boolean evenIfDoesntExist) {
		String sSearch = sPath;

		if (sSearch.startsWith("/"))
			sSearch = sSearch.substring(lfn.length());

		try (DBFunctions db = getDB()) {
			if (db == null)
				return null;

			if (monitor != null)
				monitor.incrementCounter("LFN_db_lookup");

			String q = "SELECT * FROM L" + tableName + "L WHERE lfn=?";

			db.setReadOnly(true);

			if (!sSearch.endsWith("/")) {
				q += " OR lfn=?";

				if (!db.query(q, false, sSearch, sSearch + "/"))
					return null;
			}
			else if (!db.query(q, false, sSearch))
				return null;

			if (!db.moveNext()) {
				if (logger.isLoggable(Level.FINE))
					logger.log(Level.FINE, "Empty result set for " + q + " and " + sSearch);

				if (evenIfDoesntExist)
					return new LFN(sPath, this);

				return null;
			}

			return new LFN(db, this);
		}
	}

	/**
	 * @param sPath
	 *            base path where to start searching, must be an absolute path ending in /
	 * @param sPattern
	 *            pattern to search for, in SQL wildcard format
	 * @param flags
	 *            a combination of {@link LFNUtils}.FIND_* fields
	 * @return the LFNs from this table that match
	 */
	public List<LFN> find(final String sPath, final String sPattern, final int flags) {
		return find(sPath, sPattern, flags, Long.valueOf(0), 0);
	}

	/**
	 * @param sPath
	 *            base path where to start searching, must be an absolute path ending in /
	 * @param sPattern
	 *            pattern to search for, in SQL wildcard format
	 * @param flags
	 *            a combination of {@link LFNUtils}.FIND_* fields
	 * @param queueid
	 *            a job id to filter for its files
	 * @param limit if strictly positive, restrict the number of returned entries to at most this number
	 * @return the LFNs from this table that match
	 */
	public List<LFN> find(final String sPath, final String sPattern, final int flags, final Long queueid, final long limit) {
		try (DBFunctions db = getDB()) {
			if (db == null)
				return null;

			if (monitor != null)
				monitor.incrementCounter("LFN_find");

			final List<LFN> ret = new ArrayList<>();

			String sSearch = sPath;

			if (sSearch.startsWith("/"))
				if (lfn.length() <= sSearch.length()) {
					sSearch = sSearch.substring(lfn.length());

					if (sSearch.startsWith("/"))
						sSearch = sSearch.substring(1);
				}
				else
					sSearch = "";

			String q = "SELECT * FROM L" + tableName + "L ";

			if (forceLFNIndexUsage)
				q += "FORCE INDEX (lfn)";

			q += " WHERE ";

			if ((flags & LFNUtils.FIND_REGEXP) == 0) {
				String sSearchAlternate = null;

				if (sSearch.length() == 0 && sPattern.startsWith("/")) {
					sSearch += sPattern.substring(1);
				}
				else {
					if (!sPattern.startsWith("%")) {
						if (sSearch.endsWith("/") && sPattern.startsWith("/"))
							sSearchAlternate = sSearch + sPattern.substring(1);

						sSearch += "%";
					}

					sSearch += sPattern;
				}

				if (!sPattern.endsWith("%")) {
					sSearch += "%";

					if (sSearchAlternate != null)
						sSearchAlternate += "%";
				}

				if (sSearchAlternate == null)
					q += "lfn LIKE '" + Format.escSQL(sSearch) + "'";
				else
					q += "(lfn LIKE '" + Format.escSQL(sSearch) + "' OR lfn LIKE '" + Format.escSQL(sSearchAlternate) + "')";

				q += " AND replicated=0";
			}
			else
				q += "lfn RLIKE '^" + Format.escSQL(sSearch + sPattern) + "' AND replicated=0";

			if ((flags & LFNUtils.FIND_INCLUDE_DIRS) == 0)
				q += " AND type!='d'";

			if ((flags & LFNUtils.FIND_FILTER_JOBID) != 0 && queueid != null && queueid.longValue() > 0)
				q += " AND jobid = " + queueid;

			if ((flags & LFNUtils.FIND_NO_SORT) == 0)
				q += " ORDER BY lfn";

			if (limit > 0)
				q += " LIMIT " + limit;

			db.setReadOnly(true);

			if (!db.query(q))
				return null;

			while (db.moveNext()) {
				final LFN l = new LFN(db, this);

				ret.add(l);
			}

			return ret;
		}
	}

	/**
	 * Get the LFN from this table
	 *
	 * @param entryId
	 * @return the LFN, or <code>null</code>
	 */
	public LFN getLFN(final long entryId) {
		try (DBFunctions db = getDB()) {
			db.setReadOnly(true);

			if (monitor != null)
				monitor.incrementCounter("LFN_db_lookup_entryId");

			if (!db.query("SELECT * FROM L" + tableName + "L WHERE entryId=?;", false, Long.valueOf(entryId)))
				return null;

			if (!db.moveNext())
				return null;

			return new LFN(db, this);
		}
	}

	@Override
	public int compareTo(final IndexTableEntry o) {
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
		if (!(obj instanceof IndexTableEntry))
			return false;

		return compareTo((IndexTableEntry) obj) == 0;
	}

	@Override
	public int hashCode() {
		return hashCode;
	}
}