package alien.catalogue;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.servlets.TextCache;
import alien.test.cassandra.DBCassandra;
import lazyj.DBFunctions;
import lazyj.cache.ExpirationCache;

/**
 * LFN implementation for Cassandra
 */
public class LFN_CSD implements Comparable<LFN_CSD>, CatalogEntity {
	/**
	 *
	 */
	private static final long serialVersionUID = 9158990164379160910L;

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(LFN_CSD.class.getCanonicalName());

	/**
	 * Monitoring component
	 */
	static transient final Monitor monitor = MonitorFactory.getMonitor(LFN_CSD.class.getCanonicalName());

	/**
	 * Root UUID
	 */
	static final UUID root_uuid = UUID.nameUUIDFromBytes("root".getBytes()); // 63a9f0ea-7bb9-3050-b96b-649e85481845

	/**
	 * Pool of PreparedStatements
	 */
	private static final HashMap<String, PreparedStatement> psPool = new HashMap<>();

	/**
	 * Folder booking map
	 */
	private static final ConcurrentHashMap<String, Integer> folderBookingMap = new ConcurrentHashMap<>();

	/**
	 * dirCache stats
	 */
	static int dirCache_get_hit = 0;
	/**
	 * dirCache stats
	 */
	static int dirCache_put = 0;

	/**
	 * Unique Ctime for auto insertion
	 */
	public static final Date ctime_fixed = new Date(1483225200000L);

	/**
	 * Modulo of seNumber to partition balance
	 */
	private static final int modulo_se_lookup = 100;

	/**
	 * Local cache to hold the hierarchy
	 */
	public static final ExpirationCache<String, UUID> dirCache = new ExpirationCache<>(80000);

	/**
	 * Local cache to hold recently inserted folders
	 */
	public static final ExpirationCache<String, Integer> rifs = new ExpirationCache<>(5000);

	/**
	 * lfn_index_table
	 */
	public static final String lfn_index_table = "catalogue.lfn_index";

	/**
	 * lfn_metadata_table
	 */
	public static final String lfn_metadata_table = "catalogue.lfn_metadata";

	/**
	 * lfn_ids_table
	 */
	public static final String lfn_ids_table = "catalogue.lfn_ids";

	/**
	 * se_lookup_table
	 */
	public static final String se_lookup_table = "catalogue.se_lookup";

	/**
	 * Owner
	 */
	public String owner;

	/**
	 * Last change timestamp
	 */
	public Date ctime;

	/**
	 * Size, in bytes
	 */
	public long size;

	/**
	 * Group
	 */
	public String gowner;

	/**
	 * File type
	 */
	public char type;

	/**
	 * Flag
	 */
	public int flag = 0;

	/**
	 * Access rights
	 */
	public String perm;

	/**
	 * MD5 checksum
	 */
	public String checksum;

	/**
	 * Whether or not this entry really exists in the catalogue
	 */
	public boolean exists = false;

	/**
	 * Canonical path
	 */
	public String canonicalName;

	/**
	 * Parent path (directory)
	 */
	public String path;

	/**
	 * Final part of path (name)
	 */
	public String child;

	/**
	 * id of the parent dir
	 */
	public UUID parent_id;

	/**
	 * id
	 */
	public UUID id;

	/**
	 * modulo in se_lookup
	 */
	public int modulo;

	/**
	 * Job ID that produced this file
	 *
	 * @since AliEn 2.19
	 */
	public long jobid;

	/**
	 * physical/logical locations
	 */
	public HashMap<Integer, String> pfns = null;

	/**
	 * physical locations
	 */
	public HashMap<String, String> metadata = null;

	/**
	 * @param l
	 */
	public LFN_CSD(LFN l) {
		this(l, false, false);
	}

	/**
	 * @param l
	 * @param getParent
	 * @param createParent
	 */
	public LFN_CSD(LFN l, boolean getParent, boolean createParent) {
		canonicalName = l.getCanonicalName();

		String[] p_c = getPathAndChildFromCanonicalName(canonicalName);
		path = p_c[0];
		child = p_c[1];

		size = l.getSize();
		jobid = l.jobid;
		checksum = l.getMD5();
		type = l.getType();
		perm = l.getPermissions();
		ctime = l.ctime;
		owner = l.getOwner();
		gowner = l.getGroup();
		exists = l.exists;
		id = l.guid;
		if (id != null)
			modulo = Math.abs(id.hashCode() % modulo_se_lookup);
		flag = 0;
		if (type != 'd')
			metadata = new HashMap<>();

		if (createParent) {
			try {
				if (!LFN_CSD.createDirectory(path, null, null, owner, gowner, jobid, perm, ctime)) {
					logger.severe("Cannot create LFN_CSD with createParent: " + l.getCanonicalName());
					return;
				}
			}
			catch (Exception e) {
				logger.severe("Exception trying to create LFN_CSD with createParent: " + l.getCanonicalName() + " Exception: " + e);
				return;
			}
		}

		if (getParent) {
			try {
				parent_id = getParentIdFromPath(path, null);
			}
			catch (Exception e) {
				logger.severe("Exception trying to create LFN_CSD with getParent: " + l.getCanonicalName() + e);
				return;
			}
			if (parent_id == null) {
				logger.severe("Exception trying to create LFN_CSD with getParent: " + l.getCanonicalName());
				return;
			}
		}
	}

	/**
	 * @param lfn
	 * @param getFromDB
	 * @param append_table
	 * @param p_id
	 * @param c_id
	 */
	public LFN_CSD(String lfn, boolean getFromDB, String append_table, UUID p_id, UUID c_id) {
		canonicalName = lfn;

		if (canonicalName.endsWith("/"))
			type = 'd';
		else
			type = 'u';

		String[] p_c = getPathAndChildFromCanonicalName(canonicalName);
		path = p_c[0];
		child = p_c[1];

		if (getFromDB) {
			String tm = lfn_metadata_table;
			if (append_table != null) {
				tm += append_table;
			}

			try {
				parent_id = (p_id != null ? p_id : getParentIdFromPath(path, append_table));
				if (parent_id == null)
					return;

				id = (c_id != null ? c_id : getChildIdFromParentIdAndName(parent_id, child, append_table));
				if (id == null)
					return;

				@SuppressWarnings("resource")
				final Session session = DBCassandra.getInstance();
				if (session == null)
					return;

				PreparedStatement statement = getOrInsertPreparedStatement(session,
						"select checksum,ctime,gowner,jobid,metadata,owner,perm,pfns,size,type from " + tm + " where parent_id = ? and id = ?");
				BoundStatement boundStatement = new BoundStatement(statement);
				boundStatement.bind(this.parent_id, this.id);

				boundStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);

				ResultSet results = session.execute(boundStatement);
				init(results.one());
			}
			catch (Exception e) {
				logger.severe("Exception trying to create LFN_CSD: " + e);
				return;
			}
		}
	}

	/**
	 * @param session
	 * @param querykey
	 * @return preparedstatement for the query
	 */
	public static PreparedStatement getOrInsertPreparedStatement(Session session, String querykey) {
		if (psPool.containsKey(querykey))
			return psPool.get(querykey);

		synchronized (psPool) { // we lock the pool only if we need to insert
			if (psPool.containsKey(querykey)) // in case other thread past the first condition
				return psPool.get(querykey);

			PreparedStatement ps = session.prepare(querykey);
			logger.info("Adding PreparedStatement to pool: " + querykey);
			psPool.put(querykey, ps);
			return ps;
		}
	}

	/**
	 * @param canonicalName
	 * @return parent dir(0) and name of last chunk of a path(1)
	 */
	public static String[] getPathAndChildFromCanonicalName(String canonicalName) {
		int remove = 0;
		int idx = canonicalName.lastIndexOf('/');
		if (idx == canonicalName.length() - 1) {
			idx = canonicalName.lastIndexOf('/', idx - 1);
			remove = 1;
		}
		String[] p_c = new String[2];
		p_c[0] = canonicalName.substring(0, idx + 1); // parent dir without trailing slash
		p_c[1] = canonicalName.substring(idx + 1, canonicalName.length() - remove); // last part of path without trailing slash
		return p_c;
	}

	/**
	 * @param row
	 */
	public LFN_CSD(Row row) {
		init(row);
	}

	private void init(Row row) {
		if (row == null) {
			logger.log(Level.SEVERE, "Row null creating LFN_CSD ");
			return;
		}

		try {
			pfns = (HashMap<Integer, String>) row.getMap("pfns", Integer.class, String.class);
			metadata = (HashMap<String, String>) row.getMap("metadata", String.class, String.class);
			type = row.getString("type").charAt(0);
			checksum = row.getString("checksum");
			perm = row.getString("perm");
			jobid = row.getLong("jobid");
			size = row.getLong("size");
			ctime = row.getTimestamp("ctime");
			owner = row.getString("owner");
			gowner = row.getString("gowner");
			if (type == 'd' && !canonicalName.endsWith("/"))
				canonicalName += "/";
			modulo = Math.abs(id.hashCode() % modulo_se_lookup);
			exists = true;
		}
		catch (Exception e) {
			logger.log(Level.SEVERE, "Can't create LFN_CSD from row: " + e);
		}
	}

	/**
	 * @param path_parent
	 * @param append_table
	 * @return id from the parent
	 * @throws Exception
	 *             if exception from the driver
	 */
	@SuppressWarnings("resource")
	public static UUID getParentIdFromPath(String path_parent, String append_table) throws Exception {
		String t = lfn_index_table;
		if (append_table != null) {
			t += append_table;
		}

		String parent_path = path_parent;
		try {
			UUID path_id = root_uuid;

			parent_path = parent_path.replaceAll("//", "/");
			if (parent_path.equals("/"))
				return path_id;

			parent_path = parent_path.replaceAll("^/", "");
			String[] path_chunks = parent_path.split("/");

			Session session = DBCassandra.getInstance();
			if (session == null)
				return null;

			// We loop from root until we reach our dir
			String pathAppended = "/";
			int chunksize = path_chunks.length;
			for (int i = 0; i < chunksize + 1; i++) {
				final UUID cachedUuid = dirCache.get(pathAppended);
				if (cachedUuid == null) {
					PreparedStatement statement = getOrInsertPreparedStatement(session, "select child_id from " + t + " where path_id = ? and path = ?");
					BoundStatement boundStatement = new BoundStatement(statement);
					boundStatement.bind(path_id, (i == 0 ? "/" : path_chunks[i - 1]));
					boundStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);
					ResultSet results = session.execute(boundStatement);
					if (results.getAvailableWithoutFetching() != 1)
						return null;

					path_id = results.one().getUUID("child_id");
					if (path_id == null) {
						logger.severe("Error getting parent id for path_id for path: " + (i == 0 ? "/" : path_chunks[i - 1]));
						return null;
					}

					dirCache.put(pathAppended, path_id, 5 * 60 * 1000); // 5 minutes
					dirCache_put++;
				}
				else {
					path_id = cachedUuid;
					dirCache_get_hit++;
				}
				if (i < chunksize)
					pathAppended += path_chunks[i] + "/";
			}
			return path_id;
		}
		catch (Exception e) {
			logger.severe("Exception trying to getParentIdFromPath (" + parent_path + ") LFN_CSD: " + e);
			throw (e);
		}
	}

	/**
	 * @param parent_id
	 * @param name
	 * @param append_table
	 * @return child id in the index
	 * @throws Exception
	 */
	@SuppressWarnings("resource")
	public static UUID getChildIdFromParentIdAndName(UUID parent_id, String name, String append_table) throws Exception {
		String t = lfn_index_table;
		if (append_table != null) {
			t += append_table;
		}

		try {
			Session session = DBCassandra.getInstance();
			if (session == null)
				return null;

			PreparedStatement statement = getOrInsertPreparedStatement(session, "select child_id from " + t + " where path_id = ? and path = ?");
			BoundStatement boundStatement = new BoundStatement(statement);
			boundStatement.bind(parent_id, name);
			boundStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);
			ResultSet results = session.execute(boundStatement);
			if (results.getAvailableWithoutFetching() != 1)
				return null;

			UUID id = results.one().getUUID("child_id");
			if (id == null)
				return null;

			return id;
		}
		catch (Exception e) {
			logger.severe("Exception trying to getChildIdFromParentIdAndName (" + parent_id + " ," + name + ") LFN_CSD: " + e);
			throw (e);
		}
	}

	@Override
	public String toString() {
		String str = "LFN (" + canonicalName + "): " + path + " " + child + "\n - Type: " + type;

		// if (type != 'd') {
		str += "\n - Size: " + size + "\n - Checksum: " + checksum + "\n - Perm: " + perm + "\n - Owner: " + owner + "\n - Gowner: " + gowner + "\n - JobId: " + jobid + "\n - Id: " + id + "\n - pId: "
				+ parent_id + "\n - Ctime: " + ctime + "\n - Modulo: " + modulo;
		if (pfns != null)
			str += "\n - pfns: " + pfns.toString();
		if (metadata != null)
			str += "\n - metadata: " + metadata.toString() + "\n";
		// }

		return str;
	}

	@Override
	public String getOwner() {
		return owner;
	}

	@Override
	public String getGroup() {
		return gowner;
	}

	@Override
	public String getPermissions() {
		return perm != null ? perm : "755";
	}

	@Override
	public String getName() {
		return child;
	}

	/**
	 * Get the canonical name (full path and name)
	 *
	 * @return canonical name
	 */
	public String getCanonicalName() {
		return canonicalName;
	}

	@Override
	public char getType() {
		return type;
	}

	@Override
	public long getSize() {
		return size;
	}

	@Override
	public String getMD5() {
		return checksum;
	}

	@Override
	public int compareTo(final LFN_CSD o) {
		if (this == o)
			return 0;

		return canonicalName.compareTo(o.canonicalName);
	}

	@Override
	public boolean equals(final Object obj) {
		if (obj == null || !(obj instanceof LFN_CSD))
			return false;

		if (this == obj)
			return true;

		final LFN_CSD other = (LFN_CSD) obj;

		return compareTo(other) == 0;
	}

	@Override
	public int hashCode() {
		return Integer.parseInt(perm) * 13 + canonicalName.hashCode() * 17;
	}

	/**
	 * is this LFN a directory
	 *
	 * @return <code>true</code> if this LFN is a directory
	 */
	public boolean isDirectory() {
		return (type == 'd');
	}

	/**
	 * @return <code>true</code> if this LFN points to a file
	 */
	public boolean isFile() {
		return (type == 'f' || type == '-');
	}

	/**
	 * @return <code>true</code> if this is a native collection
	 */
	public boolean isCollection() {
		return type == 'c';
	}

	/**
	 * @return <code>true</code> if this is a a member of an archive
	 */
	public boolean isMemberOfArchive() {
		return type == 'm';
	}

	/**
	 * @return <code>true</code> if this is an archive
	 */
	public boolean isArchive() {
		return type == 'a';
	}

	/**
	 * @return the list of entries in this folder
	 */
	public List<LFN_CSD> list() {
		return list(false, null, null);
	}

	/**
	 * @param get_metadata
	 * @return list of lfns
	 */
	public List<LFN_CSD> list(boolean get_metadata) {
		return list(get_metadata, null, null);
	}

	/**
	 * @param get_metadata
	 * @param append_table
	 * @param level
	 * @return list of LFNs from this table
	 */
	public List<LFN_CSD> list(boolean get_metadata, String append_table, ConsistencyLevel level) {
		if (!exists)
			return null;

		if (monitor != null)
			monitor.incrementCounter("LFN_CSD_list");

		final List<LFN_CSD> ret = new ArrayList<>();
		if (type != 'd' && (!get_metadata || this.perm != null)) {
			ret.add(this);
			return ret;
		}

		String t = lfn_index_table;
		if (append_table != null) {
			t += append_table;
		}

		ConsistencyLevel cl = ConsistencyLevel.QUORUM;
		if (level != null)
			cl = level;

		try {
			if (parent_id == null)
				parent_id = getParentIdFromPath(path, append_table);
			if (parent_id == null)
				return null;

			if (id == null)
				id = getChildIdFromParentIdAndName(parent_id, child, append_table);
			if (id == null)
				return null;

			if (type == 'd') {
				@SuppressWarnings("resource")
				final Session session = DBCassandra.getInstance();
				if (session == null)
					return null;

				PreparedStatement statement = getOrInsertPreparedStatement(session, "select path,child_id from " + t + " where path_id = ?");
				BoundStatement boundStatement = new BoundStatement(statement);
				boundStatement.bind(this.id);

				boundStatement.setConsistencyLevel(cl);

				ResultSet results = session.execute(boundStatement);
				for (Row row : results) {
					// LFN_CSD String lfn, boolean getFromDB, UUID p_id, UUID c_id
					ret.add(new LFN_CSD(this.canonicalName + row.getString("path"), get_metadata, append_table, this.id, row.getUUID("child_id")));
				}
			}
			else {
				ret.add(new LFN_CSD(this.canonicalName, get_metadata, append_table, this.parent_id, this.id));
			}
		}
		catch (Exception e) {
			logger.severe("Exception trying to whereis: " + e);
			return null;
		}

		return ret;
	}

	/**
	 * @return physical locations of the file
	 */
	public HashMap<Integer, String> whereis() {
		return whereis(null, null);
	}

	/**
	 * @param append_table
	 * @param level
	 * @return physical locations of the file
	 */
	public HashMap<Integer, String> whereis(String append_table, ConsistencyLevel level) {
		if (!exists || type == 'd')
			return null;

		String t = lfn_metadata_table;
		if (append_table != null)
			t += append_table;

		ConsistencyLevel cl = ConsistencyLevel.QUORUM;
		if (level != null)
			cl = level;

		try {
			if (parent_id == null)
				parent_id = getParentIdFromPath(path, append_table);
			if (parent_id == null)
				return null;

			if (id == null)
				id = getChildIdFromParentIdAndName(parent_id, child, append_table);
			if (id == null)
				return null;

			@SuppressWarnings("resource")
			Session session = DBCassandra.getInstance();
			if (session == null)
				return null;

			PreparedStatement statement = getOrInsertPreparedStatement(session, "select pfns from " + t + " where parent_id = ? and id = ?");
			BoundStatement boundStatement = new BoundStatement(statement);
			boundStatement.bind(this.parent_id, this.id);

			boundStatement.setConsistencyLevel(cl);

			ResultSet results = session.execute(boundStatement);
			for (Row row : results) {
				pfns = (HashMap<Integer, String>) row.getMap("pfns", Integer.class, String.class);
			}
		}
		catch (Exception e) {
			logger.severe("Exception trying to whereis: " + e);
			return null;
		}

		return pfns;
	}

	/**
	 * @return insertion result
	 */
	public boolean insert() {
		return this.insert(null, null);
	}

	/**
	 * @param append_table
	 * @param level
	 * @return insertion result
	 */
	public boolean insert(String append_table, ConsistencyLevel level) {
		// lfn | ctime | dir | gowner | jobid | link | md5 | owner | perm | pfns
		// | size | type
		boolean res = false;

		String tindex = lfn_index_table;
		if (append_table != null)
			tindex += append_table;

		String tids = lfn_ids_table;
		if (append_table != null)
			tids += append_table;

		String t = lfn_metadata_table;
		if (append_table != null)
			t += append_table;

		String ts = se_lookup_table;
		if (append_table != null)
			ts += append_table;

		ConsistencyLevel cl = ConsistencyLevel.QUORUM;
		if (level != null)
			cl = level;

		if (this.parent_id == null) {
			try {
				this.parent_id = getParentIdFromPath(this.path, append_table);
			}
			catch (Exception e) {
				logger.severe("Exception while inserting: " + this.canonicalName + " Exception: " + e);
				return false;
			}
		}

		if (this.parent_id == null) {
			logger.severe("Cannot get parent of " + this.path + " append_table: " + append_table);
			return false;
		}

		if (this.id == null)
			id = UUID.randomUUID();

		try {
			@SuppressWarnings("resource")
			final Session session = DBCassandra.getInstance();
			if (session == null)
				return false;

			BatchStatement bs = new BatchStatement(BatchStatement.Type.LOGGED);
			bs.setConsistencyLevel(cl);

			this.prepareInsertStatements(bs, session, true, true, tindex, tids, t, ts);

			ResultSet rs = session.execute(bs);
			res = rs.wasApplied();
		}
		catch (Exception e) {
			logger.severe("Exception trying to insert: " + e);
			return false;
		}

		if (res)
			exists = true;

		return res;
	}

	private BatchStatement prepareInsertStatements(BatchStatement bs, Session session, final boolean insert_se_lookup, final boolean insert_metadata, final String tindex, final String tids,
			final String t, final String ts) {
		PreparedStatement statement;
		BoundStatement boundStatement;

		// Insert the entry in the index // TODO: (double-check) use IF NOT EXISTS to avoid collisions when inserting paths ?
		statement = getOrInsertPreparedStatement(session, "INSERT INTO " + tindex + " (path_id,path,ctime,child_id,flag)" + " VALUES (?,?,?,?,?)");
		bs.add(statement.bind(parent_id, child, ctime, id, Integer.valueOf(flag)));

		// Insert the entry in the ids // TODO: (double-check) use IF NOT EXISTS to avoid collisions when inserting paths ?
		// if (!isDirectory()) {
		statement = getOrInsertPreparedStatement(session, "INSERT INTO " + tids + " (child_id,path_id,path,ctime,flag)" + " VALUES (?,?,?,?,?)");
		bs.add(statement.bind(id, parent_id, child, ctime, Integer.valueOf(flag)));
		// }

		// Insert the entry in the metadata
		if (insert_metadata) {
			if (type == 'a' || type == 'f' || type == 'm' || type == 'l') {
				statement = getOrInsertPreparedStatement(session,
						"INSERT INTO " + t + " (parent_id, id, ctime, gowner, jobid, checksum, owner, perm, pfns, size, type, metadata)" + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)");
				boundStatement = new BoundStatement(statement);
				boundStatement.bind(parent_id, id, ctime, gowner, Long.valueOf(jobid), checksum, owner, perm, pfns, Long.valueOf(size), String.valueOf(type), metadata);
			}
			else { // 'd'
				statement = getOrInsertPreparedStatement(session, "INSERT INTO " + t + " (parent_id, id, ctime, gowner, jobid, owner, perm, size, type)" + " VALUES (?,?,?,?,?,?,?,?,?)");
				boundStatement = new BoundStatement(statement);
				boundStatement.bind(parent_id, id, ctime, gowner, Long.valueOf(jobid), owner, perm, Long.valueOf(size), String.valueOf(type));
			}

			bs.add(boundStatement);
		}

		// Fake -1 seNumber for dirs in se_lookup
		if (type == 'd') {
			// pfns = new HashMap<>();
			// pfns.put(Integer.valueOf(-1), "");
			dirCache.put(this.canonicalName, id, 5 * 60 * 1000); // 5 minutes
			dirCache_put++;
		}

		// Insert into se_lookup
		if (insert_se_lookup && (type == 'a' || type == 'f') && size > 0 && pfns != null) {
			int moduloc = Math.abs(id.hashCode() % modulo_se_lookup);

			final Set<Integer> seNumbers = pfns.keySet();

			for (int seNumber : seNumbers) {
				statement = getOrInsertPreparedStatement(session, "INSERT INTO " + ts + " (seNumber, modulo, id, size, owner)" + " VALUES (?,?,?,?,?)");
				bs.add(statement.bind(Integer.valueOf(seNumber), Integer.valueOf(moduloc), id, Long.valueOf(size), owner));
			}
		}
		return bs;
	}

	/**
	 * @param folder
	 * @param table
	 * @param level
	 * @return <code>true</code> if the directory exists or was successfully created now
	 * @throws Exception
	 *             if timeouts on inner methods
	 */
	public static boolean createDirectory(String folder, String table, ConsistencyLevel level) throws Exception {
		return createDirectory(folder, table, level, null, null, 0, null, null);
	}

	/**
	 * @param folder
	 * @param table
	 * @param level
	 * @param owner
	 * @param gowner
	 * @param jobid
	 * @param perm
	 * @param ctime
	 * @return <code>true</code> if the directory exists or was successfully created now
	 * @throws Exception
	 *             if timeouts on inner methods
	 */
	public static boolean createDirectory(final String folder, final String table, final ConsistencyLevel level, final String owner, final String gowner, final long jobid, final String perm,
			Date ctime) throws Exception {
		// We want to create the whole hierarchy upstream
		// check if is already there
		int bookingTries = 0;

		if (folder.length() <= 1 || existsLfn(folder, table))
			return true;

		while (folderBookingMap.putIfAbsent(folder, Integer.valueOf(1)) != null) {
			Thread.sleep(1000 * bookingTries);
			bookingTries++;
			if (bookingTries == 3) {
				logger.severe("Can't get booking lock: " + folder);
				return false;
			}
		}

		if (rifs.get(folder) != null)
			return true;

		try {
			// return if in the cache
			if (dirCache.get(folder) != null) {
				dirCache_get_hit++;
				return true;
			}

			// get parent and create it if doesn't exist
			String[] p_c = getPathAndChildFromCanonicalName(folder);
			String path = p_c[0];

			if (!createDirectory(path, table, level, owner, gowner, jobid, perm, ctime)) {
				logger.severe("Can't create directory: " + path);
				return false;
			}

			LFN_CSD newdir = new LFN_CSD(folder, false, table, null, null);
			if (owner != null && gowner != null && perm != null && ctime != null) {
				newdir.gowner = gowner;
				newdir.owner = owner;
				newdir.jobid = jobid;
				newdir.perm = perm;
				newdir.ctime = ctime;
			}
			else {
				newdir.gowner = "aliprod";
				newdir.owner = "aliprod";
				newdir.jobid = 0l;
				newdir.perm = "755";
				newdir.ctime = ctime_fixed;
			}
			newdir.checksum = "";
			newdir.size = 0;
			newdir.flag = 0;
			newdir.type = 'd';

			return newdir.insert(table, level);
		}
		catch (Exception e) {
			logger.severe("Freeing folderBookingMap due to failing inner createDirectory or insert for : " + folder);
			throw (e);
		}
		finally {
			folderBookingMap.remove(folder);
			rifs.put(folder, Integer.valueOf(1), 30 * 1000); // 30 seconds
		}
	}

	/**
	 * @param lfn
	 * @param append_table
	 * @return <code>true</code> if the LFN exists
	 * @throws Exception
	 *             passing through from other methods explicitely
	 */
	public static boolean existsLfn(String lfn, String append_table) throws Exception {
		if (dirCache.get(lfn) != null) { // If the cache has the folder, return directly
			dirCache_get_hit++;
			return true;
		}

		String[] p_c = getPathAndChildFromCanonicalName(lfn);
		String parent_of_lfn = p_c[0];
		String child_of_lfn = p_c[1];

		UUID pid = null;
		try {
			pid = getParentIdFromPath(parent_of_lfn, append_table);
		}
		catch (Exception e) {
			logger.severe("Exception from getParentIdFromPath in existsLfn for: " + lfn);
			throw (e);
		}
		if (pid == null)
			return false;

		UUID idfolder = null;

		try {
			idfolder = getChildIdFromParentIdAndName(pid, child_of_lfn, append_table);
		}
		catch (Exception e) {
			logger.severe("Exception from getChildIdFromParentIdAndName in existsLfn for: " + lfn);
			throw (e);
		}
		if (idfolder == null)
			return false;

		return true;
	}

	/**
	 * @return dirCache get
	 */
	public static int dirCacheGet() {
		return dirCache_get_hit;
	}

	/**
	 * @return dirCache put
	 */
	public static int dirCachePut() {
		return dirCache_put;
	}

	/**
	 * @return dirCache size
	 */
	public static int dirCacheSize() {
		return dirCache.size();
	}

	/**
	 * Delete this LFN in the Catalogue
	 *
	 * @param purge
	 *            physically delete the PFNs
	 * @param recursive
	 *            for directories, remove all subentries. <B>This code doesn't check permissions, do the check before!</B>
	 * @param notifyCache
	 *            whether or not to notify AliEn's access and whereis caches of removed entries
	 *
	 * @return <code>true</code> if this LFN entry was deleted in the database
	 */
	@SuppressWarnings("resource")
	public boolean delete(final boolean purge, final boolean recursive, final boolean notifyCache) {
		if (!exists)
			throw new IllegalAccessError("You asked to delete an LFN that doesn't exist!");

		if (isDirectory() && !recursive) {
			logger.info("LFN_CSD: asked to delete a folder non-recursively: " + getCanonicalName());
			return false;
		}

		try {
			final Session session = DBCassandra.getInstance();
			if (session == null) {
				logger.severe("LFN_CSD: delete: could not get an instance: " + getCanonicalName());
				return false;
			}

			BatchStatement bs = new BatchStatement(BatchStatement.Type.LOGGED);
			bs.setConsistencyLevel(ConsistencyLevel.QUORUM);
			PreparedStatement statement;

			// Delete the entry from index and metadata
			statement = getOrInsertPreparedStatement(session, "DELETE FROM " + lfn_index_table + " WHERE path_id=? AND path=?");
			bs.add(statement.bind(this.parent_id, this.child));

			statement = getOrInsertPreparedStatement(session, "DELETE FROM " + lfn_metadata_table + " WHERE parent_id=? AND id=?");
			bs.add(statement.bind(this.parent_id, this.id));

			// Delete the entry from ids and se_lookup. Will be deleted from the hierarchy (index, metadata) using the parent folder
			statement = getOrInsertPreparedStatement(session, "DELETE FROM " + lfn_ids_table + " WHERE child_id=?");
			bs.add(statement.bind(this.id));

			if (!isDirectory()) {

				// remove se_lookups entry for physical files
				if (this.isFile() || this.isArchive()) {
					int modulol = (this.modulo > 0 ? this.modulo : Math.abs(this.id.hashCode() % modulo_se_lookup));
					for (Integer senumber : this.pfns.keySet()) {
						statement = getOrInsertPreparedStatement(session, "DELETE FROM " + se_lookup_table + " WHERE senumber=? AND modulo=? AND id=?");
						bs.add(statement.bind(senumber, Integer.valueOf(modulol), this.id));
					}
				}

				// cleanup cache entry
				if (notifyCache) {
					String toWipe = this.getCanonicalName();

					if (this.isDirectory())
						toWipe += ".*";

					TextCache.invalidateLFN(toWipe);
				}

				// insert into the physical removal queue
				if (purge && this.id != null) {
					try (DBFunctions db = ConfigUtils.getDB("alice_users")) {
						for (Integer senumber : this.pfns.keySet()) {
							db.setQueryTimeout(120);
							db.query("INSERT IGNORE INTO orphan_pfns_" + senumber + " (guid, se, md5sum, size) VALUES (string2binary(?), ?, ?, ?);", false, this.id, senumber, this.checksum,
									Long.valueOf(this.size));
						}
					}
					catch (Exception e) {
						logger.severe("LFN_CSD: delete: problem inserting into PFN cleanup queue: " + getCanonicalName() + " - " + e.toString());
						return false;
					}
				}
			}

			ResultSet rs = session.execute(bs);
			if (!rs.wasApplied()) {
				logger.severe("LFN_CSD: delete: problem deleting folder entry: " + getCanonicalName());
				return false;
			}

		}
		catch (Exception e) {
			logger.severe("LFN_CSD: delete: problem deleting folders/file: " + e.toString());
			return false;
		}

		return true;
	}

	/**
	 * @param c_id
	 * @return information of the given uuid, typically used to find the full lfn from it
	 */
	public static HashMap<String, Object> getInfofromChildId(UUID c_id) {
		HashMap<String, Object> ret = null;
		try {
			@SuppressWarnings("resource")
			final Session session = DBCassandra.getInstance();
			if (session == null) {
				logger.severe("LFN_CSD: getInfofromChildId: could not get an instance: " + c_id.toString());
				return null;
			}

			PreparedStatement statement = getOrInsertPreparedStatement(session, "select ctime, flag, path, path_id from " + lfn_ids_table + " where child_id = ?");
			BoundStatement boundStatement = new BoundStatement(statement);
			boundStatement.bind(c_id);
			boundStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);
			ResultSet results = session.execute(boundStatement);

			Row res = results.one();
			ret = new HashMap<>();
			ret.put("path", res.getString("path"));
			ret.put("path_id", res.getUUID("path_id"));
			ret.put("flag", Integer.valueOf(res.getInt("flag")));
			ret.put("ctime", res.getTimestamp("ctime"));
		}
		catch (Exception e) {
			logger.severe("LFN_CSD: getInfofromChildId: problem getting ids result: " + c_id.toString() + " - " + e.toString());
		}

		return ret;
	}

	/**
	 * @param lfnc_source
	 * @param lfnc_target
	 * @param lfnc_target_parent
	 * @return final lfn
	 */
	public static LFN_CSD mv(final LFN_CSD lfnc_source, final LFN_CSD lfnc_target, final LFN_CSD lfnc_target_parent) {
		LFN_CSD lfnc_final = null;
		// check exists
		if (!lfnc_source.exists) {
			logger.info("LFN_CSD: mv: the source or destination doesn't exist");
			return null;
		}
		// we can't move a folder into a file
		if (lfnc_source.isDirectory() && lfnc_target.exists && lfnc_target.isFile()) {
			logger.info("LFN_CSD: mv: trying to move a folder into a file: " + lfnc_source.canonicalName + " -> " + lfnc_target.canonicalName);
			return null;
		}

		// we can't overwrite entries
		if (!lfnc_source.isDirectory() && lfnc_target.exists && !lfnc_target.isDirectory()) {
			logger.info("LFN_CSD: mv: trying to overwrite an entry, not allowed: " + lfnc_source.canonicalName + " -> " + lfnc_target.canonicalName);
			return null;
		}

		// lfn_index: change path_id to the id of target
		// lfn_metadata: change parent_id to the id of the target
		// lfn_ids: change path_id to the id of the target
		try {
			@SuppressWarnings("resource")
			final Session session = DBCassandra.getInstance();
			if (session == null) {
				logger.severe("LFN_CSD: mv: could not get an instance: " + lfnc_source.getCanonicalName() + " -> " + lfnc_target.getCanonicalName());
				return null;
			}

			PreparedStatement statement;
			BatchStatement bs = new BatchStatement(BatchStatement.Type.LOGGED);
			bs.setConsistencyLevel(ConsistencyLevel.QUORUM);

			final UUID final_parent_id = (lfnc_target.exists && lfnc_target.isDirectory() ? lfnc_target.id : lfnc_target_parent.id);
			final boolean different_parent = !final_parent_id.equals(lfnc_source.parent_id);
			final boolean different_name = (!lfnc_source.isDirectory() && !lfnc_target.isDirectory() && !lfnc_source.child.equals(lfnc_target.child))
					|| (lfnc_source.isDirectory() && !lfnc_target.exists && !lfnc_source.child.equals(lfnc_target.child));

			statement = getOrInsertPreparedStatement(session, "DELETE FROM " + lfn_index_table + " WHERE path_id=? AND path=?");
			bs.add(statement.bind(lfnc_source.parent_id, lfnc_source.child));

			if (different_parent) {
				statement = getOrInsertPreparedStatement(session, "DELETE FROM " + lfn_metadata_table + " WHERE parent_id=? AND id=?");
				bs.add(statement.bind(lfnc_source.parent_id, lfnc_source.id));
				// if the destination is an existing directory, it becomes the parent
				lfnc_source.parent_id = final_parent_id;
			}

			// the final name depends on the case
			if (different_name) {
				lfnc_source.child = lfnc_target.child;
			}

			if ((different_parent || different_name)) { // !lfnc_source.isDirectory() &&
				statement = getOrInsertPreparedStatement(session, "UPDATE " + lfn_ids_table + " SET path_id=?,path=? WHERE child_id=?");
				bs.add(statement.bind(final_parent_id, lfnc_source.child, lfnc_source.id));
			}

			lfnc_source.prepareInsertStatements(bs, session, false, different_parent, lfn_index_table, lfn_ids_table, lfn_metadata_table, se_lookup_table);

			if (different_parent || different_name) {
				lfnc_source.path = (lfnc_target.isDirectory() && !different_name ? lfnc_target.getCanonicalName() : lfnc_target.path);
				lfnc_source.refreshCanonicalName();
			}

			ResultSet rs = session.execute(bs);
			if (!rs.wasApplied()) {
				logger.severe("LFN_CSD: mv: problem moving: " + lfnc_source.getCanonicalName() + " -> " + lfnc_target.getCanonicalName());
			}
			else {
				lfnc_final = lfnc_source;
			}
		}
		catch (Exception e) {
			logger.severe("LFN_CSD: mv: problem getting ids result: " + lfnc_source.getCanonicalName() + " -> " + lfnc_target.getCanonicalName() + ": " + e.toString());
		}

		return lfnc_final;
	}

	private void refreshCanonicalName() {
		canonicalName = path + child;
		if (type == 'd' && !canonicalName.endsWith("/"))
			canonicalName += "/";
	}

	/**
	 * @param owner_or_size_changed
	 * @param ctime_changed
	 * @param old_ctime
	 * @return true is LFN_CSD updated correctly, false otherwise. Takes into account if the owner or size changed to update se_lookup too
	 */
	@SuppressWarnings("resource")
	public boolean update(final boolean owner_or_size_changed, final boolean ctime_changed, final Date old_ctime) {
		try {
			final Session session = DBCassandra.getInstance();
			if (session == null) {
				logger.severe("LFN_CSD: update: could not get an instance: " + this.getCanonicalName());
				return false;
			}

			PreparedStatement statement;
			BatchStatement bs = new BatchStatement(BatchStatement.Type.LOGGED);
			bs.setConsistencyLevel(ConsistencyLevel.QUORUM);

			if (ctime_changed) {
				statement = getOrInsertPreparedStatement(session, "DELETE FROM " + lfn_index_table + " WHERE path_id=? AND path=? AND ctime=?");
				bs.add(statement.bind(parent_id, child, old_ctime));

				statement = getOrInsertPreparedStatement(session, "INSERT INTO " + lfn_index_table + " (path_id,path,ctime,child_id,flag)" + " VALUES (?,?,?,?,?)");
				bs.add(statement.bind(parent_id, child, ctime, id, Integer.valueOf(flag)));

				statement = getOrInsertPreparedStatement(session, "UPDATE " + lfn_ids_table + " SET ctime=?,flag=? WHERE child_id=?");
				bs.add(statement.bind(ctime, Integer.valueOf(flag), id));
			}
			// else { // if needed to change flag at some point, same should be done for lfn_ids
			// statement = getOrInsertPreparedStatement(session, "UPDATE " + lfn_index_table + " SET flag=? WHERE path_id=? AND path=?");
			// bs.add(statement.bind(ctime, Integer.valueOf(flag), parent_id, child));
			// }

			statement = getOrInsertPreparedStatement(session, "UPDATE " + lfn_metadata_table + " SET size=?,owner=?,gowner=?,ctime=?,checksum=?,metadata=?,pfns=? WHERE parent_id=? AND id=?");
			bs.add(statement.bind(Long.valueOf(size), owner, gowner, ctime, checksum, metadata, pfns, parent_id, id));

			if (owner_or_size_changed && pfns != null) {
				final Set<Integer> seNumbers = pfns.keySet();

				for (int seNumber : seNumbers) {
					statement = getOrInsertPreparedStatement(session, "UPDATE " + se_lookup_table + " SET owner=?,size=? where senumber=? AND modulo=? AND id=?");
					bs.add(statement.bind(owner, Long.valueOf(size), Integer.valueOf(seNumber), Integer.valueOf(modulo), id));
				}
			}

			ResultSet rs = session.execute(bs);
			if (!rs.wasApplied()) {
				logger.severe("LFN_CSD: update: problem updating entry: " + getCanonicalName());
				return false;
			}
		}
		catch (Exception e) {
			logger.severe("LFN_CSD: update: problem update entry: " + e.toString());
			return false;
		}

		return true;
	}

	/**
	 * Get the PFNs for this LFN_CSD
	 *
	 * @return set of physical locations
	 */
	public Set<PFN> getPFNs() {
		Set<PFN> pfnCache = new LinkedHashSet<>();

		if (monitor != null)
			monitor.incrementCounter("PFN_CSD_db_lookup");

		for (final Map.Entry<Integer, String> entry : this.pfns.entrySet()) {
			final PFN pfn = new PFN(entry.getKey(), entry.getValue(), this.id, this.size);
			pfnCache.add(pfn);
		}

		return pfnCache;
	}

}
