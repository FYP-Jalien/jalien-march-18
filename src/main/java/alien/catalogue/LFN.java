package alien.catalogue;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.servlets.TextCache;
import alien.user.AliEnPrincipal;
import lazyj.DBFunctions;
import lazyj.Format;
import lazyj.StringFactory;

/**
 * @author costing
 *
 */
public class LFN implements Comparable<LFN>, CatalogEntity {

	/**
	 *
	 */
	private static final long serialVersionUID = -4419468872696081193L;

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(LFN.class.getCanonicalName());

	/**
	 * Monitoring component
	 */
	static transient final Monitor monitor = MonitorFactory.getMonitor(LFN.class.getCanonicalName());

	/**
	 * entryId
	 */
	public long entryId;

	/**
	 * Owner
	 */
	public String owner;

	/**
	 * Last change timestamp
	 */
	public Date ctime;

	/**
	 * If more than one copy
	 */
	public boolean replicated;

	/**
	 * ACL id
	 */
	public int aclId;

	/**
	 * short LFN
	 *
	 * @see IndexTableEntry
	 */
	public String lfn;

	/**
	 * Expiration time
	 */
	public Date expiretime;

	/**
	 * Size, in bytes
	 */
	public long size;

	/**
	 * Parent directory, in the same IndexTableEntry
	 */
	public long dir;

	/**
	 * Group
	 */
	public String gowner;

	/**
	 * File type
	 */
	public char type;

	/**
	 * Access rights
	 */
	public String perm;

	/**
	 * The unique identifier
	 */
	public UUID guid;

	/**
	 * MD5 checksum
	 */
	public String md5;

	/**
	 * GUID time (in GUIDINDEX short style)
	 */
	public String guidtime;

	/**
	 * ?
	 */
	public boolean broken;

	/**
	 * Whether or not this entry really exists in the catalogue
	 */
	public boolean exists = false;

	/**
	 * Parent directory
	 */
	public LFN parentDir = null;

	/**
	 * Canonical path
	 */
	private String canonicalName = null;

	/**
	 * The table where this row can be found
	 */
	public IndexTableEntry indexTableEntry;

	/**
	 * Job ID that produced this file
	 *
	 * @since AliEn 2.19
	 */
	public long jobid;

	/**
	 * @param lfn
	 * @param entry
	 */
	LFN(final String lfn, final IndexTableEntry entry) {
		this.lfn = lfn.substring(entry.lfn.length());
		this.indexTableEntry = entry;

		int idx = lfn.lastIndexOf('/');

		if (idx == lfn.length() - 1)
			idx = lfn.lastIndexOf('/', idx - 1);

		if (idx >= 0 && ConfigUtils.isCentralService()) {
			final String sDir = lfn.substring(0, idx + 1);

			parentDir = LFNUtils.getLFN(sDir, true);

			if (parentDir != null) {
				dir = parentDir.entryId;

				owner = parentDir.owner;

				gowner = parentDir.gowner;

				perm = parentDir.perm;
			}
		}
	}

	/**
	 * Simple constructor with the canonical
	 *
	 * @param canonicalLFN
	 */
	LFN(final String canonicalLFN) {
		this.canonicalName = canonicalLFN;

		final int idx = canonicalName.lastIndexOf('/');

		if (idx > 0)
			lfn = canonicalName.substring(idx + 1);
		else
			lfn = canonicalName;
	}

	/**
	 * Get the parent directory
	 *
	 * @return parent directory
	 */
	public LFN getParentDir() {
		return getParentDir(false);
	}

	/**
	 * @param evenIfNotExist
	 * @return parent directory
	 */
	public LFN getParentDir(final boolean evenIfNotExist) {
		if (parentDir != null)
			return parentDir;

		if (dir > 0)
			parentDir = indexTableEntry.getLFN(dir);

		if (parentDir == null) {
			final String sParentDir = getCanonicalName();

			if (sParentDir.length() > 1) {
				int idx = sParentDir.lastIndexOf('/');

				if (idx == sParentDir.length() - 1)
					idx = sParentDir.lastIndexOf('/', idx - 1);

				if (idx >= 0) {
					parentDir = LFNUtils.getLFN(sParentDir.substring(0, idx + 1), evenIfNotExist);

					if (parentDir != null && !parentDir.exists) {
						parentDir.owner = this.owner;
						parentDir.gowner = this.gowner;
						parentDir.perm = this.perm;
					}
				}
			}
		}

		return parentDir;
	}

	/**
	 * @param db
	 * @param entry
	 */
	public LFN(final DBFunctions db, final IndexTableEntry entry) {
		init(db);

		this.indexTableEntry = entry;
	}

	@Override
	public int hashCode() {
		return getCanonicalName().hashCode();
	}

	private void init(final DBFunctions db) {
		exists = true;

		entryId = db.getl("entryId");

		owner = StringFactory.get(db.gets("owner"));

		ctime = db.getDate("ctime", null);

		replicated = db.getb("replicated", false);

		aclId = db.geti("aclId", -1);

		lfn = StringFactory.get(db.gets("lfn"));

		expiretime = db.getDate("expiretime", null);

		size = db.getl("size");

		dir = db.geti("dir");

		gowner = StringFactory.get(db.gets("gowner"));

		final String ftype = db.gets("type");

		if (ftype.length() > 0)
			type = ftype.charAt(0);
		else
			type = lfn.endsWith("/") ? 'd' : 'f';

		perm = StringFactory.get(db.gets("perm"));

		final byte[] guidBytes = db.getBytes("guid");

		if (guidBytes != null)
			guid = GUID.getUUID(guidBytes);
		else
			guid = null;

		md5 = StringFactory.get(db.gets("md5"));

		guidtime = StringFactory.get(db.gets("guidtime"));

		broken = db.getb("broken", false);

		jobid = db.getl("jobid", -1);
	}

	@Override
	public String toString() {
		return "LFN entryId\t: " + entryId + "\n" + "owner\t\t: " + owner + ":" + gowner + "\n" + "ctime\t\t: " + ctime + " (expires " + expiretime + ")\n" + "replicated\t: " + replicated + "\n"
				+ "aclId\t\t: " + aclId + "\n" + "lfn\t\t: " + lfn + "\n" + "dir\t\t: " + dir + "\n" + "size\t\t: " + size + "\n" + "type\t\t: " + type + "\n" + "perm\t\t: " + perm + "\n"
				+ "guid\t\t: " + guid + "\n" + "md5\t\t: " + md5 + "\n" + "guidtime\t: " + guidtime + "\n" + "broken\t\t: " + broken + "\n" + "jobid\t\t: " + jobid;
	}

	/**
	 * @return a map of properties, to be used in JSON serialization
	 */
	public Map<String, Object> toMap() {
		final Map<String, Object> ret = new LinkedHashMap<>();

		ret.put("lfn", getCanonicalName());
		ret.put("size", Long.valueOf(size));
		ret.put("type", String.valueOf(type));
		ret.put("perm", perm);
		ret.put("ctime", Long.valueOf(ctime.getTime()));
		ret.put("owner", owner);
		ret.put("gowner", gowner);

		if (guid != null)
			ret.put("guid", guid);

		if (md5 != null && !md5.isBlank())
			ret.put("md5", md5);

		ret.put("turl", "alien://" + getCanonicalName());

		if (jobid > 0)
			ret.put("jobid", Long.valueOf(jobid));

		if (expiretime != null)
			ret.put("expires", Long.valueOf(expiretime.getTime()));

		ret.put("entryId", Long.valueOf(entryId));

		// Other internal fields that are not expected to be useful to clients
		// ret.put("replicated", Boolean.valueOf(replicated));
		// ret.put("aclId", Long.valueOf(aclId));
		// ret.put("dir", Long.valueOf(dir));

		// if (guidtime != null)
		// ret.put("guidtime", guidtime);

		// ret.put("broken", Boolean.valueOf(broken));

		return ret;
	}

	/**
	 * Get the canonical name (full path and name)
	 *
	 * @return canonical name without the '/' at the end, if any
	 */
	public String getStrippedCanonicalName() {
		String s = getCanonicalName();

		if (s != null && s.endsWith("/"))
			s = s.substring(0, s.length() - 1);

		return s;
	}

	/**
	 * Get the canonical name (full path and name)
	 *
	 * @return canonical name
	 */
	public String getCanonicalName() {
		if (canonicalName != null)
			return canonicalName;

		final String sLFN = indexTableEntry.lfn;

		final boolean bEnds = sLFN.endsWith("/");
		final boolean bStarts = lfn.startsWith("/");

		if (bEnds && bStarts)
			canonicalName = sLFN.substring(0, sLFN.length() - 1) + lfn;
		else if (!bEnds && !bStarts)
			canonicalName = sLFN + "/" + lfn;
		else
			canonicalName = sLFN + lfn;

		canonicalName = StringFactory.get(canonicalName);

		return canonicalName;
	}

	/**
	 * @return the last token of the name
	 */
	public String getFileName() {
		final String fullName = getStrippedCanonicalName();

		final int idx = fullName.lastIndexOf('/');

		if (idx >= 0)
			return fullName.substring(idx + 1);

		return fullName;
	}

	/**
	 * @return parent directory path
	 */
	public String getParentName() {
		final String fullName = getStrippedCanonicalName();

		final int idx = fullName.lastIndexOf('/');

		if (idx > 0)
			return fullName.substring(0, idx);

		return fullName;
	}

	/**
	 * Get the physical locations of this file
	 *
	 * @return the physical locations for this file
	 */
	public Set<PFN> whereis() {
		if (!exists || guid == null)
			return null;

		final GUID id = GUIDUtils.getGUID(this);

		if (id == null)
			return null;

		return id.getPFNs();
	}

	/**
	 * Get the real physical locations of this file
	 *
	 * @return real locations
	 */
	public Set<PFN> whereisReal() {
		if (!exists || guid == null)
			return null;

		final GUID id = GUIDUtils.getGUID(this);

		if (id == null)
			return null;

		final Set<GUID> realGUIDs = id.getRealGUIDs();

		if (realGUIDs == null || realGUIDs.size() == 0)
			return null;

		final Set<PFN> ret = new LinkedHashSet<>();

		for (final GUID realId : realGUIDs) {
			realId.addKnownLFN(this);

			final Set<PFN> pfns = realId.getPFNs();

			if (pfns == null)
				continue;

			ret.addAll(pfns);
		}

		return ret;
	}

	/**
	 * Check whether or not this LFN points to a physical file or is a pointer to an archive
	 *
	 * @return <code>true</code> if the file is physically on disk, <code>false</code> if it is located inside an archive or even if doesn't exist
	 */
	public boolean isReal() {
		if (!exists || guid == null)
			return false;

		final GUID id = GUIDUtils.getGUID(guid);

		if (id == null)
			return false;

		final Set<PFN> pfns = id.getPFNs();

		if (pfns == null || pfns.size() == 0)
			return true;

		for (final PFN pfn : pfns)
			if (pfn.pfn.startsWith("guid://"))
				return false;

		return true;
	}

	@Override
	public int compareTo(final LFN o) {
		if (this == o)
			return 0;

		if (indexTableEntry != null && o.indexTableEntry != null) {
			final int diff = indexTableEntry.compareTo(o.indexTableEntry);

			if (diff != 0)
				return diff;

			// in the same indextable the relative path can be compared directly
			return lfn.compareTo(o.lfn);
		}

		// System.err.println("Comparing " + getCanonicalName() + " to " + o.getCanonicalName());

		// in the general case we compare full paths
		return getCanonicalName().compareTo(o.getCanonicalName());
	}

	@Override
	public boolean equals(final Object obj) {
		if (obj == null || !(obj instanceof LFN))
			return false;

		if (this == obj)
			return true;

		final LFN other = (LFN) obj;

		return compareTo(other) == 0;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see alien.catalogue.CatalogEntity#getGroup()
	 */
	@Override
	public String getGroup() {
		return gowner;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see alien.catalogue.CatalogEntity#getName()
	 */
	@Override
	public String getName() {
		return getCanonicalName();
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see alien.catalogue.CatalogEntity#getOwner()
	 */
	@Override
	public String getOwner() {
		return owner;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see alien.catalogue.CatalogEntity#getPermissions()
	 */
	@Override
	public String getPermissions() {
		return perm != null ? perm : "755";
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see alien.catalogue.CatalogEntity#getType()
	 */
	@Override
	public char getType() {
		return type;
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

	private static final String e(final String s) {
		if (s == null)
			return "null";

		return "'" + Format.escSQL(s) + "'";
	}

	private static final DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private static final synchronized String format(final Date d) {
		if (d == null)
			return null;

		return formatter.format(d);
	}

	/**
	 * Insert a new LFN in the catalogue
	 *
	 * @return <code>true</code> if the new entry was inserted, <code>false</code> if the query failed
	 */
	boolean insert() {
		String lfnToInsert = lfn;

		if (type == 'd') {
			if (!lfnToInsert.endsWith("/"))
				lfnToInsert += "/";
		}
		else
			while (lfnToInsert.endsWith("/"))
				lfnToInsert = lfnToInsert.substring(0, lfnToInsert.length() - 1);

		if (ctime == null)
			ctime = new Date();

		final String q = "INSERT INTO L" + indexTableEntry.tableName + "L (owner, ctime, replicated, aclId, lfn, expiretime, size, "
				+ "dir, gowner, type, perm, guid, md5, guidtime, broken, jobid) VALUES (" + e(owner) + "," + e(format(ctime)) + "," + (replicated ? "1" : "0") + "," + (aclId > 0 ? "" + aclId : "null")
				+ "," + e(lfnToInsert) + "," + e(format(expiretime)) + "," + size + "," + dir + "," + e(gowner) + "," + (type > 0 && "cdf-".indexOf(type) != -1 ? e("" + type) : "f") + "," + e(perm)
				+ "," + (guid != null ? "string2binary('" + guid + "')," : "null,") + e(md5) + "," + e(guidtime) + "," + (broken ? 1 : 0) + "," + (jobid > 0 ? "" + jobid : "null") + ");";

		if (monitor != null)
			monitor.incrementCounter("LFN_insert");

		try (DBFunctions db = indexTableEntry.getDB()) {
			db.setLastGeneratedKey(true);

			final boolean result = db.query(q);

			if (result) {
				exists = true;
				entryId = db.getLastGeneratedKeyLong().longValue();
			}

			return result;
		}
	}

	/**
	 * Set a new expiration time for this LFN
	 * 
	 * @param newExpiration
	 * @return the previous value of the LFN expiration time
	 */
	public Date setExpireTime(final Date newExpiration) {
		final Date oldExpiration = expiretime;

		expiretime = newExpiration;

		if (exists) {
			// otherwise insert() will set the correct expiration value

			final String q = "UPDATE L" + indexTableEntry.tableName + "L SET expiretime=" + e(format(expiretime)) + " WHERE entryId=" + entryId;

			if (monitor != null)
				monitor.incrementCounter("LFN_set_expire");

			try (DBFunctions db = indexTableEntry.getDB()) {
				db.query(q);
			}
		}

		return oldExpiration;
	}

	/**
	 * @return <code>true</code> if the database entry was updated
	 */
	boolean update() {
		if (!exists)
			return false;

		ctime = new Date();

		final String q = "UPDATE L" + indexTableEntry.tableName + "L SET size=" + size + ",owner=" + e(owner) + ",gowner=" + e(gowner) + ",ctime=" + e(format(ctime)) + ",md5=" + e(md5) + ",perm="
				+ e(perm) + " WHERE entryId=" + entryId;

		if (monitor != null)
			monitor.incrementCounter("LFN_update");

		try (DBFunctions db = indexTableEntry.getDB()) {
			return db.query(q) && db.getUpdateCount() == 1;
		}
	}

	/**
	 * Delete this LFN in the Catalogue
	 *
	 * @param purge
	 *            physically delete the PFNs
	 * @param recursive
	 *            for directories, remove all subentries. <B>This code doesn't check permissions, do the check before!</B>
	 *
	 * @return <code>true</code> if this LFN entry was deleted in the database
	 */
	public boolean delete(final boolean purge, final boolean recursive) {
		return delete(purge, recursive, true);
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
	public boolean delete(final boolean purge, final boolean recursive, final boolean notifyCache) {
		if (!exists)
			throw new IllegalAccessError("You asked to delete an LFN that doesn't exist in the database");

		if (isDirectory()) {
			final List<LFN> subentries = list();

			if (subentries != null && subentries.size() > 0) {
				if (!recursive)
					return false; // asked to delete a non-empty directory
									// without indicating the recursive flag

				// do not notify the cache of every subentry but only once for the entire folder
				for (final LFN subentry : subentries)
					if (!subentry.delete(purge, recursive, false))
						return false;
			}
		}

		if (monitor != null)
			monitor.incrementCounter("LFN_delete");

		final String q = "DELETE FROM L" + indexTableEntry.tableName + "L WHERE entryId=?;";

		boolean ok = false;

		try (DBFunctions db = indexTableEntry.getDB()) {
			if (db.query(q, false, Long.valueOf(entryId))) {
				if (notifyCache)
					try {
						String toWipe = getCanonicalName();

						if (isDirectory())
							toWipe += ".*";

						TextCache.invalidateLFN(toWipe);
					}
					catch (final Throwable t) {
						logger.log(java.util.logging.Level.WARNING, "Cannot invalidate cache entry", t);
					}

				exists = false;
				entryId = 0;
				ok = true;
			}

			if (ok && purge && guid != null)
				try (DBFunctions db2 = ConfigUtils.getDB("alice_users")) {
					db2.setQueryTimeout(120);
					db2.query("INSERT IGNORE INTO orphan_pfns_0 (guid,size) VALUES (string2binary(?), ?);", false, guid.toString(), Long.valueOf(size));
				}
		}

		return ok;
	}

	/**
	 * Change the ownership of this LFN.
	 *
	 * @param newOwner
	 * @return the previous owner, if the ownership was updated, or <code>null</code> if nothing was touched
	 */
	public String chown(final AliEnPrincipal newOwner) {
		if (!exists)
			throw new IllegalAccessError("You asked to chown an LFN that doesn't exist in the database");

		if (!this.owner.equals(newOwner.getName())) {
			final String oldOwner = this.owner;

			this.owner = this.gowner = StringFactory.get(newOwner.getName());

			if (update())
				return oldOwner;
		}

		return null;
	}

	/**
	 * Permission bits regex
	 */
	static final Pattern PERMISSIONS = Pattern.compile("^[0-7]{3}$");

	/**
	 * Change the access permissions on this LFN
	 * 
	 * @param newPermissions
	 * @return the previous permissions, if anything changed and the change was successfully propagated to the database, or <code>null</code> if nothing was touched
	 */
	public String chmod(final String newPermissions) {
		if (!exists)
			throw new IllegalAccessError("You asked to chmod an LFN that doesn't exist in the database");

		if (newPermissions == null || newPermissions.length() != 3 || !PERMISSIONS.matcher(newPermissions).matches())
			throw new IllegalAccessError("Invalid permissions string " + newPermissions);

		if (!newPermissions.equals(perm)) {
			final String oldPerms = perm;

			this.perm = StringFactory.get(newPermissions);

			if (update())
				return oldPerms;
		}

		return null;
	}

	/**
	 * @return the list of entries in this folder
	 */
	public List<LFN> list() {
		if (indexTableEntry == null)
			return null;

		final List<LFN> ret = new ArrayList<>();

		if (monitor != null)
			monitor.incrementCounter("LFN_list");

		final IndexTableEntry separateTable = CatalogueUtils.getSeparateTable(getCanonicalName());

		if (separateTable != null) {
			final String q = "SELECT * FROM L" + separateTable.tableName + "L WHERE dir=(SELECT entryId FROM L" + separateTable.tableName
					+ "L WHERE lfn='') AND lfn IS NOT NULL AND lfn!='' ORDER BY lfn ASC;";

			try (DBFunctions db = separateTable.getDB()) {
				db.setReadOnly(true);

				db.query(q);

				while (db.moveNext())
					ret.add(new LFN(db, separateTable));
			}

			return ret;
		}

		final String q = "SELECT * FROM L" + indexTableEntry.tableName + "L WHERE dir=? AND lfn IS NOT NULL AND lfn!='' ORDER BY lfn ASC;";

		try (DBFunctions db = indexTableEntry.getDB()) {
			db.setReadOnly(true);

			db.query(q, false, Long.valueOf(entryId));

			while (db.moveNext())
				ret.add(new LFN(db, indexTableEntry));
		}

		return ret;
	}

	/**
	 * @return the set of files in this collection, or <code>null</code> if this is not a collection
	 */
	public Set<String> listCollection() {
		if (!isCollection() || !exists)
			return null;

		if (monitor != null)
			monitor.incrementCounter("LFN_listcollection");

		if (logger.isLoggable(Level.FINE))
			logger.fine(CatalogueUtils.getHost(this.indexTableEntry.hostIndex).db);

		try (DBFunctions db = ConfigUtils.getDB(CatalogueUtils.getHost(this.indexTableEntry.hostIndex).db)) {
			db.setReadOnly(true);
			db.setQueryTimeout(300);

			if (!db.query("SELECT origLFN FROM COLLECTIONS_ELEM INNER JOIN COLLECTIONS USING (collectionID) WHERE collGUID=string2binary(?) ORDER BY 1;", false, guid.toString()))
				return null;

			final TreeSet<String> ret = new TreeSet<>();

			while (db.moveNext())
				ret.add(StringFactory.get(db.gets(1)));

			return ret;
		}
	}

	/**
	 * Sort by file size (asc) then by file name (asc)
	 */
	public static final Comparator<LFN> SIZE_COMPARATOR = (o1, o2) -> {
		final long diff = o1.size - o2.size;

		if (diff < 0)
			return -1;
		if (diff > 0)
			return 1;

		return o1.compareTo(o2);
	};

	@Override
	public long getSize() {
		return size;
	}

	@Override
	public String getMD5() {
		return md5;
	}
}
