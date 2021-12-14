package alien.catalogue;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.catalogue.access.AccessType;
import alien.catalogue.access.AuthorizationFactory;
import alien.config.ConfigUtils;
import alien.quotas.FileQuota;
import alien.quotas.QuotaUtilities;
import alien.se.SE;
import alien.se.SEUtils;
import alien.site.OutputEntry;
import alien.taskQueue.TaskQueueUtils;
import alien.user.AliEnPrincipal;
import alien.user.AuthorizationChecker;
import lazyj.DBFunctions;
import lazyj.Format;
import lazyj.StringFactory;

/**
 * @author costing
 *
 */
public class BookingTable {

	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(BookingTable.class.getCanonicalName());

	private static final DBFunctions getDB() {
		final DBFunctions db = ConfigUtils.getDB("alice_users");
		db.setQueryTimeout(600);
		return db;
	}

	/**
	 * @param lfn
	 * @param requestedGUID
	 * @param requestedPFN
	 * @param se
	 * @return the PFN with the write access envelope if allowed to write or <code>null</code> if the PFN doesn't indicate a physical file but the entry was successfully booked
	 * @throws IOException
	 *             if not allowed to do that
	 */
	public static PFN bookForWriting(final LFN lfn, final GUID requestedGUID, final PFN requestedPFN, final SE se) throws IOException {
		return bookForWriting(AuthorizationFactory.getDefaultUser(), lfn, requestedGUID, requestedPFN, se);
	}

	/**
	 * @param user
	 *            <code>null</code> not allowed
	 * @param lfn
	 *            <code>null</code> not allowed
	 * @param requestedGUID
	 *            <code>null</code> not allowed
	 * @param requestedPFN
	 *            can be <code>null</code> and then a PFN specific for this SE and this GUID is generated
	 * @param se
	 *            <code>null</code> not allowed
	 * @return the PFN with the write access envelope if allowed to write or <code>null</code> if the PFN doesn't indicate a physical file but the entry was successfully booked
	 * @throws IOException
	 *             if not allowed to do that
	 */
	public static PFN bookForWriting(final AliEnPrincipal user, final LFN lfn, final GUID requestedGUID, final PFN requestedPFN, final SE se) throws IOException {
		if (lfn == null)
			throw new IllegalArgumentException("LFN cannot be null");

		if (user == null)
			throw new IllegalArgumentException("Principal cannot be null");

		if (se == null)
			throw new IllegalArgumentException("SE cannot be null");

		if (!se.canWrite(user))
			throw new IllegalArgumentException("SE doesn't allow " + user.getName() + " to write there");

		if (requestedGUID == null)
			throw new IllegalArgumentException("requested GUID cannot be null");

		LFN check = lfn;

		if (!check.exists)
			check = check.getParentDir();

		if (!AuthorizationChecker.canWrite(check, user)) {
			String message = "User " + user.getName() + " is not allowed to write LFN " + lfn.getCanonicalName();

			if (check == null)
				message += ": no such folder " + lfn.getParentName();
			else if (!check.equals(lfn))
				message += ": not enough rights on " + check.getCanonicalName();

			throw new IOException(message);
		}

		try (DBFunctions db = getDB()) {
			// check if the GUID is already registered in the catalogue
			final GUID checkGUID = GUIDUtils.getGUID(requestedGUID.guid);

			if (checkGUID != null) {
				// first question, is the user allowed to write it ?
				if (!AuthorizationChecker.canWrite(checkGUID, user))
					throw new IOException("User " + user.getName() + " is not allowed to write GUID " + checkGUID);

				if (checkGUID.size != requestedGUID.size)
					throw new IOException("You want to upload a different content size");

				if (checkGUID.md5 != null && requestedGUID.md5 != null && !checkGUID.md5.equalsIgnoreCase(requestedGUID.md5))
					throw new IOException("You want to upload a different content");

				// check if there isn't a replica already on this storage element
				final Set<PFN> pfns = checkGUID.getPFNs();

				if (pfns != null)
					for (final PFN pfn : pfns)
						if (se.equals(pfn.getSE()))
							throw new IOException("This GUID already has a replica in the requested SE");
			}
			else {
				// check the file quota only for new files, extra replicas don't count towards the quota limit

				final FileQuota quota = QuotaUtilities.getFileQuota(requestedGUID.owner);

				if (quota != null && !quota.canUpload(1, requestedGUID.size))
					throw new IOException("User " + requestedGUID.owner + " has exceeded the file quota and is not allowed to write any more files");
			}

			if (requestedPFN != null) {
				// TODO should we check whether or not this PFN exists? It's a heavy op ...
			}

			final PFN pfn = requestedPFN != null ? requestedPFN : new PFN(requestedGUID, se);

			pfn.setGUID(requestedGUID);

			// delete previous failed attempts since we are overwriting this pfn
			db.query("DELETE FROM LFN_BOOKED WHERE guid=string2binary(?) AND se=? AND pfn=? AND expiretime<0;", false, requestedGUID.guid.toString(), se.getName(), pfn.getPFN());

			// now check the booking table for previous attempts
			db.setReadOnly(true);
			db.query("SELECT owner FROM LFN_BOOKED WHERE guid=string2binary(?) AND se=? AND pfn=? AND expiretime>0;", false, requestedGUID.guid.toString(), se.getName(), pfn.getPFN());
			db.setReadOnly(false);

			if (db.moveNext()) {
				// there is a previous attempt on this GUID to this SE, who is the owner?
				if (user.canBecome(db.gets(1))) {
					final String reason = AuthorizationFactory.fillAccess(user, pfn, AccessType.WRITE);

					if (reason != null)
						throw new IOException("Access denied: " + reason);

					// that's fine, it's the same user, we can recycle the entry
					db.query("UPDATE LFN_BOOKED SET expiretime=unix_timestamp(now())+86400 WHERE guid=string2binary(?) AND se=? AND pfn=?;", false, requestedGUID.guid.toString(), se.getName(),
							pfn.getPFN());
				}
				else
					throw new IOException("You are not allowed to do this");
			}
			else {
				// make sure a previously queued deletion request for this file is wiped before giving out a new token
				db.query("DELETE FROM orphan_pfns WHERE guid=string2binary(?) AND se=?;", false, requestedGUID.guid.toString(), Integer.valueOf(se.seNumber));
			 	db.query("DELETE FROM orphan_pfns_" + se.seNumber + " WHERE guid=string2binary(?);", true, requestedGUID.guid.toString());

				final String reason = AuthorizationFactory.fillAccess(user, pfn, AccessType.WRITE);

				if (reason != null)
					throw new IOException("Access denied: " + reason);

				// create the entry in the booking table
				final StringBuilder q = new StringBuilder("INSERT INTO LFN_BOOKED (lfn,owner,md5sum,expiretime,size,pfn,se,gowner,user,guid,jobid) VALUES (");

				String lfnName = lfn.getCanonicalName();

				if (lfnName.equalsIgnoreCase("/" + requestedGUID.guid.toString()))
					lfnName = "";

				q.append(e(lfnName)).append(','); // LFN
				q.append(e(user.getName())).append(','); // owner
				q.append(e(requestedGUID.md5)).append(','); // md5sum
				q.append("unix_timestamp(now())+86400,"); // expiretime, 24 hours from now
				q.append(requestedGUID.size).append(','); // size
				q.append(e(pfn.getPFN())).append(','); // pfn
				q.append(e(se.getName())).append(','); // SE

				final Set<String> roles = user.getRoles();

				if (roles != null && roles.size() > 0)
					q.append(e(roles.iterator().next()));
				else
					q.append("null");

				q.append(','); // gowner
				q.append(e(user.getName())).append(','); // user
				q.append("string2binary('" + requestedGUID.guid.toString() + "'),"); // guid

				if (lfn.jobid > 0)
					q.append(lfn.jobid);
				else
					q.append("null");

				q.append(");");

				db.query(q.toString());
			}

			return pfn;
		}
	}

	/**
	 * @author costing
	 * @since 2019-07-15
	 */
	public static enum BOOKING_STATE {
		/**
		 * Commit the entry to the catalogue
		 */
		COMMITED,
		/**
		 * When the entry is known to have failed, mark it for asap removing
		 */
		REJECTED,
		/**
		 * The upload was successful but the file is not to be registered yet
		 */
		KEPT
	}

	/**
	 * Promote this entry to the catalog
	 *
	 * @param user
	 * @param pfn
	 * @return true if successful, false if not
	 */
	public static LFN commit(final AliEnPrincipal user, final PFN pfn) {
		return mark(user, pfn, BOOKING_STATE.COMMITED);
	}

	/**
	 * Mark this entry as failed, to be recycled
	 *
	 * @param user
	 * @param pfn
	 * @return true if marking was ok, false if not
	 */
	public static boolean reject(final AliEnPrincipal user, final PFN pfn) {
		return mark(user, pfn, BOOKING_STATE.REJECTED) != null;
	}

	/**
	 * Mark this entry as a valid output from a job, to be used at a later time by, for example, registerOutput
	 *
	 * @param user
	 * @param pfn
	 * @return true if at least one PFN was kept as output of a previous job, but not committed
	 */
	public static boolean keep(final AliEnPrincipal user, final PFN pfn) {
		return mark(user, pfn, BOOKING_STATE.KEPT) != null;
	}

	private static LFN BOGUS_ENTRY = new LFN("/bogus");

	/**
	 * Move the entry from the booking table
	 *
	 * @param user user requesting this operation
	 * @param pfn the PFN to commit
	 * @param state state to put the entry in
	 * @return <code>null</code> in case of error, or the booked LFN if the entry was committed (or some bogus entry if it was removed, the value is not important in this case)
	 */
	public static LFN mark(final AliEnPrincipal user, final PFN pfn, final BOOKING_STATE state) {
		LFN ret = null;

		try (DBFunctions db = getDB()) {
			if (user == null) {
				logger.log(Level.WARNING, "Not marking since the user is null");
				return null;
			}

			if (pfn == null) {
				logger.log(Level.WARNING, "Not marking since the PFN is null");
				return null;
			}

			String w = "pfn" + eq(pfn.getPFN());

			final SE se = pfn.getSE();

			if (se == null) {
				logger.log(Level.WARNING, "Not marking since there is no valid SE in this PFN: " + pfn);
				return null;
			}

			w += " AND se" + eq(se.getName());

			final GUID guid = pfn.getGuid();

			if (guid == null) {
				logger.log(Level.WARNING, "Not marking since there is no GUID in this PFN: " + pfn);
				return null;
			}

			w += " AND guid=string2binary(" + e(guid.guid.toString()) + ")";

			w += " AND owner" + eq(user.getName());

			if (state == BOOKING_STATE.REJECTED) {
				if (db.query("UPDATE LFN_BOOKED SET expiretime=-1*(unix_timestamp(now())+60*60*24*30) WHERE " + w) && db.getUpdateCount() > 0)
					return BOGUS_ENTRY;

				return null;
			}

			if (state == BOOKING_STATE.KEPT) {
				if (db.query("UPDATE LFN_BOOKED SET existing=10 WHERE " + w) && db.getUpdateCount() > 0)
					return BOGUS_ENTRY;

				return null;
			}

			if (!guid.addPFN(pfn))
				if (guid.hasReplica(pfn.seNumber))
					logger.log(Level.FINE, "Could not add the PFN to this GUID: " + guid + "\nPFN: " + pfn);
				else {
					logger.log(Level.WARNING, "Could not add the PFN to this GUID: " + guid + "\nPFN: " + pfn);
					return null;
				}

			db.setReadOnly(true);

			db.query("SELECT lfn,jobid FROM LFN_BOOKED WHERE " + w);

			db.setReadOnly(false);

			boolean allNullLFNs = true;

			while (db.moveNext()) {
				final String sLFN = db.gets(1);

				if (sLFN.length() == 0)
					continue;

				allNullLFNs = false;

				final LFN lfn = LFNUtils.getLFN(sLFN, true);

				if (lfn.exists)
					ret = lfn;
				else {
					lfn.size = guid.size;
					lfn.owner = guid.owner;
					lfn.gowner = guid.gowner;
					lfn.perm = guid.perm;
					lfn.aclId = guid.aclId;
					lfn.ctime = guid.ctime;
					lfn.expiretime = guid.expiretime;
					lfn.guid = guid.guid;
					// lfn.guidtime = ?;

					lfn.md5 = guid.md5;
					lfn.type = guid.type != 0 ? guid.type : 'f';

					lfn.guidtime = GUIDUtils.getIndexTime(guid.guid);

					lfn.jobid = db.getl(2, -1);

					final boolean inserted = LFNUtils.insertLFN(lfn);

					if (!inserted) {
						logger.log(Level.WARNING, "Could not insert this LFN in the catalog : " + lfn);
						return null;
					}

					ret = lfn;
				}
			}

			if (allNullLFNs) {
				// The LFN was not passed, used by transfers to create replicas of a GUID without references to an LFN
				// But then they rely on an LFN being returned as a confirmation
				ret = new LFN("/bogus");
			}

			// was booked, now let's move it to the catalog
			db.query("DELETE FROM LFN_BOOKED WHERE " + w);
		}

		return ret;
	}

	private static final String eq(final String s) {
		if (s == null)
			return " IS NULL";

		return "='" + Format.escSQL(s) + "'";
	}

	private static final String e(final String s) {
		if (s != null)
			return "'" + Format.escSQL(s) + "'";

		return "null";
	}

	/**
	 * Get the object for a booked PFN
	 *
	 * @param pfn
	 * @return the object, if exactly one entry exists, <code>null</code> if it was not booked
	 * @throws IOException
	 *             if any problem (more than one entry, invalid SE ...)
	 */
	public static PFN getBookedPFN(final String pfn) throws IOException {
		try (DBFunctions db = getDB()) {
			db.setReadOnly(true);
			db.setCursorType(ResultSet.TYPE_SCROLL_INSENSITIVE);

			if (!db.query("SELECT *, binary2string(guid) as guid_as_string FROM LFN_BOOKED WHERE pfn=?;", false, pfn))
				throw new IOException("Could not get the booked details for this pfn, query execution failed");

			final int count = db.count();

			if (count == 0)
				return null;

			if (count > 1)
				throw new IOException("More than one entry with this pfn: '" + pfn + "'");

			final SE se = SEUtils.getSE(db.gets("se"));

			if (se == null)
				throw new IOException("This SE doesn't exist: '" + db.gets("se") + "' for '" + pfn + "'");

			final GUID guid = GUIDUtils.getGUID(UUID.fromString(db.gets("guid_as_string")), true);

			if (!guid.exists()) {
				guid.size = db.getl("size");
				guid.md5 = StringFactory.get(db.gets("md5sum"));
				guid.owner = StringFactory.get(db.gets("owner"));
				guid.gowner = StringFactory.get(db.gets("gowner"));
				guid.perm = "755";
				guid.ctime = new Date();
				guid.expiretime = null;
				guid.type = 0;
				guid.aclId = -1;
			}

			final PFN retpfn = new PFN(guid, se);

			retpfn.pfn = pfn;

			return retpfn;
		}
	}

	/**
	 * Release all the booked PFNs by a previous iteration of this job ID
	 *
	 * @param jobID
	 * @return the number of physical files (PFNs) that were marked as ready to be collected by the orphan PFNs cleanup procedure
	 */
	public static int resubmitJob(final Long jobID) {
		try (DBFunctions db = getDB()) {
			db.query("UPDATE LFN_BOOKED SET expiretime=-1*(unix_timestamp(now())+60*60*24*30) WHERE jobid=?", false, jobID);

			return db.getUpdateCount();
		}
	}

	/**
	 * Register all the uncommitted files of a particular job ID
	 *
	 * @param user
	 * @param jobID
	 * @return the registered files
	 */
	public static Set<LFN> registerOutput(final AliEnPrincipal user, final Long jobID) {
		final Set<LFN> ret = new HashSet<>();

		try (DBFunctions db = getDB()) {
			// this query will return both physical files as well as archive members, if any
			db.query("SELECT pfn FROM LFN_BOOKED WHERE jobid=? AND expiretime>0 AND owner=?;", false, jobID, user.getName());

			while (db.moveNext()) {
				try {
					final PFN pfn = getBookedPFN(db.gets(1));

					if (pfn != null) {
						final LFN l = mark(user, pfn, BOOKING_STATE.COMMITED);

						if (l != null) {
							l.setExpireTime(new Date(System.currentTimeMillis() + 1000L * 60 * 60 * 24 * 14));
							ret.add(l);							
						}
					}
				}
				catch (@SuppressWarnings("unused") final IOException e) {
					// ignore
				}
			}
		}

		return ret;
	}

	/**
	 * 
	 * Books the members of an archive for writing
	 * 
	 * @param archive
	 * @param archive_lfn 
	 * @param outputDir
	 * @param user
	 * @return true for no IOExceptions
	 * @throws IOException
	 */
	public static boolean bookArchiveContents(final OutputEntry archive, final LFN archive_lfn, final String outputDir, final AliEnPrincipal user) throws IOException {
		final ArrayList<String> members = archive.getFilesIncluded();
		final HashMap<String, Long> sizes = archive.getSizesIncluded();
		final HashMap<String, String> md5s = archive.getMD5sIncluded();

		final SE se = SEUtils.getSE("no_se");

		try {
			final String base_pfn = "guid:///" + archive_lfn.guid.toString() + "?ZIP=";

			for (String member : members) {
				if (!sizes.containsKey(member)) {
					TaskQueueUtils.putJobLog(archive.getQueueId().longValue(), "error", "File " + member + ": doesn't exist or has 0 size. Skip.", null);
					continue;
				}
				else if (!md5s.containsKey(member)) {
					TaskQueueUtils.putJobLog(archive.getQueueId().longValue(), "error", "File " + member + ": unable to calculate MD5. Skip.", null);
					continue;
				}
				
				// GUID
				final UUID uuid = GUIDUtils.generateTimeUUID();
				GUID member_g = GUIDUtils.getGUID(uuid, true);
				member_g.owner = user.getName();

				// PFN
				String member_spfn = base_pfn + member;
				PFN member_pfn = new PFN(member_g, se);
				member_pfn.pfn = member_spfn;

				// LFN
				String member_slfn = outputDir + member;
				LFN member_lfn = LFNUtils.getLFN(member_slfn, true);
				member_lfn.guid = uuid;
				member_lfn.md5 = md5s.get(member);
				member_lfn.size = sizes.get(member).longValue();
				member_lfn.jobid = archive.getQueueId().longValue();
				member_lfn.type = 'f';
				member_lfn.owner = user.getName();

				bookForWriting(user, member_lfn, member_g, member_pfn, se);
			}
		}
		catch (final IOException e) {
			return false;
		}
		return true;
	}
}
