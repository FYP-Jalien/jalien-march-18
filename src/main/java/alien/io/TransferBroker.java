/**
 *
 */
package alien.io;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.PFN;
import alien.catalogue.access.AccessType;
import alien.catalogue.access.AuthorizationFactory;
import alien.config.ConfigUtils;
import alien.monitoring.MonitorFactory;
import alien.se.SE;
import alien.se.SEUtils;
import alien.user.AliEnPrincipal;
import alien.user.UserFactory;
import apmon.ApMon;
import lazyj.DBFunctions;
import lazyj.DBFunctions.DBConnection;
import lazyj.Format;
import lazyj.cache.ExpirationCache;

/**
 * @author costing
 * @since Dec 9, 2010
 */
public class TransferBroker {
	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(TransferBroker.class.getCanonicalName());

	private TransferBroker() {
		// just hide it
	}

	private static TransferBroker instance = null;

	/**
	 * @return singleton
	 */
	public static synchronized TransferBroker getInstance() {
		if (instance == null)
			instance = new TransferBroker();

		return instance;
	}

	private ResultSet resultSet = null;

	private Statement stat = null;

	private final void executeClose() {
		if (resultSet != null) {
			try {
				resultSet.close();
			}
			catch (@SuppressWarnings("unused") final Throwable t) {
				// ignore
			}

			resultSet = null;
		}

		if (stat != null) {
			try {
				stat.close();
			}
			catch (@SuppressWarnings("unused") final Throwable t) {
				// ignore
			}

			stat = null;
		}
	}

	private int updateCount = -1;

	@SuppressWarnings("resource")
	private final boolean executeQuery(final DBConnection dbc, final String query) {
		executeClose();

		try {
			dbc.setReadOnly(false);
			stat = dbc.getConnection().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			stat.setQueryTimeout(600);

			if (stat.execute(query, Statement.NO_GENERATED_KEYS)) {
				updateCount = -1;

				resultSet = stat.getResultSet();
			}
			else {
				updateCount = stat.getUpdateCount();

				executeClose();
			}

			return true;
		}
		catch (final SQLException e) {
			logger.log(Level.WARNING, "Exception executing the query", e);

			return false;
		}
	}

	private long lastTimeNoWork = 0;

	private DBFunctions dbCached = ConfigUtils.getDB("transfers");

	private final ExpirationCache<String, Integer> maxTransfersCache = new ExpirationCache<>(1024);

	private int getMaxTransfers(final String seName) {
		final Integer i = maxTransfersCache.get(seName.toLowerCase());

		if (i != null)
			return i.intValue();

		int ret = 0;

		try (DBFunctions db = ConfigUtils.getDB("transfers")) {
			db.setReadOnly(true);
			db.setQueryTimeout(60);

			db.query("SELECT max(max_transfers) FROM PROTOCOLS WHERE sename=?", false, seName);

			if (db.moveNext())
				ret = db.geti(1);
		}

		maxTransfersCache.put(seName.toLowerCase(), Integer.valueOf(ret), 1000 * 60 * 5);

		return ret;
	}

	/**
	 * @return the next transfer to be performed, or <code>null</code> if there is nothing to do
	 */
	/**
	 * @param agent
	 * @return the next transfer to execute, if any is available
	 */
	public synchronized Transfer getWork(final TransferAgent agent) {
		if (System.currentTimeMillis() < lastTimeNoWork)
			return null;

		if (dbCached == null) {
			dbCached = ConfigUtils.getDB("transfers");

			if (dbCached == null) {
				logger.log(Level.WARNING, "Could not connect to the transfers database");

				lastTimeNoWork = System.currentTimeMillis();

				return null;
			}

			dbCached.setReadOnly(true);
			dbCached.setQueryTimeout(300);
		}

		cleanup();

		touch(null, agent);

		long transferId = -1;
		String sLFN = null;
		String targetSE = null;
		String onDeleteRemoveReplica = null;

		while (transferId < 0) {
			if (!dbCached.moveNext()) {
				// dbCached.query(
				// "select transferId,lfn,destination,remove_replica from TRANSFERS_DIRECT inner join PROTOCOLS on sename=destination and status='WAITING' LEFT OUTER JOIN (select se_name, count(1) as
				// active_cnt from active_transfers group by se_name) a on (se_name=sename) where max_transfers>0 and (active_cnt is null or active_cnt<max_transfers) group by 1,2,3,4 order by
				// coalesce(max(active_cnt),0)/sum(max_transfers) asc, transferId-1000*attempts asc limit 50;");

				dbCached.query(
						"select /*! SQL_BUFFER_RESULT */ /*! SQL_SMALL_RESULT */  transferId, lfn, destination, remove_replica from TRANSFERS_DIRECT inner join (select sename, sum(max_transfers) mt, coalesce(max(active_cnt),0) at from PROTOCOLS left outer join (select se_name, count(1) as active_cnt from active_transfers group by se_name) a on (se_name=sename) group by sename having at<mt) b ON destination=sename where status='WAITING' order by at/mt asc, transferId-1000*attempts asc limit 100;");

				if (!dbCached.moveNext()) {
					logger.log(Level.FINE, "There is no waiting transfer in the queue");

					lastTimeNoWork = System.currentTimeMillis() + 30 * 1000 + ThreadLocalRandom.current().nextInt(30 * 1000);

					return null;
				}
			}

			final Set<String> ignoredSEs = new HashSet<>();

			do
				try (DBFunctions db = ConfigUtils.getDB("transfers")) {
					transferId = dbCached.getl(1);
					sLFN = dbCached.gets(2);
					targetSE = dbCached.gets(3);
					onDeleteRemoveReplica = dbCached.gets(4);

					if (transferId < 0 || sLFN == null || sLFN.length() == 0 || targetSE == null || targetSE.length() == 0) {
						logger.log(Level.INFO, "Transfer details are wrong");

						lastTimeNoWork = System.currentTimeMillis();

						return null;
					}

					if (ignoredSEs.contains(targetSE.toLowerCase())) {
						transferId = -1;

						continue;
					}

					db.setReadOnly(true);
					db.setQueryTimeout(60);

					db.query("SELECT count(1) FROM active_transfers WHERE se_name=?", false, targetSE);

					db.setReadOnly(false);

					if (db.geti(1) >= getMaxTransfers(targetSE)) {
						ignoredSEs.add(targetSE.toLowerCase());

						transferId = -1;

						continue;
					}

					final DBConnection dbc = db.getConnection();

					executeQuery(dbc, "lock tables TRANSFERS_DIRECT write, active_transfers write;");

					try {
						executeQuery(dbc, "update TRANSFERS_DIRECT set status='TRANSFERRING' where transferId=" + transferId + " AND status='WAITING';");

						if (updateCount == 0) {
							logger.log(Level.INFO, "Concurrent selection of " + transferId + ", retrying");
							transferId = -1;
							continue;
						}

						executeQuery(dbc, "insert into active_transfers (last_active, se_name, transfer_id, transfer_agent_id, pid, host) VALUES (" + System.currentTimeMillis() / 1000 + ", " + "'"
								+ Format.escSQL(targetSE) + "', " + transferId + ", " + agent.getTransferAgentID() + ", " + agent.getPID() + ", '" + Format.escSQL(agent.getHostName()) + "');");

					}
					finally {
						executeQuery(dbc, "unlock tables;");
						executeClose();

						dbc.free();
					}

					break;
				}
				catch (final Exception e) {
					logger.log(Level.WARNING, "Exception fetching data from the query", e);
					// ignore
				}
			while (dbCached.moveNext());
		}

		GUID guid;
		final LFN lfn;

		boolean runningOnGUID = false;

		if (GUIDUtils.isValidGUID(sLFN)) {
			guid = GUIDUtils.getGUID(sLFN);

			if (guid == null) {
				logger.log(Level.WARNING, "GUID '" + sLFN + "' doesn't exist in the catalogue for transfer ID " + transferId);
				markTransfer(transferId, Transfer.FAILED_SYSTEM, "GUID was not found in the database");
				return null;
			}

			// because of this only admin will be allowed to mirror GUIDs
			// without indicating the LFN (eg for storage replication)
			lfn = LFNUtils.getLFN("/" + sLFN, true);
			lfn.guid = guid.guid;
			lfn.size = guid.size;
			lfn.md5 = guid.md5;

			guid.lfnCache = new HashSet<>();
			guid.lfnCache.add(lfn);

			runningOnGUID = true;
		}
		else {
			lfn = LFNUtils.getLFN(sLFN);

			if (lfn == null || !lfn.exists) {
				logger.log(Level.WARNING, "LFN '" + sLFN + "' doesn't exist in the catalogue for transfer ID " + transferId);
				markTransfer(transferId, Transfer.FAILED_SYSTEM, "LFN doesn't exist in the catalogue");
				return null;
			}

			logger.log(Level.FINE, transferId + " : LFN is " + lfn);

			if (lfn.guid == null) {
				logger.log(Level.WARNING, "GUID '" + lfn.guid + "' is null for transfer ID " + transferId + ", lfn '" + sLFN + "'");
				markTransfer(transferId, Transfer.FAILED_SYSTEM, "GUID is null for this LFN");
				return null;
			}

			guid = GUIDUtils.getGUID(lfn);

			if (guid == null) {
				logger.log(Level.WARNING, "GUID '" + lfn.guid + "' doesn't exist in the catalogue for transfer ID " + transferId + ", lfn '" + sLFN + "'");
				markTransfer(transferId, Transfer.FAILED_SYSTEM, "GUID was not found in the database");
				return null;
			}

			guid.lfnCache = new HashSet<>();
			guid.lfnCache.add(lfn);
		}

		logger.log(Level.FINE, transferId + " : GUID is " + guid);

		final Set<PFN> pfns;

		if (!runningOnGUID) {
			pfns = lfn.whereisReal();

			if (pfns != null)
				for (final PFN p : pfns) {
					final GUID pfnGUID = p.getGuid();

					if (!pfnGUID.equals(guid)) {
						logger.log(Level.INFO, "Switching to mirroring " + pfnGUID.guid + " instead of " + guid.guid + " because this is the real file for " + lfn.getCanonicalName());

						guid = pfnGUID; // switch to mirroring the archive
										// instead of the pointer to it

						break;
					}
				}
		}
		else {
			final Set<GUID> realGUIDs = guid.getRealGUIDs();

			pfns = new LinkedHashSet<>();

			if (realGUIDs != null && realGUIDs.size() > 0)
				for (final GUID realId : realGUIDs) {
					final Set<PFN> replicas = realId.getPFNs();

					if (replicas == null)
						continue;

					pfns.addAll(replicas);

					if (!guid.equals(realId)) {
						logger.log(Level.INFO, "Switching to mirroring " + realId.guid + " instead of " + guid.guid + " because this is the real file");

						guid = realId; // switch to mirroring the archive
										// instead of the pointer to it
					}
				}
		}

		if (pfns == null || pfns.size() == 0) {
			logger.log(Level.WARNING, "No existing replicas to mirror for transfer ID " + transferId);
			markTransfer(transferId, Transfer.FAILED_SYSTEM, "No replicas to mirror");
			touch(null, agent);
			return null;
		}

		final StringTokenizer seTargetSEs = new StringTokenizer(targetSE, ",; \t\r\n");

		final Collection<PFN> targets = new ArrayList<>();

		final int targetSEsCount = seTargetSEs.countTokens();

		int replicaExists = 0;
		int seDoesntExist = 0;
		int sourceAuthFailed = 0;
		int targetAuthFailed = 0;

		String lastReason = null;

		while (seTargetSEs.hasMoreTokens()) {
			final SE se = SEUtils.getSE(seTargetSEs.nextToken());

			if (se == null) {
				logger.log(Level.WARNING, "Target SE '" + targetSE + "' doesn't exist for transfer ID " + transferId);
				seDoesntExist++;
				continue;
			}

			logger.log(Level.FINE, transferId + " : Target SE is " + se);

			boolean replicaFound = false;

			for (final PFN pfn : pfns)
				if (se.equals(pfn.getSE())) {
					logger.log(Level.WARNING, "There already exists a replica of '" + sLFN + "' on '" + targetSE + "' for transfer ID " + transferId);
					replicaExists++;
					replicaFound = true;
					continue;
				}

			if (replicaFound)
				continue;

			int localSourceAuthFailed = 0;

			for (final PFN source : pfns)
				if (source.ticket == null) {
					final String reason = AuthorizationFactory.fillAccess(source, AccessType.READ);

					if (reason != null) {
						logger.log(Level.WARNING, "Could not obtain source authorization for transfer ID " + transferId + " : " + reason);
						sourceAuthFailed++;
						localSourceAuthFailed++;
						lastReason = reason;
						continue;
					}
				}

			if (localSourceAuthFailed == pfns.size())
				continue;

			final PFN target;

			AliEnPrincipal account = AuthorizationFactory.getDefaultUser();

			if (account.canBecome("admin"))
				account = UserFactory.getByUsername("admin");

			// target = BookingTable.bookForWriting(account, lfn, guid, null, se);
			target = new PFN(guid, se);
			final String reason = AuthorizationFactory.fillAccess(account, target, AccessType.WRITE);

			if (reason != null) {
				logger.log(Level.WARNING, "Could not obtain target authorization for transfer ID " + transferId + " : " + reason);
				targetAuthFailed++;
				lastReason = reason;
				continue;
			}

			logger.log(Level.FINE, transferId + " : booked PFN is " + target);

			targets.add(target);
		}

		if (targets.size() == 0) {
			String message = "";

			int exitCode = Transfer.FAILED_SYSTEM;

			if (targetSEsCount == 0)
				message = "No target SE indicated";
			else {
				if (replicaExists > 0) {
					message = "There is already a replica on " + (replicaExists > 1 ? "these storages" : "this storage") + (replicaExists < targetSEsCount ? " (" + replicaExists + ")" : "");

					if (replicaExists == targetSEsCount)
						exitCode = Transfer.OK;
				}

				if (seDoesntExist > 0) {
					if (message.length() > 0)
						message += ", ";

					message += "Target SE is not defined" + (seDoesntExist < targetSEsCount ? " (" + seDoesntExist + ")" : "");
				}

				if (sourceAuthFailed > 0) {
					if (message.length() > 0)
						message += ", ";

					message += "Source authorization failed: " + lastReason + (sourceAuthFailed < targetSEsCount ? " (" + sourceAuthFailed + ")" : "");
				}

				if (targetAuthFailed > 0) {
					if (message.length() > 0)
						message += ", ";

					message += "Target authorization failed: " + lastReason + (targetAuthFailed < targetSEsCount ? " (" + targetAuthFailed + ")" : "");
				}
			}

			markTransfer(transferId, exitCode, message);
			touch(null, agent);
			return null;
		}

		final Transfer t = new Transfer(transferId, pfns, targets, onDeleteRemoveReplica);

		reportMonitoring(t);

		return t;
	}

	private static final String getTransferStatus(final int exitCode) {
		switch (exitCode) {
			case Transfer.OK:
				return "DONE";
			case Transfer.FAILED_SOURCE:
				return "FAILED";
			case Transfer.FAILED_TARGET:
				return "FAILED";
			case Transfer.FAILED_UNKNOWN:
				return "FAILED";
			case Transfer.FAILED_SYSTEM:
				return "KILLED";
			case Transfer.DELAYED:
				return "WAITING";
			default:
				return "TRANSFERRING";
		}
	}

	private static final int getAliEnTransferStatus(final int exitCode) {
		switch (exitCode) {
			case Transfer.OK:
				return 7;
			case Transfer.FAILED_SOURCE:
				return -1;
			case Transfer.FAILED_TARGET:
				return -1;
			case Transfer.FAILED_UNKNOWN:
				return -1;
			case Transfer.FAILED_SYSTEM:
				return -2;
			case Transfer.DELAYED:
				return -3;
			default:
				return 5; // transferring
		}
	}

	private static long lastCleanedUp = 0;

	private static long lastArchived = System.currentTimeMillis();

	private void cleanup() {
		// no need to synchronize this method
		if (System.currentTimeMillis() - lastCleanedUp < 1000 * 60)
			return;

		lastCleanedUp = System.currentTimeMillis();

		try (DBFunctions db = ConfigUtils.getDB("transfers")) {
			if (db == null)
				return;

			db.query("UPDATE transfer_optimizers SET setting=" + lastCleanedUp + " WHERE activity=0 AND setting<" + (lastCleanedUp - 1000 * 60));

			if (db.getUpdateCount() > 0) {
				final DBConnection dbc = db.getConnection();
				executeQuery(dbc, "SET autocommit = 0;");
				executeQuery(dbc, "lock tables TRANSFERS_DIRECT write, PROTOCOLS read, active_transfers write;");

				try {
					executeQuery(dbc, "DELETE FROM active_transfers WHERE last_active<" + (lastCleanedUp / 1000 - 600));

					executeQuery(dbc, "UPDATE TRANSFERS_DIRECT SET status='KILLED', attempts=attempts-1, finished=" + lastCleanedUp / 1000
							+ ", reason='TransferAgent no longer active' WHERE status='TRANSFERRING' AND transferId NOT IN (SELECT transfer_id FROM active_transfers);");

					executeQuery(dbc, "UPDATE TRANSFERS_DIRECT SET status='WAITING', finished=0 WHERE (status='INSERTING') OR ((status='FAILED' OR status='KILLED') AND (attempts>=0));");
				}
				finally {
					executeQuery(dbc, "commit;");
					executeQuery(dbc, "unlock tables;");
					executeQuery(dbc, "SET autocommit = 1;");

					executeClose();

					dbc.free();
				}
			}
		}
		catch (final Throwable t) {
			logger.log(Level.SEVERE, "Exception cleaning up", t);
		}

		// only check once an hour. the actual execution interval is in the below query
		if (System.currentTimeMillis() - lastArchived < 1000 * 60 * 60)
			return;

		lastArchived = System.currentTimeMillis();

		final String archiveTableName = "TRANSFERSARCHIVE" + Calendar.getInstance().get(Calendar.YEAR);

		// keep finished transfers in the active table for one day
		final long limit = System.currentTimeMillis() / 1000 - 60L * 60 * 24 * 1;

		// transfers waiting for longer than 2 weeks are also archived, they should have been picked up in the mean time
		final long limitReceived = System.currentTimeMillis() / 1000 - 60L * 60 * 24 * 14;

		try (DBFunctions db = ConfigUtils.getDB("transfers")) {
			if (db == null)
				return;

			// archive every 3 hours
			db.query("UPDATE transfer_optimizers SET setting=" + lastArchived + " WHERE activity=1 AND setting<" + (lastArchived - 1000L * 60 * 60 * 3));

			if (db.getUpdateCount() <= 0)
				return;

			if (!db.query("SELECT 1 FROM " + archiveTableName + " LIMIT 1;", true) && !db.query("CREATE TABLE " + archiveTableName + " LIKE TRANSFERS_DIRECT;")) {
				logger.log(Level.SEVERE, "Exception creating the archive table " + archiveTableName);
				return;
			}
		}

		try (DBFunctions db = ConfigUtils.getDB("transfers")) {
			final DBConnection dbc = db.getConnection();

			try {
				if (!executeQuery(dbc, "lock tables TRANSFERS_DIRECT write, " + archiveTableName + " write;")) {
					logger.log(Level.SEVERE, "Cannot lock main and archive tables for archival operation");
					return;
				}

				if (executeQuery(dbc, "INSERT IGNORE INTO " + archiveTableName + " SELECT * FROM TRANSFERS_DIRECT WHERE (finished<" + limit + " AND finished>0) OR (received<" + limitReceived + ");"))
					executeQuery(dbc, "DELETE FROM TRANSFERS_DIRECT WHERE (finished<" + limit + " AND finished>0) OR (received<" + limitReceived + ");");
			}
			finally {
				executeQuery(dbc, "unlock tables;");
				executeClose();

				dbc.free();
			}
		}
		catch (final Throwable t) {
			logger.log(Level.SEVERE, "Exception archiving", t);
		}
		finally {
			lastArchived = System.currentTimeMillis();
		}
	}

	/**
	 * Mark a transfer as active
	 *
	 * @param t
	 * @param ta
	 * @return <code>false</code> if the operation cannot be performed
	 */
	public static synchronized boolean touch(final Transfer t, final TransferAgent ta) {
		try (DBFunctions db = ConfigUtils.getDB("transfers")) {
			if (db == null)
				return false;

			db.setQueryTimeout(600);

			if (t == null) {
				db.query("DELETE FROM active_transfers WHERE transfer_agent_id=? AND pid=? AND host=?;", false, ta.getTransferAgentID(), Integer.valueOf(ta.getPID()), ta.getHostName());
				return true;
			}

			db.setReadOnly(true);

			db.query("SELECT transfer_agent_id, pid, host FROM active_transfers WHERE transfer_id=? AND (transfer_agent_id!=? OR pid!=? OR host!=?);", false, Long.valueOf(t.getTransferId()),
					ta.getTransferAgentID(), Integer.valueOf(MonitorFactory.getSelfProcessID()), ConfigUtils.getLocalHostname());

			db.setReadOnly(false);

			if (db.moveNext()) {
				logger.log(Level.WARNING,
						"Transfer " + t.getTransferId() + " was already picked up by agent #" + db.gets(1) + " @ " + db.gets(3) + "/" + db.gets(2) + ", refusing to concurrently execute it.");

				return false;
			}

			final Map<String, Object> values = new HashMap<>();

			final StringBuilder seList = new StringBuilder();

			for (final PFN pfn : t.targets) {
				final SE targetSE = pfn.getSE();

				if (targetSE != null) {
					if (seList.length() > 0)
						seList.append(',');

					seList.append(targetSE.seName);
				}
			}

			if (seList.length() > 0)
				values.put("se_name", seList.toString());
			else
				values.put("se_name", "unknown");

			values.put("last_active", Long.valueOf(System.currentTimeMillis() / 1000));
			values.put("transfer_id", Long.valueOf(t.getTransferId()));
			values.put("transfer_agent_id", ta.getTransferAgentID());
			values.put("pid", Integer.valueOf(MonitorFactory.getSelfProcessID()));
			values.put("host", ConfigUtils.getLocalHostname());

			if (t.lastTriedSE > 0) {
				final SE se = SEUtils.getSE(t.lastTriedSE);

				if (se != null)
					values.put("active_source", se.seName);
				else
					values.put("active_source", "unknown");
			}
			else
				values.put("active_source", "");

			if (t.lastTriedProtocol != null)
				values.put("active_protocol", t.lastTriedProtocol.toString());
			else
				values.put("active_protocol", "");

			db.query(DBFunctions.composeUpdate("active_transfers", values, Arrays.asList("transfer_agent_id", "pid", "host")));

			if (db.getUpdateCount() == 0)
				db.query(DBFunctions.composeInsert("active_transfers", values));

			db.setReadOnly(true);

			db.query("SELECT status FROM TRANSFERS_DIRECT WHERE transferId=?;", false, Long.valueOf(t.getTransferId()));

			db.setReadOnly(false);

			final String prevStatus = db.gets(1);

			if (db.moveNext() && "TRANSFERRING".equalsIgnoreCase(prevStatus)) {
				db.query("UPDATE TRANSFERS_DIRECT SET status='TRANSFERRING', reason='', finished=null WHERE transferId=" + t.getTransferId() + " AND status!='TRANSFERRING';");

				if (db.getUpdateCount() > 0)
					logger.log(Level.INFO, "Re-stated " + t.getTransferId() + " from " + prevStatus + " to TRANSFERRING");
			}
		}
		catch (final Throwable ex) {
			logger.log(Level.SEVERE, "Exception updating status", ex);
		}

		return true;
	}

	private static boolean markTransfer(final long transferId, final int exitCode, final String reason) {
		try (DBFunctions db = ConfigUtils.getDB("transfers")) {
			if (db == null)
				return false;

			db.setQueryTimeout(600);

			String formattedReason = reason;

			if (formattedReason != null && formattedReason.length() > 250)
				formattedReason = formattedReason.substring(0, 250);

			int finalExitCode = exitCode;

			final Long transfer = Long.valueOf(transferId);

			if (exitCode > Transfer.OK && exitCode < Transfer.DELAYED) {
				db.setReadOnly(true);
				db.query("SELECT attempts FROM TRANSFERS_DIRECT WHERE transferId=?;", false, transfer);
				db.setReadOnly(false);

				if (db.moveNext() && db.geti(1) > 0)
					finalExitCode = Transfer.DELAYED;
			}

			db.query("update TRANSFERS_DIRECT set status=?, reason=?, finished=?, attempts=attempts-1 WHERE transferId=?;", false, getTransferStatus(finalExitCode), formattedReason,
					Long.valueOf(System.currentTimeMillis() / 1000), transfer);

			if (db.getUpdateCount() < 1)
				return false;

			db.query("update PROTOCOLS set current_transfers=greatest(coalesce(current_transfers,0)-1,0) WHERE sename=(SELECT destination FROM TRANSFERS_DIRECT WHERE transferId=?);", false, transfer);
		}

		return true;
	}

	private static final void reportMonitoring(final Transfer t) {
		try {
			final ApMon apmon;

			try {
				final Vector<String> targets = new Vector<>();
				targets.add(ConfigUtils.getConfig().gets("CS_ApMon", "aliendb4.cern.ch"));

				apmon = new ApMon(targets);
			}
			catch (final Exception e) {
				logger.log(Level.WARNING, "Could not initialize apmon", e);
				return;
			}

			final String cluster = "TransferQueue_Transfers_" + ConfigUtils.getConfig().gets("Organization", "ALICE");

			final Vector<String> p = new Vector<>();
			final Vector<Object> v = new Vector<>();

			p.add("statusID");
			v.add(Integer.valueOf(getAliEnTransferStatus(t.getExitCode())));

			p.add("size");
			v.add(Double.valueOf(t.sources.iterator().next().getGuid().size));

			p.add("started");
			v.add(Double.valueOf(t.startedWork / 1000d));

			if (t.getExitCode() >= Transfer.OK) {
				p.add("finished");
				v.add(Double.valueOf(System.currentTimeMillis() / 1000d));

				if (t.lastTriedSE > 0) {
					final SE se = SEUtils.getSE(t.lastTriedSE);

					if (se != null) {
						p.add("SE");
						v.add(se.seName);
					}
				}

				if (t.lastTriedProtocol != null) {
					p.add("Protocol");
					v.add(t.lastTriedProtocol.toString());
				}
			}

			String owner = null;
			final StringBuilder seList = new StringBuilder();

			for (final PFN target : t.targets) {
				final SE targetSE = target.getSE();
				if (targetSE != null) {
					if (seList.length() > 0)
						seList.append(',');

					seList.append(targetSE.seName);
				}

				if (owner == null)
					owner = target.getGuid().owner;
			}

			if (seList.length() > 0) {
				p.add("destination");
				v.add(seList.toString());
			}

			if (owner != null) {
				p.add("user");
				v.add(owner);
			}

			try {
				apmon.sendParameters(cluster, String.valueOf(t.getTransferId()), p.size(), p, v);
			}
			catch (final Exception e) {
				logger.log(Level.WARNING, "Could not send apmon message: " + p + " -> " + v, e);
			}
			finally {
				apmon.stopIt();
			}
		}
		catch (final Throwable ex) {
			logger.log(Level.WARNING, "Exception reporting the monitoring", ex);
		}
	}

	/**
	 * When a transfer has completed, call this method to update the database status
	 *
	 * @param t
	 */
	public static void notifyTransferComplete(final Transfer t) {
		// TODO : verify the storage reply envelope here

		markTransfer(t.getTransferId(), t.getExitCode(), t.getFailureReason());

		reportMonitoring(t);

		for (final PFN target : t.getSuccessfulTransfers()) {
			if (!target.getGuid().addPFN(target)) {
				logger.log(Level.WARNING, "Could not commit booked transfer: " + target);

				markTransfer(t.getTransferId(), Transfer.FAILED_SYSTEM, "Could not commit booked transfer: " + target);
				return;
			}

			// cleanup of potentially booked PFNs that are otherwise going to be garbage collected and removed from the storage
			try (DBFunctions db = ConfigUtils.getDB("alice_users")) {
				if (db != null)
					db.query("DELETE FROM LFN_BOOKED WHERE pfn=?", false, target.getPFN());
			}
		}

		if (t.getSuccessfulTransfers().size() > 0 && t.onCompleteRemoveReplica != null && t.onCompleteRemoveReplica.length() > 0) {
			GUID g = null;

			for (final PFN p : t.sources) {
				g = p.getGuid();

				if (g != null)
					break;
			}

			if (g == null)
				for (final PFN p : t.targets) {
					g = p.getGuid();

					if (g != null)
						break;
				}

			if (g != null) {
				if (g.removePFN(SEUtils.getSE(t.onCompleteRemoveReplica), true) == null)
					logger.log(Level.WARNING, "Was asked to remove the replica on " + t.onCompleteRemoveReplica + " of transfer ID " + t.getTransferId() + " but the removal didn't work");
			}
			else
				logger.log(Level.WARNING,
						"Was asked to remove the replica on " + t.onCompleteRemoveReplica + " of transfer ID " + t.getTransferId() + " but I cannot do that since the GUID is unknown");
		}
	}
}
