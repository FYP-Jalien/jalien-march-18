package alien.taskQueue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import alien.api.Dispatcher;
import alien.api.ServerException;
import alien.api.catalogue.LFNfromString;
import alien.catalogue.BookingTable;
import alien.catalogue.CatalogueUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.PackageUtils;
import alien.config.ConfigUtils;
import alien.io.IOUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import alien.quotas.FileQuota;
import alien.quotas.QuotaUtilities;
import alien.shell.ErrNo;
import alien.user.AliEnPrincipal;
import alien.user.AuthorizationChecker;
import alien.user.LDAPHelper;
import alien.user.UsersHelper;
import apmon.ApMon;
import lazyj.DBFunctions;
import lazyj.Format;
import lazyj.StringFactory;
import lazyj.Utils;
import lazyj.cache.ExpirationCache;
import lazyj.cache.GenericLastValuesCache;

/**
 * @author ron
 * @since Mar 1, 2011
 */
public class TaskQueueUtils {

	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(TaskQueueUtils.class.getCanonicalName());

	/**
	 * Monitoring component
	 */
	static final Monitor monitor = MonitorFactory.getMonitor(TaskQueueUtils.class.getCanonicalName());

	/**
	 * Flag that tells if the QUEUE table is v2.20+ (JDL text in QUEUEJDL, using status, host, user, notification ids and so on instead of the string versions)
	 */
	public static final boolean dbStructure2_20;

	private static final Map<String, String> fieldMap;

	static {
		fieldMap = new HashMap<>();
		fieldMap.put("path_table", "QUEUEJDL");
		fieldMap.put("path_field", "path");
		fieldMap.put("spyurl_table", "QUEUEPROC");
		fieldMap.put("spyurl_field", "spyurl");
		fieldMap.put("node_table", "QUEUE");
		fieldMap.put("node_field", "nodeId");
		fieldMap.put("exechost_table", "QUEUE");
		fieldMap.put("exechost_field", "execHostId");
	}

	static {
		if (ConfigUtils.isCentralService()) {
			try (DBFunctions db = getQueueDB()) {
				if (db != null) {
					db.setReadOnly(true);
					db.setQueryTimeout(30);

					db.query("select count(1) from information_schema.tables where table_schema='processes' and table_name='QUEUEJDL';");

					dbStructure2_20 = db.geti(1) == 1;
				}
				else {
					logger.log(Level.WARNING, "There is no direct database connection to the task queue.");

					dbStructure2_20 = false;
				}
			}
		}
		else
			dbStructure2_20 = false;
	}

	// private static final DateFormat formatter = new
	// SimpleDateFormat("MMM dd HH:mm");

	/**
	 * @return the database connection to 'processes'
	 */
	public static DBFunctions getQueueDB() {
		final DBFunctions db = ConfigUtils.getDB("processes");
		return db;
	}

	/**
	 * @return the database connection to 'ADMIN'
	 */
	public static DBFunctions getAdminDB() {
		final DBFunctions db = ConfigUtils.getDB("admin");
		return db;
	}

	/**
	 * Get the Job from the QUEUE
	 *
	 * @param queueId
	 * @return the job, or <code>null</code> if it cannot be located
	 */
	public static Job getJob(final long queueId) {
		return getJob(queueId, false);
	}

	private static final String ALL_BUT_JDL = "queueId, priority, execHost, sent, split, name, spyurl, commandArg, finished, masterjob, status, splitting, node, error, current, received, validate, command, merging, submitHost, path, site, started, expires, finalPrice, effectivePriority, price, si2k, jobagentId, agentid, notify, chargeStatus, optimized, mtime";

	/**
	 * Get the Job from the QUEUE
	 *
	 * @param queueId
	 * @param loadJDL
	 * @return the job, or <code>null</code> if it cannot be located
	 */
	public static Job getJob(final long queueId, final boolean loadJDL) {
		return getJob(queueId, loadJDL, 0);
	}

	/**
	 * Get the Job from the QUEUE
	 *
	 * @param queueId
	 * @param loadJDL
	 * @param archiveYear
	 *            queue archive year to query instead of the main queue
	 * @return the job, or <code>null</code> if it cannot be located
	 */
	public static Job getJob(final long queueId, final boolean loadJDL, final int archiveYear) {
		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return null;

			db.setQueryTimeout(300);

			if (monitor != null) {
				monitor.incrementCounter("TQ_db_lookup");
				monitor.incrementCounter("TQ_jobdetails");
			}

			final String q;

			if (dbStructure2_20) {
				if (archiveYear < 2000) {
					if (loadJDL)
						q = "SELECT QUEUE.*,origJdl as JDL, path FROM QUEUE INNER JOIN QUEUEJDL using(queueId) WHERE queueId=?";
					else
						q = "SELECT * FROM QUEUE WHERE queueId=?";
				}
				else if (loadJDL)
					q = "SELECT *,origJdl as JDL FROM QUEUEARCHIVE" + archiveYear + " WHERE queueId=?";
				else
					q = "SELECT * FROM QUEUEARCHIVE" + archiveYear + " WHERE queueId=?";
			}
			else if (archiveYear < 2000)
				q = "SELECT " + (loadJDL ? "*" : ALL_BUT_JDL) + " FROM QUEUE WHERE queueId=?";
			else
				q = "SELECT " + (loadJDL ? "*" : ALL_BUT_JDL) + " FROM QUEUEARCHIVE" + archiveYear + " WHERE queueId=?";

			db.setReadOnly(true);

			try (Timing t = new Timing(monitor, "TQ_jobdetails_ms")) {
				if (!db.query(q, false, Long.valueOf(queueId)))
					return null;
			}

			if (!db.moveNext())
				return null;

			return new Job(db, loadJDL);
		}
	}

	/**
	 * Get the list of active masterjobs
	 *
	 * @param account
	 *            the account for which the masterjobs are needed, or <code>null</code> for all active masterjobs, of everybody
	 * @return the list of active masterjobs for this account
	 */
	public static List<Job> getMasterjobs(final String account) {
		return getMasterjobs(account, false);
	}

	/**
	 * Get the list of active masterjobs
	 *
	 * @param account
	 *            the account for which the masterjobs are needed, or <code>null</code> for all active masterjobs, of everybody
	 * @param loadJDL
	 * @return the list of active masterjobs for this account
	 */
	public static List<Job> getMasterjobs(final String account, final boolean loadJDL) {
		return getMasterjobs(account, loadJDL, 0);
	}

	/**
	 * Get the list of active masterjobs
	 *
	 * @param account
	 *            the account for which the masterjobs are needed, or <code>null</code> for all active masterjobs, of everybody
	 * @param loadJDL
	 * @param archiveYear
	 *            queue archive year to query instead of the main queue
	 * @return the list of active masterjobs for this account
	 */
	public static List<Job> getMasterjobs(final String account, final boolean loadJDL, final int archiveYear) {
		final List<Job> ret = new ArrayList<>();

		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return null;

			if (monitor != null) {
				monitor.incrementCounter("TQ_db_lookup");
				monitor.incrementCounter("TQ_getmasterjobs");
			}

			String q;

			if (dbStructure2_20) {
				if (archiveYear < 2000) {
					if (loadJDL)
						q = "SELECT QUEUE.*,origJdl as JDL, path FROM QUEUE INNER JOIN QUEUEJDL using(queueId) ";
					else
						q = "SELECT * FROM QUEUE ";
				}
				else if (loadJDL)
					q = "SELECT *,origJdl as JDL FROM QUEUEARCHIVE" + archiveYear + " ";
				else
					q = "SELECT * FROM QUEUEARCHIVE" + archiveYear + " ";

				q += "WHERE split=0 AND statusId!=" + JobStatus.KILLED.getAliEnLevel() + " ";
			}
			else if (archiveYear < 2000)
				q = "SELECT " + (loadJDL ? "*" : ALL_BUT_JDL) + " FROM QUEUE WHERE split=0 AND status!='KILLED' ";
			else
				q = "SELECT " + (loadJDL ? "*" : ALL_BUT_JDL) + " FROM QUEUEARCHIVE" + archiveYear + " WHERE split=0 AND status!='KILLED' ";

			if (account != null && account.length() > 0)
				if (dbStructure2_20)
					q += "AND userId=" + getUserId(account);
				else
					q += "AND submitHost LIKE '" + Format.escSQL(account) + "@%'";

			q += " AND received>UNIX_TIMESTAMP(now())-60*60*24*14";

			db.setReadOnly(true);
			db.setQueryTimeout(600);

			try (Timing t = new Timing(monitor, "TQ_getmasterjobs_ms")) {
				db.query(q);
			}

			while (db.moveNext())
				ret.add(new Job(db, loadJDL));

			Collections.sort(ret);
		}

		return ret;
	}

	/**
	 * @param initial
	 * @param maxAge
	 *            the age in milliseconds
	 * @return the jobs that are active or have finished since at most maxAge
	 */
	public static List<Job> filterMasterjobs(final List<Job> initial, final long maxAge) {
		if (initial == null)
			return null;

		final List<Job> ret = new ArrayList<>(initial.size());

		final long now = System.currentTimeMillis();

		for (final Job j : initial)
			if (j.isActive() || j.mtime == null || (now - j.mtime.getTime() < maxAge))
				ret.add(j);

		return ret;
	}

	/**
	 * @param account
	 * @return the masterjobs for this account and the subjob statistics for them
	 */
	public static Map<Job, Map<JobStatus, Integer>> getMasterjobStats(final String account) {
		return getMasterjobStats(getMasterjobs(account));
	}

	/**
	 * @param jobs
	 * @return the same masterjobs and the respective subjob statistics
	 */
	public static Map<Job, Map<JobStatus, Integer>> getMasterjobStats(final List<Job> jobs) {
		final Map<Job, Map<JobStatus, Integer>> ret = new TreeMap<>();

		if (jobs.size() == 0)
			return ret;

		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return ret;

			final StringBuilder sb = new StringBuilder(jobs.size() * 10);

			final Map<Long, Job> reverse = new HashMap<>();

			for (final Job j : jobs) {
				if (sb.length() > 0)
					sb.append(',');

				sb.append(j.queueId);

				reverse.put(Long.valueOf(j.queueId), j);
			}

			if (monitor != null) {
				monitor.incrementCounter("TQ_db_lookup");
				monitor.incrementCounter("TQ_getmasterjob_stats");
			}

			final String q;

			if (dbStructure2_20)
				q = "select split,statusId,count(1) from QUEUE where split in (" + sb.toString() + ") AND statusId!=" + JobStatus.KILLED.getAliEnLevel() + " group by split,statusId order by 1;";
			else
				q = "select split,status,count(1) from QUEUE where split in (" + sb.toString() + ") AND status!='KILLED' group by split,status order by 1;";

			db.setReadOnly(true);
			db.setQueryTimeout(600);

			try (Timing timing = new Timing(monitor, "TQ_getmasterjob_stats_ms")) {
				db.query(q);
			}

			Map<JobStatus, Integer> m = null;
			long oldJobID = -1;

			while (db.moveNext()) {
				final long j = db.getl(1);

				if (j != oldJobID || m == null) {
					m = new HashMap<>();

					final Long jobId = Long.valueOf(j);
					ret.put(reverse.get(jobId), m);
					reverse.remove(jobId);

					oldJobID = j;
				}

				JobStatus status;

				if (dbStructure2_20)
					status = JobStatus.getStatusByAlien(Integer.valueOf(db.geti(2)));
				else
					status = JobStatus.getStatus(db.gets(2));

				m.put(status, Integer.valueOf(db.geti(3)));
			}

			// now, what is left, something that doesn't have subjobs ?
			for (final Job j : reverse.values()) {
				m = new HashMap<>(1);
				m.put(j.status(), Integer.valueOf(1));
				ret.put(j, m);
			}

			return ret;
		}
	}

	/**
	 * Get the subjobs of this masterjob
	 *
	 * @param queueId
	 * @return the subjobs, if any
	 */
	public static List<Job> getSubjobs(final long queueId) {
		return getSubjobs(queueId, false);
	}

	/**
	 * Get the subjobs of this masterjob
	 *
	 * @param queueId
	 * @param loadJDL
	 * @return the subjobs, if any
	 */
	public static List<Job> getSubjobs(final long queueId, final boolean loadJDL) {
		return getSubjobs(queueId, loadJDL, 0);
	}

	/**
	 * Get the subjobs of this masterjob
	 *
	 * @param queueId
	 * @param loadJDL
	 * @param archiveYear
	 *            archive year to query instead of the main queue table
	 * @return the subjobs, if any
	 */
	public static List<Job> getSubjobs(final long queueId, final boolean loadJDL, final int archiveYear) {
		final List<Job> ret = new ArrayList<>();

		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return null;

			if (monitor != null) {
				monitor.incrementCounter("TQ_db_lookup");
				monitor.incrementCounter("TQ_getsubjobs");
			}

			String q;

			if (dbStructure2_20) {
				if (archiveYear < 2000) {
					if (loadJDL)
						q = "SELECT QUEUE.*,origJdl AS jdl, path FROM QUEUE INNER JOIN QUEUEJDL using(queueId)";
					else
						q = "SELECT * FROM QUEUE";
				}
				else if (loadJDL)
					q = "SELECT *,origJdl AS jdl FROM QUEUEARCHIVE" + archiveYear;
				else
					q = "SELECT * FROM QUEUEARCHIVE" + archiveYear;

				q += " WHERE split=? AND statusId!=" + JobStatus.KILLED.getAliEnLevel();
			}
			else if (archiveYear < 2000)
				q = "SELECT " + (loadJDL ? "*" : ALL_BUT_JDL) + " FROM QUEUE WHERE split=? AND status!='KILLED';";
			else
				q = "SELECT " + (loadJDL ? "*" : ALL_BUT_JDL) + " FROM QUEUEARCHIVE" + archiveYear + " WHERE split=? AND status!='KILLED';";

			db.setReadOnly(true);
			db.setQueryTimeout(300);

			try (Timing timing = new Timing(monitor, "TQ_getsubjobs_ms")) {
				db.query(q, false, Long.valueOf(queueId));
			}

			while (db.moveNext())
				ret.add(new Job(db, loadJDL));

			Collections.sort(ret);
		}

		return ret;
	}

	/**
	 * Get the subjob status of this masterjob
	 *
	 * @param queueId
	 * @param status
	 * @param id
	 * @param site
	 * @param limit
	 * @return the subjobs, if any
	 */
	public static List<Job> getMasterJobStat(final long queueId, final Set<JobStatus> status, final List<Long> id, final List<String> site, final int limit) {

		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return null;

			if (monitor != null)
				monitor.incrementCounter("TQ_db_lookup");

			String where = "";

			if (queueId > 0)
				where = " split=" + queueId + " and ";
			else
				return null;

			if (status != null && status.size() > 0 && !status.contains(JobStatus.ANY)) {
				final StringBuilder whe = new StringBuilder(" ( statusId in (");

				boolean first = true;

				for (final JobStatus s : status) {
					if (!first)
						whe.append(',');
					else
						first = false;

					if (dbStructure2_20)
						whe.append(s.getAliEnLevel());
					else
						whe.append('\'').append(s.toSQL()).append('\'');
				}

				where += whe + ") ) and ";
			}

			if (id != null && id.size() > 0) {
				final StringBuilder whe = new StringBuilder(" ( queueId in (");

				boolean first = true;

				for (final long i : id) {
					if (!first)
						whe.append(',');
					else
						first = false;

					whe.append(i);
				}

				where += whe + ") ) and ";
			}

			if (site != null && site.size() > 0) {
				final StringBuilder whe = new StringBuilder(" ( ");

				boolean first = true;

				for (final String s : site) {
					if (!first)
						whe.append(" or ");
					else
						first = false;

					if (dbStructure2_20)
						whe.append("ifnull(substring(exechost,POSITION('\\@' in exechost)+1),'')='").append(Format.escSQL(s)).append('\'');
					else
						whe.append("execHostId=").append(getHostId(s));
				}

				where += whe.substring(0, whe.length() - 3) + " ) and ";
			}

			if (dbStructure2_20)
				where += " statusId!=" + JobStatus.KILLED.getAliEnLevel();
			else
				where += " status!='KILLED' ";

			int lim = 20000;
			if (limit > 0 && limit < 100000)
				lim = limit;

			final String q;

			if (dbStructure2_20)
				q = "SELECT queueId,statusId,split,execHostId FROM QUEUE WHERE " + where + " ORDER BY queueId ASC limit " + lim + ";";
			else
				q = "SELECT queueId,status,split,execHost FROM QUEUE WHERE " + where + " ORDER BY queueId ASC limit " + lim + ";";

			db.setReadOnly(true);
			db.setQueryTimeout(600);

			if (!db.query(q))
				return null;

			final List<Job> ret = new ArrayList<>();

			while (db.moveNext())
				ret.add(new Job(db, false));

			return ret;
		}
	}

	/**
	 * @param jobs
	 * @return the jobs grouped by their state
	 */
	public static Map<JobStatus, List<Job>> groupByStates(final List<Job> jobs) {
		if (jobs == null)
			return null;

		final Map<JobStatus, List<Job>> ret = new TreeMap<>();

		if (jobs.size() == 0)
			return ret;

		for (final Job j : jobs) {
			List<Job> l = ret.get(j.status());

			if (l == null) {
				l = new ArrayList<>();
				ret.put(j.status(), l);
			}

			l.add(j);
		}

		return ret;
	}

	/**
	 * @param queueId
	 * @return trace log
	 */
	public static String getJobTraceLog(final long queueId) {
		final JobTraceLog trace = new JobTraceLog(queueId);
		return trace.getTraceLog();
	}

	/**
	 * @param job
	 * @param newStatus
	 * @return <code>true</code> if the job status was changed
	 */
	public static boolean setJobStatus(final long job, final JobStatus newStatus) {
		return setJobStatus(job, newStatus, null, null);
	}

	/**
	 * @param job
	 * @param newStatus
	 * @param oldStatusConstraint
	 *            change the status only if the job is still in this state. Can be <code>null</code> to disable checking the current status.
	 * @return <code>true</code> if the job status was changed
	 */
	public static boolean setJobStatus(final long job, final JobStatus newStatus, final JobStatus oldStatusConstraint) {
		return setJobStatus(job, newStatus, oldStatusConstraint, null);
	}

	private static final Set<String> QUEUEID = Set.of("queueId");

	private static ApMon centralMLService = null;

	/**
	 * @param job
	 * @param newStatus
	 * @param oldStatusConstraint
	 *            change the status only if the job is still in this state. Can be <code>null</code> to disable checking the current status.
	 * @param extrafields
	 *            other fields to set at the same time
	 * @return <code>true</code> if the job status was changed
	 */
	public static boolean setJobStatus(final long job, final JobStatus newStatus, final JobStatus oldStatusConstraint, final HashMap<String, Object> extrafields) {
		if (job <= 0)
			throw new IllegalArgumentException("Job ID " + job + " is illegal");

		if (newStatus == null)
			throw new IllegalArgumentException("The new status code cannot be null");

		try (DBFunctions db = getQueueDB()) {
			if (db == null) {
				logger.log(Level.SEVERE, "Cannot get the queue database entry");

				return false;
			}

			String q;

			if (dbStructure2_20)
				q = "SELECT statusId FROM QUEUE where queueId=?;";
			else
				q = "SELECT status FROM QUEUE where queueId=?;";

			db.setReadOnly(true);
			db.setQueryTimeout(600);

			if (!db.query(q, false, Long.valueOf(job))) {
				logger.log(Level.SEVERE, "Error executing the select query from QUEUE");

				return false;
			}

			if (!db.moveNext()) {
				logger.log(Level.WARNING, "Could not find queueId " + job + " in the queue");

				return false;
			}

			db.setReadOnly(false);

			JobStatus oldStatus;

			if (dbStructure2_20)
				oldStatus = JobStatus.getStatusByAlien(Integer.valueOf(db.geti(1)));
			else
				oldStatus = JobStatus.getStatus(db.gets(1));

			if (oldStatus == null) {
				logger.log(Level.WARNING, "Cannot get the status string from " + db.gets(1));
				return false;
			}

			if (oldStatusConstraint != null && oldStatus != oldStatusConstraint) {
				if (logger.isLoggable(Level.FINE))
					logger.log(Level.FINE,
							"Refusing to do the update of " + job + " to state " + newStatus.name() + " because old status is not " + oldStatusConstraint.name() + " but " + oldStatus.name());

				return false;
			}

			Object newstatus;

			if (dbStructure2_20) {
				String extra = "";
				if (newStatus == JobStatus.RUNNING)
					extra = ",started=UNIX_TIMESTAMP()";

				newstatus = Integer.valueOf(newStatus.getAliEnLevel());
				q = "UPDATE QUEUE SET statusId=?" + extra + " WHERE queueId=?;";
			}
			else {
				newstatus = newStatus.toSQL();
				q = "UPDATE QUEUE SET status=? WHERE queueId=?;";
			}

			db.setQueryTimeout(600);

			if (!db.query(q, false, newstatus, Long.valueOf(job)))
				return false;

			final boolean updated = db.getUpdateCount() != 0;

			putJobLog(job, "state", "Job state transition from " + oldStatus.name() + " to " + newStatus.name(), null);

			if (JobStatus.finalStates().contains(newStatus) || newStatus == JobStatus.SAVED_WARN || newStatus == JobStatus.SAVED)
				deleteJobToken(job);

			String execHost = "NO_SITE";

			if (extrafields != null) {
				if (logger.isLoggable(Level.FINE))
					logger.log(Level.FINE, "extrafields: " + extrafields.toString());

				for (final Map.Entry<String, Object> entry : extrafields.entrySet()) {
					final String key = entry.getKey();
					final Object value = entry.getValue();

					if (fieldMap.containsKey(entry.getKey() + "_table")) {
						final HashMap<String, Object> map = new HashMap<>();

						int hostId;
						if (key.contains("node") || key.contains("exechost")) {
							hostId = TaskQueueUtils.getOrInsertFromLookupTable("host", value.toString());
							map.put(fieldMap.get(key + "_field"), Integer.valueOf(hostId));
						}
						else
							map.put(fieldMap.get(key + "_field"), value);

						map.put("queueId", Long.valueOf(job));

						final String query = DBFunctions.composeUpdate(fieldMap.get(key + "_table"), map, QUEUEID);
						db.query(query);
					}
				}

				execHost = extrafields.getOrDefault("exechost", execHost).toString();
			}

			// lazy initialization of the ML service collecting job transitions
			synchronized (fieldMap) {
				if (centralMLService == null) {
					final Vector<String> targets = new Vector<>();
					try {
						targets.add(ConfigUtils.getConfig().gets("CS_ApMon", "aliendb4.cern.ch"));
						centralMLService = new ApMon(targets);
					}
					catch (final Exception e) {
						logger.log(Level.WARNING, "Could not initialize apmon (" + targets + ")", e);
					}
				}
			}

			// send status change to ML
			if (centralMLService != null) {
				final Vector<String> parameters = new Vector<>();
				final Vector<Object> values = new Vector<>();

				parameters.add("jobID");
				values.add(Double.valueOf(job));

				parameters.add("statusID");
				values.add(Integer.valueOf(newStatus.getAliEnLevel()));

				try {
					centralMLService.sendParameters("TaskQueue_Jobs_ALICE", execHost, parameters.size(), parameters, values);
				}
				catch (final Exception e) {
					logger.log(Level.WARNING, "Failed to send job status update to central ML", e);
				}
			}

			return updated;
		}
	}

	/**
	 * @param queueId
	 * @return the JDL of this subjobID, if known, <code>null</code> otherwise
	 */
	public static String getJDL(final long queueId) {
		return getJDL(queueId, true);
	}

	/**
	 * @param queueId
	 * @param originalJDL
	 *            if <code>true</code> then the original JDL will be returned, otherwise the processed JDL. Only possible for AliEn v2.20+, for older versions the only known JDL is returned.
	 * @return the JDL of this subjobID, if known, <code>null</code> otherwise
	 */
	@SuppressWarnings("deprecation")
	public static String getJDL(final long queueId, final boolean originalJDL) {
		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return null;

			db.setQueryTimeout(120);

			if (monitor != null) {
				monitor.incrementCounter("TQ_db_lookup");
				monitor.incrementCounter("TQ_get_jdl");
			}

			String q;

			if (dbStructure2_20)
				q = "SELECT origJdl" + (originalJDL ? "" : ",resultsJdl") + " FROM QUEUEJDL WHERE queueId=?;";
			else
				q = "SELECT jdl FROM QUEUE WHERE queueId=?;";

			db.setReadOnly(true);

			if (!db.query(q, false, Long.valueOf(queueId)) || !db.moveNext()) {
				final Date d = new Date();
				q = "SELECT origJdl" + (originalJDL ? "" : ",resultsJdl") + " as JDL FROM QUEUEARCHIVE" + (1900 + d.getYear()) + " WHERE queueId=?";

				if (!db.query(q, false, Long.valueOf(queueId)) || !db.moveNext()) {
					final String jdlArchiveDir = ConfigUtils.getConfig().gets("alien.taskQueue.TaskQueueUtils.jdlArchiveDir");

					if (jdlArchiveDir.length() > 0) {
						File f = new File(jdlArchiveDir, queueId + ".txt");

						if (f.exists() && f.canRead()) {
							String content = Utils.readFile(f.getAbsolutePath());

							final int idx = content.indexOf("// --------");

							if (idx >= 0)
								content = content.substring(0, idx);

							return content;
						}

						f = new File(jdlArchiveDir, queueId + ".html");

						String content = null;

						if (f.exists() && f.canRead())
							content = Utils.readFile(f.getAbsolutePath());
						else {
							f = new File(jdlArchiveDir, (queueId / 10000000) + ".zip");

							if (f.exists() && f.canRead()) {
								final Path zipFile = Paths.get(f.getAbsolutePath());

								try (FileSystem fileSystem = FileSystems.newFileSystem(zipFile, (ClassLoader) null)) {
									final Path source = fileSystem.getPath(queueId + ".html");

									if (source != null) {
										final ByteArrayOutputStream baos = new ByteArrayOutputStream();

										Files.copy(source, baos);

										content = baos.toString();
									}
								}
								catch (@SuppressWarnings("unused") final IOException e) {
									// ignore
								}
							}
						}

						if (content != null) {
							content = Utils.htmlToText(content);

							final int idx = content.indexOf("// --------");

							if (idx >= 0)
								content = content.substring(0, idx);

							return content;
						}
					}

					logger.log(Level.WARNING, "Could not locate the archived jdl of " + queueId);

					return null;
				}
			}

			if (dbStructure2_20 && !originalJDL) {
				final String jdl = db.gets(2);

				if (jdl.length() > 0)
					return jdl;
			}

			return db.gets(1);
		}
	}

	/**
	 * @param states
	 * @param users
	 * @param sites
	 * @param nodes
	 * @param mjobs
	 * @param jobids
	 * @param orderByKey
	 * @param limit
	 * @return the ps listing
	 */
	public static List<Job> getPS(final Collection<JobStatus> states, final Collection<String> users, final Collection<String> sites, final Collection<String> nodes, final Collection<Long> mjobs,
			final Collection<Long> jobids, final String orderByKey, final int limit) {

		final List<Job> ret = new ArrayList<>();

		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return null;

			if (monitor != null)
				monitor.incrementCounter("TQ_db_lookup");

			int lim = 20000;

			if (limit > 0 && limit < 50000)
				lim = limit;

			String where = "";

			if (states != null && states.size() > 0 && !states.contains(JobStatus.ANY)) {
				final StringBuilder whe = new StringBuilder();

				if (dbStructure2_20)
					whe.append(" (statusId in (");
				else
					whe.append(" (status in (");

				boolean first = true;

				for (final JobStatus s : states) {
					if (!first)
						whe.append(",");
					else
						first = false;

					if (dbStructure2_20)
						whe.append(s.getAliEnLevel());
					else
						whe.append('\'').append(s.toSQL()).append('\'');
				}

				if (!first)
					where += whe + ") ) and ";
			}

			if (users != null && users.size() > 0 && !users.contains("%")) {
				final StringBuilder whe = new StringBuilder(" ( ");

				boolean first = true;

				for (final String u : users) {
					if (!first)
						whe.append(" or ");
					else
						first = false;

					if (dbStructure2_20)
						whe.append("userId=").append(getUserId(u));
					else
						whe.append("submitHost like '").append(Format.escSQL(u)).append("@%'");
				}

				if (!first)
					where += whe + " ) and ";
			}

			if (sites != null && sites.size() > 0 && !sites.contains("%")) {
				final Collection<Integer> siteIDs = new HashSet<>();

				for (final String s : sites) {
					final Collection<Integer> ids = getSiteIDs(s);

					if (ids != null)
						siteIDs.addAll(ids);
				}

				if (siteIDs.size() > 0)
					where += " ( siteId in (" + Format.toCommaList(siteIDs) + ") ) and ";
				else
					return ret;
			}

			if (nodes != null && nodes.size() > 0 && !nodes.contains("%")) {
				final StringBuilder whe = new StringBuilder(" ( nodeId in (");

				boolean first = true;

				for (final String n : nodes) {
					final Integer nodeId = getHostId(n);

					if (nodeId != null) {
						if (!first)
							whe.append(',');
						else
							first = false;

						whe.append(nodeId);
					}
				}

				if (!first)
					where += whe + ") ) and ";
			}

			if (mjobs != null && mjobs.size() > 0) {
				if (!mjobs.contains(Long.valueOf(0))) {
					final StringBuilder whe = new StringBuilder(" ( split in (");

					boolean first = true;

					for (final Long m : mjobs) {
						if (!first)
							whe.append(',');
						else
							first = false;

						whe.append(m);
					}

					if (!first)
						where += whe + ") ) and ";
				}
				else {
					where += " ( split = 0 ) and ";
				}
			}

			if (jobids != null && jobids.size() > 0 && !jobids.contains(Long.valueOf(0))) {
				final StringBuilder whe = new StringBuilder(" ( queueId in (");

				boolean first = true;

				for (final Long i : jobids) {
					if (!first)
						whe.append(',');
					else
						first = false;

					whe.append(i);
				}

				if (!first)
					where += whe + ") ) and ";
			}

			if (where.endsWith(" and "))
				where = where.substring(0, where.length() - 5);

			String orderBy = " order by ";

			if (orderByKey == null || orderByKey.length() == 0)
				orderBy += "queueId";
			else
				orderBy += "`" + orderByKey + "`";

			if (where.length() > 0)
				where = " WHERE " + where;

			final String q;

			if (dbStructure2_20)
				q = "SELECT * FROM (SELECT * FROM QUEUE " + where + " ORDER BY queueId DESC " + " limit " + lim + ") x " + orderBy;
			else
				q = "SELECT " + ALL_BUT_JDL + " FROM QUEUE " + where + orderBy + " limit " + lim + ";";

			if (logger.isLoggable(Level.FINE))
				logger.log(Level.FINE, "selection query: " + q);

			db.setReadOnly(true);
			db.setQueryTimeout(600);

			if (!db.query(q))
				return null;

			while (db.moveNext()) {
				final Job j = new Job(db, false);
				ret.add(j);
			}
		}

		return ret;

	}

	/**
	 * @return matching jobs histograms
	 */
	public static Map<Integer, Integer> getMatchingHistogram() {
		final Map<Integer, Integer> ret = new TreeMap<>();

		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return ret;

			final Map<Integer, AtomicInteger> work = new HashMap<>();

			if (monitor != null) {
				monitor.incrementCounter("TQ_db_lookup");
				monitor.incrementCounter("TQ_matching_histogram");
			}

			db.setReadOnly(true);
			db.setQueryTimeout(60);

			db.query("select site,sum(counter) from JOBAGENT where counter>0 group by site");

			while (db.moveNext()) {
				final String site = db.gets(1);

				int key = 0;

				final StringTokenizer st = new StringTokenizer(site, ",; ");

				while (st.hasMoreTokens())
					if (st.nextToken().length() > 0)
						key++;

				final int value = db.geti(2);

				final Integer iKey = Integer.valueOf(key);

				final AtomicInteger ai = work.get(iKey);

				if (ai == null)
					work.put(iKey, new AtomicInteger(value));
				else
					ai.addAndGet(value);
			}

			for (final Map.Entry<Integer, AtomicInteger> entry : work.entrySet())
				ret.put(entry.getKey(), Integer.valueOf(entry.getValue().intValue()));

			return ret;
		}
	}

	/**
	 * @return the number of matching waiting jobs for each site
	 */
	public static Map<String, Integer> getMatchingJobsSummary() {
		final Map<String, Integer> ret = new TreeMap<>();

		int addToAll = 0;

		final Map<String, AtomicInteger> work = new HashMap<>();

		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return ret;

			db.setReadOnly(true);
			db.setQueryTimeout(60);

			if (monitor != null) {
				monitor.incrementCounter("TQ_db_lookup");
				monitor.incrementCounter("TQ_matching_jobs_summary");
			}

			db.query("select site,sum(counter) from JOBAGENT where counter>0 group by site;");

			while (db.moveNext()) {
				final String sites = db.gets(1);
				final int count = db.geti(2);

				if (sites.length() == 0)
					addToAll = count;
				else {
					final StringTokenizer st = new StringTokenizer(sites, ",; ");

					while (st.hasMoreTokens()) {
						final String site = st.nextToken().trim();

						final AtomicInteger ai = work.get(site);

						if (ai == null)
							work.put(site, new AtomicInteger(count));
						else
							ai.addAndGet(count);
					}
				}
			}
		}

		for (final Map.Entry<String, AtomicInteger> entry : work.entrySet())
			ret.put(entry.getKey(), Integer.valueOf(entry.getValue().intValue() + addToAll));

		if (addToAll > 0) {
			final Set<String> sites = LDAPHelper.checkLdapInformation("(objectClass=organizationalUnit)", "ou=Sites,", "ou", false);

			if (sites != null)
				for (final String site : sites)
					if (!ret.containsKey(site))
						ret.put(site, Integer.valueOf(addToAll));
		}

		return ret;
	}

	private static final Pattern p = Pattern.compile("\\$(\\d+)");

	/**
	 * @param jdlArguments
	 * @return array of arguments to apply to the JDL
	 * @see #applyJDLArguments(String, String...)
	 * @see #submit(LFN, AliEnPrincipal, String...)
	 */
	public static String[] splitArguments(final String jdlArguments) {
		final StringTokenizer st = new StringTokenizer(jdlArguments);

		final List<String> split = new LinkedList<>();

		while (st.hasMoreTokens()) {
			final String tok = st.nextToken();

			if (tok.length() > 0)
				split.add(tok);
		}

		return split.toArray(new String[0]);
	}

	/**
	 * @param jdlContents
	 *            JDL specification
	 * @param arguments
	 *            arguments to the JDL, should be at least as many as the largest $N that shows up in the JDL
	 * @return the parsed JDL, with all $N parameters replaced with the respective argument
	 * @throws IOException
	 *             if there is any problem parsing the JDL content
	 */
	public static JDL applyJDLArguments(final String jdlContents, final String... arguments) throws IOException {
		if (jdlContents == null)
			return null;

		String jdlToSubmit = JDL.removeComments(jdlContents);

		Matcher m = p.matcher(jdlToSubmit);

		while (m.find()) {
			final String s = m.group(1);

			final int i = Integer.parseInt(s);

			if (arguments == null || arguments.length < i)
				throw new IOException("The JDL indicates argument $" + i + " but you haven't provided it");

			final String processedArgs = Format.replace(arguments[i - 1], "$", "\\$");

			jdlToSubmit = jdlToSubmit.replaceAll("\\$" + i + "(?!\\d)", processedArgs);

			m = p.matcher(jdlToSubmit);
		}

		final JDL jdl = new JDL(jdlToSubmit);

		if (arguments != null && arguments.length > 0)
			jdl.set("JDLArguments", String.join(" ", arguments));

		return jdl;
	}

	/**
	 * Check the Executable and ValidationCommand paths in the catalogue and expand the relative ones to the first file found in user's own folders, including the same directory from where the JDL was
	 * submitted
	 *
	 * @param jdl
	 *            JDL to check
	 * @param account
	 *            account that runs it
	 * @param role
	 *            the role (can be <code>null</code>)
	 * @throws IOException
	 *             if some file cannot be located
	 */
	private static final void expandExecutables(final JDL jdl, final AliEnPrincipal account) throws IOException {
		final Set<String> pathsToCheck = new LinkedHashSet<>();

		pathsToCheck.add("/bin/");
		pathsToCheck.add("/alice/bin/");
		pathsToCheck.add("/panda/bin/");

		pathsToCheck.add(UsersHelper.getHomeDir(account.getName()) + "bin/");

		final String jdlPath = jdl.gets("JDLPath");

		if (jdlPath != null && jdlPath.length() > 0 && jdlPath.indexOf('/') >= 0)
			pathsToCheck.add(jdlPath.substring(0, jdlPath.lastIndexOf('/') + 1));

		for (final String jdlTag : new String[] { "Executable", "ValidationCommand" }) {
			final String executable = jdl.gets(jdlTag);

			if (executable == null) {
				if ("Executable".equals(jdlTag))
					throw new IOException("The JDL has to indicate an Executable");

				continue;
			}

			boolean found = false;

			try {
				final List<String> options = new LinkedList<>();

				if (!executable.startsWith("/"))
					for (final String path : pathsToCheck)
						options.add(path + executable);
				else
					options.add(executable);

				final LFNfromString answer = Dispatcher.execute(new LFNfromString(account, true, false, options));

				final List<LFN> lfns = answer.getLFNs();

				if (lfns != null && !lfns.isEmpty())
					for (final LFN l : lfns)
						if (l.isFile()) {
							found = true;
							jdl.set(jdlTag, l.getCanonicalName());
						}
			}
			catch (final ServerException se) {
				throw new IOException(se.getMessage(), se);
			}

			if (!found)
				throw new IOException("The " + jdlTag + " name you indicated (" + executable + ") cannot be located in any standard PATH");
		}
	}

	/**
	 * Submit the JDL indicated by this file
	 *
	 * @param file
	 *            the catalogue name of the JDL to be submitted
	 * @param account
	 *            account from where the submit command was received
	 * @param arguments
	 *            arguments to the JDL, should be at least as many as the largest $N that shows up in the JDL
	 * @return the job ID
	 * @throws IOException
	 *             in case of problems like downloading the respective JDL or not enough arguments provided to it
	 */
	public static long submit(final LFN file, final AliEnPrincipal account, final String... arguments) throws IOException {
		if (file == null || !file.exists || !file.isFile())
			throw new IllegalArgumentException("The LFN is not a valid file");

		final String jdlContents = IOUtils.getContents(file);

		if (jdlContents == null || jdlContents.length() == 0)
			throw new IOException("Could not download " + file.getCanonicalName());

		final JDL jdl = applyJDLArguments(jdlContents, arguments);

		jdl.set("JDLPath", file.getCanonicalName());

		return submit(jdl, account);
	}

	private static void prepareJDLForSubmission(final JDL jdl, final AliEnPrincipal account) throws IOException {
		expandExecutables(jdl, account);

		Float price = jdl.getFloat("Price");

		if (price == null)
			price = Float.valueOf(1);

		jdl.set("Price", price);

		Integer ttl = jdl.getInteger("TTL");

		if (ttl == null || ttl.intValue() <= 0)
			ttl = Integer.valueOf(21600);

		jdl.set("TTL", ttl);

		jdl.set("Type", "Job");

		if (jdl.get("OrigRequirements") == null)
			jdl.set("OrigRequirements", jdl.get("Requirements"));

		if (jdl.get("MemorySize") == null)
			jdl.set("MemorySize", "8GB");

		if (jdl.getInteger("CPUCores") == null)
			jdl.set("CPUCores", "1");

		jdl.append("JDLVariables", "CPUCores");

		// remove any JDLVariable that doesn't have an associated value at this point
		final Object o = jdl.get("JDLVariables");

		if (o instanceof Collection) {
			final Collection<?> values = (Collection<?>) o;

			final Iterator<?> it = values.iterator();

			while (it.hasNext()) {
				final String variable = it.next().toString();

				if (jdl.get(variable) == null)
					it.remove();
			}
		}

		jdl.set("User", account.getName());

		// set the requirements anew
		jdl.delete("Requirements");

		jdl.addRequirement("other.Type == \"machine\"");

		final Collection<String> packages = jdl.getList("Packages");

		if (packages != null)
			for (final String pack : packages)
				jdl.addRequirement("member(other.Packages,\"" + pack + "\")");

		jdl.addRequirement(jdl.gets("OrigRequirements"));

		jdl.addRequirement("other.TTL > " + ttl);
		jdl.addRequirement("other.Price <= " + price.intValue());

		final Collection<String> inputFiles = jdl.getList("InputFile");

		if (inputFiles != null)
			for (final String file : inputFiles) {
				if (file.indexOf('/') < 0)
					throw new IOException("InputFile contains an illegal entry: " + file);

				String lfn = file;

				if (lfn.startsWith("LF:"))
					lfn = lfn.substring(3);
				else
					throw new IOException("InputFile doesn't start with 'LF:' : " + lfn);

				final LFN l = LFNUtils.getLFN(lfn);

				if (l == null || !l.isFile())
					throw new IOException("InputFile " + lfn + " doesn't exist in the catalogue");
			}

		final Collection<String> inputData = jdl.getList("InputData");

		if (inputData != null)
			for (final String file : inputData) {
				if (file.indexOf('/') < 0)
					throw new IOException("InputData contains an illegal entry: " + file);

				String lfn = file;

				if (lfn.startsWith("LF:"))
					lfn = lfn.substring(3);
				else
					throw new IOException("InputData doesn't start with 'LF:' : " + lfn);

				if (lfn.indexOf(',') >= 0)
					lfn = lfn.substring(0, lfn.indexOf(',')); // "...,nodownload"
																// for example

				final LFN l = LFNUtils.getLFN(lfn);

				if (l == null || !l.isFile())
					throw new IOException("InputData " + lfn + " doesn't exist in the catalogue");
			}

		// sanity check of other tags

		for (final String tag : Arrays.asList("ValidationCommand", "InputDataCollection")) {
			final Collection<String> files = jdl.getList(tag);

			if (files == null)
				continue;

			for (final String file : files) {
				String fileName = file;

				if (fileName.startsWith("LF:"))
					fileName = fileName.substring(3);

				if (fileName.indexOf(',') >= 0)
					fileName = fileName.substring(0, fileName.indexOf(','));

				final LFN l = LFNUtils.getLFN(fileName);

				if (l == null || (!l.isFile() && !l.isCollection()))
					throw new IOException(tag + " tag required " + fileName + " which is not valid: " + (l == null ? "not in the catalogue" : "not a file or collection"));
			}
		}
	}

	/**
	 * Submit this JDL body
	 *
	 * @param j
	 *            job description, in plain text
	 * @param account
	 *            account from where the submit command was received
	 * @return the job ID
	 * @throws IOException
	 *             in case of problems such as the number of provided arguments is not enough
	 * @see #applyJDLArguments(String, String...)
	 */
	public static long submit(final JDL j, final AliEnPrincipal account) throws IOException {
		final String owner = prepareSubmission(j, account);

		final FileQuota quota = QuotaUtilities.getFileQuota(owner);

		if (quota != null && !quota.canUpload(1, 1))
			throw new IOException("User " + owner + " has exceeded the file quota and is not allowed to write any more files");

		return insertJob(j, account, owner, null);
	}

	/**
	 * Check the validity of the JDL (package versions, existing critical input files etc) and if the indicated account has access to the indicated role. Will also decorate the JDL with various helper
	 * tags.
	 *
	 * @param j
	 *            JDL to submit
	 * @param account
	 *            AliEn account that requests the submission
	 * @return the AliEn account name that will own the job
	 * @throws IOException
	 */
	public static String prepareSubmission(final JDL j, final AliEnPrincipal account) throws IOException {
		// TODO : check this account's quota before submitting

		final String packageMessage = PackageUtils.checkPackageRequirements(j);

		if (packageMessage != null)
			throw new IOException(packageMessage);

		prepareJDLForSubmission(j, account);

		return account.getName();
	}

	/**
	 * Insert a job in the given status. N
	 *
	 * @param j
	 *            full JDL
	 * @param account
	 *            AliEn account
	 * @param owner
	 *            AliEn account name that the indicated account has access to
	 * @param targetStatus
	 *            job status. Can be <code>null</code> to have the default behavior of putting it to <code>INSERTING</code> and letting AliEn process it.
	 * @return the just inserted job ID
	 * @throws IOException
	 */
	public static long insertJob(final JDL j, final AliEnPrincipal account, final String owner, final JobStatus targetStatus) throws IOException {
		final String clientAddress;

		final InetAddress addr = account.getRemoteEndpoint();

		if (addr != null)
			clientAddress = Utils.getHostName(addr.getHostAddress());
		else
			clientAddress = ConfigUtils.getLocalHostname();

		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				throw new IOException("This service has no direct database connection");

			db.setQueryTimeout(300);

			final Map<String, Object> values = new HashMap<>();

			final String executable = j.getExecutable();

			final Float price = j.getFloat("Price");

			values.put("priority", Integer.valueOf(0));

			final String notify = j.gets("email");

			final JobStatus jobStatus = targetStatus != null ? targetStatus : JobStatus.INSERTING;

			if (dbStructure2_20) {
				values.put("statusId", Integer.valueOf(jobStatus.getAliEnLevel()));
				values.put("userId", getUserId(owner));
				values.put("submitHostId", getHostId(clientAddress));
				values.put("commandId", getCommandId(executable));

				if (notify != null && notify.length() > 0)
					values.put("notifyId", getNotifyId(notify));
			}
			else {
				values.put("status", jobStatus.toSQL());
				values.put("jdl", "\n    [\n" + j.toString() + "\n    ]");
				values.put("submitHost", owner + "@" + clientAddress);
				values.put("notify", notify);
				values.put("name", executable);
			}

			values.put("chargeStatus", Integer.valueOf(0));
			values.put("price", price);
			values.put("received", Long.valueOf(System.currentTimeMillis() / 1000));

			Long masterjobID = j.getLong("MasterJobID");

			if (JobStatus.SPLIT.equals(jobStatus) || j.get("Split") != null) {
				values.put("masterjob", Integer.valueOf(1));
				masterjobID = null;
			}
			else
				values.put("masterjob", Integer.valueOf(0));

			if (masterjobID != null)
				values.put("split", masterjobID);
			else
				values.put("split", Integer.valueOf(0));

			final Integer cpuCores = j.getInteger("CPUCores");

			values.put("cpucores", cpuCores == null || cpuCores.intValue() < 0 || cpuCores.intValue() > 100 ? Integer.valueOf(1) : cpuCores);

			final String insert = DBFunctions.composeInsert("QUEUE", values);

			db.setLastGeneratedKey(true);

			if (!db.query(insert))
				throw new IOException("Could not insert the job in the queue");

			final Long pid = db.getLastGeneratedKeyLong();

			if (pid == null)
				throw new IOException("Last generated key is unknown");

			db.query("INSERT INTO QUEUEPROC (queueId) VALUES (?);", false, pid);

			if (dbStructure2_20) {
				final Map<String, Object> valuesJDL = new HashMap<>();

				valuesJDL.put("queueId", pid);
				valuesJDL.put("origJdl", "\n    [\n" + j.toString() + "\n    ]");

				final String insertJDL = DBFunctions.composeInsert("QUEUEJDL", valuesJDL);

				db.query(insertJDL);
			}

			setAction(jobStatus);

			putJobLog(pid.longValue(), "state", "Job state transition to " + jobStatus.toString(), null);

			return pid.longValue();
		}
	}

	private static final GenericLastValuesCache<String, Integer> userIdCache = new GenericLastValuesCache<>() {
		private static final long serialVersionUID = 1L;

		@Override
		protected boolean cacheNulls() {
			return false;
		}

		@Override
		protected Integer resolve(final String key) {
			try (DBFunctions db = getQueueDB()) {
				if (db == null)
					return null;

				db.setReadOnly(true);

				db.query("SELECT userId FROM QUEUE_USER where user=?;", false, key);

				if (!db.moveNext()) {
					db.setReadOnly(false);

					db.setLastGeneratedKey(true);

					final Set<String> ids = LDAPHelper.checkLdapInformation("uid=" + key, "ou=People,", "CCID");

					int id = 0;

					if (ids != null && ids.size() > 0)
						for (final String s : ids)
							try {
								id = Integer.parseInt(s);
								break;
							}
							catch (@SuppressWarnings("unused") final Throwable t) {
								// ignore
							}

					if (id > 0) {
						if (db.query("INSERT INTO QUEUE_USER (userId, user) VALUES (?, ?);", true, Integer.valueOf(id), key))
							return Integer.valueOf(id);

						// did it fail because the user was inserted by somebody
						// else?
						db.query("SELECT userId FROM QUEUE_USER where user=?;", false, key);

						if (db.moveNext())
							return Integer.valueOf(db.geti(1));

						// if it gets here it means there is a duplicate CCID in
						// LDAP

						logger.log(Level.WARNING, "Duplicate CCID " + id + " in LDAP, failed to correctly insert user " + key
								+ " because of it. Will generate a new userid for this guy, but the consistency with LDAP is lost now!");
					}

					if (db.query("INSERT INTO QUEUE_USER (user) VALUES (?);", true, key))
						return db.getLastGeneratedKey();

					// somebody probably has inserted the same entry concurrently
					db.query("SELECT userId FROM QUEUE_USER where user=?", false, key);

					if (db.moveNext())
						return Integer.valueOf(db.geti(1));
				}
				else
					return Integer.valueOf(db.geti(1));

				return null;
			}
		}
	};

	/**
	 * Get the user ID for a given owner string
	 *
	 * @param owner
	 * @return user ID
	 */
	static synchronized Integer getUserId(final String owner) {
		if (owner == null || owner.length() == 0)
			return null;

		return userIdCache.get(owner);
	}

	private static final GenericLastValuesCache<String, Integer> commandIdCache = new GenericLastValuesCache<>() {
		private static final long serialVersionUID = 1L;

		@Override
		protected boolean cacheNulls() {
			return false;
		}

		@Override
		protected Integer resolve(final String key) {
			try (DBFunctions db = getQueueDB()) {
				if (db == null)
					return null;

				db.setReadOnly(true);

				db.query("SELECT commandId FROM QUEUE_COMMAND where command=?;", false, key);

				db.setReadOnly(false);

				if (!db.moveNext()) {
					db.setLastGeneratedKey(true);

					if (db.query("INSERT INTO QUEUE_COMMAND (command) VALUES (?);", true, key))
						return db.getLastGeneratedKey();

					// somebody probably has inserted the same entry
					// concurrently
					db.query("SELECT commandId FROM QUEUE_COMMAND where command=?;", false, key);

					if (db.moveNext())
						return Integer.valueOf(db.geti(1));
				}
				else
					return Integer.valueOf(db.geti(1));
			}

			return null;
		}
	};

	private static synchronized Integer getCommandId(final String command) {
		if (command == null || command.length() == 0)
			return null;

		return commandIdCache.get(command);
	}

	private static final GenericLastValuesCache<String, Integer> hostIdCache = new GenericLastValuesCache<>() {
		private static final long serialVersionUID = 1L;

		@Override
		protected boolean cacheNulls() {
			return false;
		}

		@Override
		protected Integer resolve(final String key) {
			try (DBFunctions db = getQueueDB()) {
				if (db == null)
					return null;

				db.setReadOnly(true);

				db.query("SELECT hostId FROM QUEUE_HOST where host=?;", false, key);

				db.setReadOnly(false);

				if (!db.moveNext()) {
					db.setLastGeneratedKey(true);

					if (db.query("INSERT INTO QUEUE_HOST (host) VALUES (?);", true, key))
						return db.getLastGeneratedKey();

					// somebody probably has inserted the same entry
					// concurrently
					db.query("SELECT hostId FROM QUEUE_HOST where host=?", false, key);

					if (db.moveNext())
						return Integer.valueOf(db.geti(1));
				}
				else
					return Integer.valueOf(db.geti(1));
			}

			return null;
		}
	};

	private static synchronized Integer getHostId(final String host) {
		if (host == null || host.length() == 0)
			return null;

		return hostIdCache.get(host);
	}

	private static final GenericLastValuesCache<String, Integer> notifyIdCache = new GenericLastValuesCache<>() {
		private static final long serialVersionUID = 1L;

		@Override
		protected boolean cacheNulls() {
			return false;
		}

		@Override
		protected Integer resolve(final String key) {
			try (DBFunctions db = getQueueDB()) {
				if (db == null)
					return null;

				db.setReadOnly(true);

				db.query("SELECT notifyId FROM QUEUE_NOTIFY where notify=?;", false, key);

				db.setReadOnly(false);

				if (!db.moveNext()) {
					db.setLastGeneratedKey(true);

					if (db.query("INSERT INTO QUEUE_NOTIFY (notify) VALUES (?);", true, key))
						return db.getLastGeneratedKey();

					// somebody probably has inserted the same entry
					// concurrently
					db.query("SELECT notifyId FROM QUEUE_NOTIFY where notify=?;", false, key);

					if (db.moveNext())
						return Integer.valueOf(db.geti(1));
				}
				else
					return Integer.valueOf(db.geti(1));
			}

			return null;
		}
	};

	private static synchronized Integer getNotifyId(final String notify) {
		if (notify == null || notify.length() == 0)
			return null;

		return notifyIdCache.get(notify);
	}

	private static final GenericLastValuesCache<Integer, String> userCache = new GenericLastValuesCache<>() {
		private static final long serialVersionUID = 1L;

		@Override
		protected boolean cacheNulls() {
			return false;
		}

		@Override
		protected String resolve(final Integer key) {
			try (DBFunctions db = getQueueDB()) {
				if (db == null)
					return null;

				db.setReadOnly(true);

				db.query("SELECT user FROM QUEUE_USER where userId=?;", false, key);

				if (db.moveNext())
					return StringFactory.get(db.gets(1));

				return null;
			}
		}
	};

	/**
	 * @param userId
	 * @return user name for the respective userId
	 */
	public static String getUser(final int userId) {
		if (userId <= 0)
			return null;

		return userCache.get(Integer.valueOf(userId));
	}

	private static final GenericLastValuesCache<Integer, String> hostCache = new GenericLastValuesCache<>() {
		private static final long serialVersionUID = 1L;

		@Override
		protected int getMaximumSize() {
			return 20000;
		}

		@Override
		protected boolean cacheNulls() {
			return false;
		}

		@Override
		protected String resolve(final Integer key) {
			try (DBFunctions db = getQueueDB()) {
				if (db == null)
					return null;

				db.setReadOnly(true);

				db.query("SELECT host FROM QUEUE_HOST where hostId=?;", false, key);

				if (db.moveNext())
					return StringFactory.get(db.gets(1));
			}

			return null;
		}
	};

	/**
	 * @param hostId
	 * @return host name for the respective hostId
	 */
	public static String getHost(final int hostId) {
		if (hostId <= 0)
			return null;

		return hostCache.get(Integer.valueOf(hostId));
	}

	private static final GenericLastValuesCache<Integer, String> notifyCache = new GenericLastValuesCache<>() {
		private static final long serialVersionUID = 1L;

		@Override
		protected boolean cacheNulls() {
			return false;
		}

		@Override
		protected String resolve(final Integer key) {
			try (DBFunctions db = getQueueDB()) {
				if (db == null)
					return null;

				db.setReadOnly(true);

				db.query("SELECT notify FROM QUEUE_NOTIFY where notifyId=?;", false, key);

				if (db.moveNext())
					return StringFactory.get(db.gets(1));
			}

			return null;
		}
	};

	/**
	 * @param notifyId
	 * @return notification string (email address) for the respective notifyId
	 */
	public static String getNotify(final int notifyId) {
		if (notifyId <= 0)
			return null;

		return notifyCache.get(Integer.valueOf(notifyId));
	}

	private static final GenericLastValuesCache<Integer, String> commandCache = new GenericLastValuesCache<>() {
		private static final long serialVersionUID = 1L;

		@Override
		protected boolean cacheNulls() {
			return false;
		}

		@Override
		protected String resolve(final Integer key) {
			try (DBFunctions db = getQueueDB()) {
				if (db == null)
					return null;

				db.setReadOnly(true);
				db.setQueryTimeout(30);

				db.query("SELECT command FROM QUEUE_COMMAND where commandId=?", false, key);

				if (db.moveNext())
					return StringFactory.get(db.gets(1));
			}

			return null;
		}
	};

	/**
	 * @param commandId
	 * @return the command corresponding to the id
	 */
	public static String getCommand(final int commandId) {
		if (commandId <= 0)
			return null;

		return commandCache.get(Integer.valueOf(commandId));
	}

	/**
	 * @param user
	 * @param queueId
	 * @return state of the kill operation
	 */
	public static boolean killJob(final AliEnPrincipal user, final long queueId) {
		final Job j = TaskQueueUtils.getJob(queueId);

		if (j == null)
			return false;

		return killJob(user, j);
	}

	/**
	 * @param user
	 * @param j
	 * @return state of the kill operation
	 */
	public static boolean killJob(final AliEnPrincipal user, final Job j) {
		if (AuthorizationChecker.canModifyJob(j, user)) {
			System.out.println("Authorized job kill for [" + j.queueId + "] by user/role [" + user.getName() + "].");

			return killJob(j);
		}

		System.out.println("Job kill authorization failed for [" + j.queueId + "] by user/role [" + user.getName() + "].");
		return false;
	}

	private static boolean killJob(final Job j) {
		if (setJobStatus(j, JobStatus.KILLED, null, null, null, null)) {
			if (j.execHost != null) {
				// my ($port) =
				// $self->{DB}->getFieldFromHosts($data->{exechost}, "hostport")
				// or
				// $self->info("Unable to fetch hostport for host $data->{exechost}")
				// and return (-1,
				// "unable to fetch hostport for host $data->{exechost}");
				//
				// $DEBUG and $self->debug(1,
				// "Sending a signal to $data->{exechost} $port to kill the process... ");

				String target = j.execHost;

				final int idx = target.indexOf('@');

				if (idx >= 0)
					target = target.substring(idx + 1);

				final int expires = (int) (System.currentTimeMillis() / 1000) + 300;

				insertMessage(j.queueId, target, "ClusterMonitor", "killProcess", String.valueOf(j.queueId), expires);
			}

			// The removal has to be done properly, in Perl it was just the
			// default !/alien-job directory
			// $self->{CATALOGUE}->execute("rmdir", $procDir, "-r")

			if (j.isMaster()) {
				final List<Job> subjobs = TaskQueueUtils.getSubjobs(j.queueId);

				if (subjobs != null) {
					for (final Job subjob : subjobs)
						killJob(subjob);
				}
			}

			return true;
		}

		return false;
	}

	// status and jdl
	private static boolean updateJob(final Job j, final JobStatus newStatus) {
		if (newStatus.smallerThanEquals(j.status()) && (j.status() == JobStatus.ZOMBIE || j.status() == JobStatus.IDLE || j.status() == JobStatus.INTERACTIV) && j.isMaster())
			return false;

		if (j.status() == JobStatus.WAITING && j.agentid > 0)
			if (!deleteJobAgent(j.agentid))
				logger.log(Level.WARNING, "Error killing jobAgent: [" + j.agentid + "].");

		if (j.notify != null && !"".equals(j.notify))
			sendNotificationMail(j);

		if (j.split != 0)
			setSubJobMerges(j);

		if (j.status() != newStatus) {
			try (DBFunctions db = getQueueDB()) {
				if (!db.query("UPDATE QUEUE SET statusId=? WHERE queueId=? and statusId=?;", false, Integer.valueOf(newStatus.getAliEnLevel()), Long.valueOf(j.queueId),
						Integer.valueOf(j.status().getAliEnLevel())))
					return false;

				if (db.getUpdateCount() > 0) {
					// job stats was updated correctly, let's update the queue table too

					if (j.site != null) {
						final int siteId = getSiteId(j.site);

						if (siteId > 0)
							db.query(
									"UPDATE SITEQUEUES SET " + j.status().toSQL() + "=GREATEST(" + j.status().toSQL() + "-1,0), " + newStatus.toSQL() + "=GREATEST(" + newStatus
											+ ",0)+1 WHERE siteId=?",
									false, Integer.valueOf(siteId));
					}
				}
				else
					return false;
			}
		}

		if (newStatus == JobStatus.KILLED || newStatus == JobStatus.SAVED || newStatus == JobStatus.SAVED_WARN || newStatus == JobStatus.STAGING)
			setAction(newStatus);

		// if the state already was the desired one, return <code>true</code>
		return true;
	}

	/**
	 * @param j
	 * @param newStatus
	 * @param arg
	 * @param site
	 * @param spyurl
	 * @param node
	 * @return <code>true</code> if the status was successfully changed
	 */
	public static boolean setJobStatus(final Job j, final JobStatus newStatus, final String arg, final String site, final String spyurl, final String node) {
		final String time = String.valueOf(System.currentTimeMillis() / 1000);

		final HashMap<String, String> jdltags = new HashMap<>();

		jdltags.put("procinfotime", time);

		if (spyurl != null)
			jdltags.put("spyurl", spyurl);
		if (site != null)
			jdltags.put("site", site);
		if (node != null)
			jdltags.put("node", node);

		if (newStatus == JobStatus.WAITING)
			jdltags.put("exechost", arg);
		else if (newStatus == JobStatus.RUNNING)
			jdltags.put("started", time);
		else if (newStatus == JobStatus.STARTED) {
			jdltags.put("started", time);
			jdltags.put("batchid", arg);
		}
		else if (newStatus == JobStatus.SAVING)
			jdltags.put("error", arg);
		else if ((newStatus == JobStatus.SAVED && arg != null && !"".equals(arg)) || newStatus == JobStatus.ERROR_V || newStatus == JobStatus.STAGING)
			jdltags.put("jdl", arg);
		else if (newStatus == JobStatus.DONE || newStatus == JobStatus.DONE_WARN)
			jdltags.put("finished", time);
		else if (JobStatus.finalStates().contains(newStatus) || newStatus == JobStatus.SAVED_WARN || newStatus == JobStatus.SAVED) {
			jdltags.put("spyurl", "");
			jdltags.put("finished", time);
			deleteJobToken(j.queueId);
		}

		// put the JobLog message

		String message = "Job state transition from " + j.getStatusName() + " to " + newStatus;

		final boolean success = updateJob(j, newStatus);

		if (!success)
			message = "FAILED: " + message;

		putJobLog(j.queueId, "state", message, jdltags);

		if (site != null) {
			// # lock queues with submission errors ....
			// if ($status eq "ERROR_S") {
			// $self->_setSiteQueueBlocked($site)
			// or $self->{LOGGER}->error("JobManager",
			// "In changeStatusCommand cannot block site $site for ERROR_S");
			// } elsif ($status eq "ASSIGNED") {
			// my $sitestat = $self->getSiteQueueStatistics($site);
			// if (@$sitestat) {
			// if (@$sitestat[0]->{'ASSIGNED'} > 5) {
			// $self->_setSiteQueueBlocked($site)
			// or $self->{LOGGER}->error("JobManager",
			// "In changeStatusCommand cannot block site $site for ERROR_S");
			// }
			// }
			// }
		}
		return success;
	}

	private static boolean insertMessage(final long queueId, final String target, final String service, final String message, final String messageArgs, final int expires) {
		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return false;

			final String q = "INSERT IGNORE INTO MESSAGES ( ID, TargetHost, TargetService, Message, MessageArgs, Expires)  VALUES (?, ?, ?, ?, ?, ?);";

			db.setQueryTimeout(60);

			if (db.query(q, false, Long.valueOf(queueId), target, service, message, messageArgs, Integer.valueOf(expires))) {
				if (monitor != null)
					monitor.incrementCounter("Message_db_insert");

				return true;
			}

			return false;
		}
	}

	/**
	 * @param jobId
	 * @param username
	 * @return the new token
	 */
	public static JobToken insertJobToken(final long jobId, final String username) {
		try (DBFunctions db = getQueueDB()) {
			final JobToken jb = new JobToken(jobId, username);
			if (!jb.updateOrInsert(db)) {
				logger.info("Cannot insert (or update) token for job: " + jobId);
				return null;
			}

			if (jb.exists())
				return jb;

			return null;
		}
	}

	// private static JobToken getJobToken(final long jobId) {
	// try (DBFunctions db = getQueueDB()) {
	// if (monitor != null) {
	// monitor.incrementCounter("TQ_db_lookup");
	// monitor.incrementCounter("TQ_jobtokendetails");
	// }
	//
	// final long lQueryStart = System.currentTimeMillis();
	//
	// final String q = "SELECT * FROM QUEUE_TOKEN WHERE queueId=?;";
	//
	// db.setReadOnly(true);
	// db.setQueryTimeout(30);
	//
	// if (!db.query(q, false, Long.valueOf(jobId)))
	// return null;
	//
	// monitor.addMeasurement("TQ_jobtokendetails_time", (System.currentTimeMillis() - lQueryStart) / 1000d);
	//
	// if (!db.moveNext())
	// return null;
	//
	// return new JobToken(db);
	// }
	// }

	private static boolean deleteJobToken(final long queueId) {
		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return false;

			if (monitor != null)
				monitor.incrementCounter("QUEUE_db_lookup");

			db.setQueryTimeout(60);
			db.setReadOnly(false);
			if (!db.query("DELETE FROM QUEUE_TOKEN WHERE queueId=?;", false, Long.valueOf(queueId))) {
				putJobLog(queueId, "state", "Failed to execute queue token deletion query", null);
				return false;
			}

			db.setQueryTimeout(60);
			db.setReadOnly(false);
			if (!db.query("DELETE FROM JOBTOKEN WHERE jobId=?;", false, Long.valueOf(queueId))) { // TODO: delete after full migration
				putJobLog(queueId, "state", "Failed to execute job token deletion query", null);
				return false;
			}

			putJobLog(queueId, "state", "Job token deletion query done for: " + queueId, null);
			return true;
		}
	}

	private static final String JOBMESSAGES_INSERT = "INSERT INTO JOBMESSAGES (timestamp, jobId, procinfo, tag) VALUES (?, ?, ?, ?);";

	/**
	 * @param queueId
	 * @param action
	 * @param message
	 * @param joblogtags
	 * @return <code>true</code> if the log was successfully added
	 */
	public static boolean putJobLog(final long queueId, final String action, final String message, final HashMap<String, String> joblogtags) {
		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return false;

			db.setQueryTimeout(60);

			if (monitor != null) {
				monitor.incrementCounter("TQ_db_lookup");
				monitor.incrementCounter("TQ_JOBMESSAGES_insert");
			}

			final Long now = Long.valueOf(System.currentTimeMillis() / 1000);
			final Long qid = Long.valueOf(queueId);

			if (!db.query(JOBMESSAGES_INSERT, false, now, qid, message, action))
				return false;

			if (joblogtags != null && joblogtags.size() > 0)
				for (final Map.Entry<String, String> entry : joblogtags.entrySet()) {
					if (!db.query(JOBMESSAGES_INSERT, false, now, qid, entry.getValue(), entry.getKey()))
						return false;
				}
		}

		return true;
	}

	private static boolean deleteJobAgent(final int jobagentId) {
		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return false;

			db.setQueryTimeout(60);

			logger.log(Level.INFO, "We would be asked to kill jobAgent: [" + jobagentId + "].");

			db.query("update JOBAGENT set counter=counter-1 where entryId=?", false, Integer.valueOf(jobagentId));

			final int updated = db.getUpdateCount();

			db.query("delete from JOBAGENT where counter<1");

			return updated > 0;
		}
	}

	private static void sendNotificationMail(@SuppressWarnings("unused") final Job j) {
		// send j.notify an info
		// TODO:
	}

	private static boolean setSubJobMerges(final Job j) {

		// if ($info->{split}) {
		// $self->info("We have to check if all the subjobs of $info->{split} have finished");
		// $self->do(
		// "insert into JOBSTOMERGE (masterId) select ? from DUAL where not exists (select masterid from JOBSTOMERGE where masterid = ?)",
		// {bind_values => [ $info->{split}, $info->{split} ]}
		// );
		// $self->do("update ACTIONS set todo=1 where action='MERGING'");
		// }
		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return false;

			if (monitor != null) {
				monitor.incrementCounter("TQ_db_lookup");
				monitor.incrementCounter("TQ_JOBSTOMERGE_lookup");
			}

			db.setQueryTimeout(60);

			final String q = "INSERT INTO JOBSTOMERGE (masterId) SELECT " + j.split + " FROM DUAL WHERE NOT EXISTS (SELECT masterid FROM JOBSTOMERGE WHERE masterid = " + j.split + ");";

			if (!db.query(q))
				return false;

			return setAction(JobStatus.MERGING);
		}
	}

	private static boolean setAction(final JobStatus status) {
		// $self->update("ACTIONS", {todo => 1}, "action='$status'");
		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return false;

			db.setQueryTimeout(30);

			if (monitor != null) {
				monitor.incrementCounter("TQ_db_update");
				monitor.incrementCounter("TQ_ACTIONS_update");
			}

			final String q = "UPDATE ACTIONS SET todo=1 WHERE action=? AND todo=0;";

			if (!db.query(q, false, status.toSQL()))
				return false;
		}

		return true;
	}

	/**
	 * @param args
	 */
	public static void main(final String[] args) {
		System.out.println("QUEUE TESTING...");

		try (DBFunctions db = getAdminDB()) {
			if (db == null)
				System.out.println("ADMIN DB NULL.");
		}

		System.out.println("---------------------------------------------------------------------");

		if (insertJobToken(12341234, "me") == null)
			System.out.println("exists, update refused.");

		System.out.println("---------------------------------------------------------------------");

		if (insertJobToken(12341234, "me") == null)
			System.out.println("exists, update refused.");

		System.out.println("---------------------------------------------------------------------");

		deleteJobToken(12341234);
	}

	/**
	 * Get the number of jobs in the respective state
	 *
	 * @param states
	 * @return the aggregated number of jobs per user
	 */
	public static Map<String, Integer> getJobCounters(final Set<JobStatus> states) {
		final Map<String, Integer> ret = new TreeMap<>();

		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return null;

			db.setQueryTimeout(600);

			final StringBuilder sb = new StringBuilder();

			if (monitor != null)
				monitor.incrementCounter("TQ_db_lookup");

			if (states != null && !states.contains(JobStatus.ANY))
				for (final JobStatus s : states) {
					if (sb.length() > 0)
						sb.append(',');

					if (dbStructure2_20)
						sb.append(s.getAliEnLevel());
					else
						sb.append('\'').append(s.toSQL()).append('\'');
				}

			String q;

			if (dbStructure2_20)
				q = "SELECT user,count(1) FROM QUEUE INNER JOIN QUEUE_USER using(userId) WHERE statusId IN (" + sb + ")";
			else
				q = "SELECT substring_index(submithost,'@',1),count(1) FROM QUEUE WHERE status IN (" + sb + ")";

			q += "GROUP BY 1;";

			db.setReadOnly(true);

			db.query(q);

			while (db.moveNext())
				ret.put(StringFactory.get(db.gets(1)), Integer.valueOf(db.geti(2)));
		}

		return ret;
	}

	/**
	 * @param ce
	 * @param status
	 */
	public static void setSiteQueueStatus(final String ce, final String status) {
		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return;

			// TODO: jdls?

			logger.log(Level.INFO, "Setting site with ce " + ce + " to " + status);

			db.setQueryTimeout(30);

			db.query("update SITEQUEUES set statustime=UNIX_TIMESTAMP(NOW()), status=? where site=?", false, status, ce);

			if (db.getUpdateCount() == 0) {
				logger.log(Level.FINE, "Inserting the site " + ce);
				insertSiteQueue(ce);
			}
		}
	}

	/**
	 * @param ce
	 */
	public static void insertSiteQueue(final String ce) {
		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return;

			db.setQueryTimeout(60);

			if (!db.query("insert into SITEQUEUES (siteid, site) select ifnull(max(siteid)+1,1), ? from SITEQUEUES", false, ce)) {
				logger.log(Level.INFO, "Couldn't insert queue " + ce);
				return;
			}

			resyncSiteQueueTable();
		}
	}

	/**
	 *
	 */
	public static void resyncSiteQueueTable() {
		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return;

			db.setQueryTimeout(600);

			final Map<String, Integer> status = getJobStatusFromDB();

			String sql = " update SITEQUEUES left join (select siteid, sum(cost) REALCOST, ";
			String set = " Group by statusId, siteid) dd group by siteid) bb using (siteid) set cost=REALCOST, ";

			for (final String st : status.keySet()) {
				sql += " max(if(statusId=" + status.get(st) + ", count, 0)) REAL" + st + ",";
				set += " " + st + "=REAL" + st + ",";
			}
			sql = sql.substring(0, sql.length() - 1);
			sql = set.substring(0, set.length() - 1);

			sql += " from (select siteid, statusId, sum(cost) as cost, count(*) as count from QUEUE join QUEUEPROC using(queueid) ";
			sql += set;

			logger.log(Level.INFO, "resyncSiteQueueTable with " + sql);

			db.query(sql, false);
		}
	}

	private static Map<String, Integer> jobStatuses = new HashMap<>();
	private static long jobStatusesLastUpdated = 0;

	/**
	 * @return dictionary of job statuses
	 */
	public static Map<String, Integer> getJobStatusFromDB() {
		if ((System.currentTimeMillis() - jobStatusesLastUpdated) > 1000 * 60)
			try (DBFunctions db = getQueueDB()) {
				if (db == null)
					return null;

				db.setQueryTimeout(60);

				final HashMap<String, Integer> status = new HashMap<>();

				if (db.query("select status, statusId from QUEUE_STATUS", false))
					while (db.moveNext())
						status.put(db.gets(1), Integer.valueOf(db.geti(2)));

				if (status.size() > 0) {
					jobStatusesLastUpdated = System.currentTimeMillis();
					jobStatuses = status;
				}
			}

		return jobStatuses;
	}

	/**
	 * @param host
	 * @param status
	 * @return <code>true</code> if the status was updated
	 */
	public static boolean updateHostStatus(final String host, final String status) {
		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return false;

			db.setQueryTimeout(60);

			if (host == null || "".equals(host) || status == null || "".equals(status)) {
				logger.log(Level.INFO, "Host or status parameters are empty");
				return false;
			}

			if (logger.isLoggable(Level.FINE))
				logger.log(Level.FINE, "Updating host " + host + " to status " + status);

			if (!db.query("update HOSTS set status=?,date=UNIX_TIMESTAMP(NOW()) where hostName=?", false, status, host)) {
				logger.log(Level.WARNING, "Update HOSTS failed: " + host + " and " + status);
				return false;
			}

			return db.getUpdateCount() != 0;
		}
	}

	/**
	 * @param key
	 * @param value
	 * @return value for this key
	 */
	public static int getOrInsertFromLookupTable(final String key, final String value) {
		// FIXME: these values can also be cached
		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return 0;

			db.setQueryTimeout(60);

			final String table = "QUEUE_" + key.toUpperCase();
			final String id = key + "id";
			final String q = "select " + id + " from " + table + " where " + key + "=?";

			if (logger.isLoggable(Level.FINER))
				logger.log(Level.FINER, "Going to get hostId, query: " + q);

			db.setReadOnly(true);
			db.query(q, false, value);

			// the host exists
			if (db.moveNext()) {
				if (logger.isLoggable(Level.FINER))
					logger.log(Level.FINER, "The host exists: " + db.geti(1));

				return db.geti(1);
			}
			// host doesn't exist, we insert it
			if (logger.isLoggable(Level.FINE))
				logger.log(Level.FINE, "The host doesn't exist. Inserting...");

			db.setLastGeneratedKey(true);

			final String qi = "insert into " + table + " (" + key + ") values (?);";
			db.setReadOnly(false);
			final boolean ret = db.query(qi, false, value);

			if (logger.isLoggable(Level.FINE))
				logger.log(Level.FINE, qi + " with ?=" + value + ": " + ret);

			if (ret) {
				final int val = db.getLastGeneratedKey().intValue();
				if (logger.isLoggable(Level.FINE))
					logger.log(Level.FINE, "Returning: " + val);

				return val;
			}

			// something went wrong ? :-(
			return 0;
		}
	}

	/**
	 * @param host
	 * @param port
	 * @param version
	 * @return value for this key
	 */
	public static int getHostOrInsert(final String host, final int port, final String version) {
		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return 0;

			final String q = "select hostId from HOSTS where hostName=?";

			if (logger.isLoggable(Level.FINER))
				logger.log(Level.FINER, "Going to get HOST " + host + ", query: " + q);

			db.setReadOnly(true);
			db.query(q, false, host);

			// the host exists
			int hostId;
			if (db.moveNext()) {
				if (logger.isLoggable(Level.FINE))
					logger.log(Level.FINE, "The HOST exists");

				hostId = db.geti(1);
			}
			else { // we insert the host
				if (logger.isLoggable(Level.FINE))
					logger.log(Level.FINE, "The host doesn't exist. Inserting " + host + ":" + port + ":" + version);

				hostId = insertHost(host, port, version);
				if (hostId == 0) {
					logger.severe("Couldn't insertHost in getFromHostsOrInsert");
					return 0;
				}
			}

			return hostId;
		}
	}

	private static int insertHost(final String host, final int port, final String version) {
		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return 0;

			final String domain = host.substring(host.indexOf(".") + 1);

			final List<Integer> domains = getSitesByDomain(domain);

			int siteId;
			if (domains == null || domains.size() <= 0) {
				siteId = insertIntoSites(domain);
				if (siteId == 0) {
					logger.severe("Error (insertHost): couldn't insert into sites!");
					return 0;
				}
			}
			else
				siteId = domains.get(0).intValue();

			final String qi = "insert into HOSTS (hostId, hostName, siteId, hostPort, Version) values (?,?,?,?,?);";
			db.setReadOnly(false);
			db.setLastGeneratedKey(true);
			final boolean ret = db.query(qi, false, Integer.valueOf(0), host, Integer.valueOf(siteId), Integer.valueOf(port), version);

			if (logger.isLoggable(Level.FINER))
				logger.log(Level.FINER, "insertHost with query : " + qi + " with ?=" + host + " and siteId: " + siteId);

			if (ret) {
				final int val = db.getLastGeneratedKey().intValue();

				if (logger.isLoggable(Level.FINE))
					logger.log(Level.FINE, "Returning HOST hostId: " + val);

				return val;
			}
			return 0;
		}
	}

	private static int insertIntoSites(final String domain) {
		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return 0;

			final HashMap<String, Object> domainInfo = LDAPHelper.getInfoDomain(domain);

			if (domainInfo == null || domainInfo.size() == 0) {
				logger.severe("Error: cannot find site root configuration in LDAP for domain: " + domain);
				return 0;
			}

			final String qi = "insert into SITES (siteName,siteId,masterHostId,adminName,location,domain, longitude, latitude,record,url) values (?,?,?,?,?,?,?,?,?,?);";
			db.setReadOnly(false);
			db.setLastGeneratedKey(true);

			if (logger.isLoggable(Level.FINE))
				logger.log(Level.FINE, "insertIntoSites: " + qi + " with domain: " + domain);

			final boolean ret = db.query(qi, false, domainInfo.get("ou"), Integer.valueOf(0), Integer.valueOf(0), domainInfo.containsKey("adminsitrator") ? domainInfo.get("administrator") : "",
					domainInfo.containsKey("location") ? domainInfo.get("location") : "", domain, domainInfo.containsKey("longitude") ? domainInfo.get("longitude") : Double.valueOf(0),
					domainInfo.containsKey("latitude") ? domainInfo.get("latitude") : Double.valueOf(0.0), domainInfo.containsKey("record") ? domainInfo.get("record") : "",
					domainInfo.containsKey("url") ? domainInfo.get("url") : "");

			if (ret) {
				final int val = db.getLastGeneratedKey().intValue();

				if (logger.isLoggable(Level.FINE))
					logger.log(Level.FINE, "Returning SITES siteId: " + val);

				return val;
			}

			return 0;
		}
	}

	private static List<Integer> getSitesByDomain(final String domain) {
		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return null;

			final ArrayList<Integer> sites = new ArrayList<>();

			final String q = "select siteId from SITES where domain=?";

			if (logger.isLoggable(Level.FINER))
				logger.log(Level.FINER, "Going to get sites for domain: " + domain + ", query: " + q);

			db.setReadOnly(true);
			db.query(q, false, domain);
			while (db.moveNext())
				sites.add(Integer.valueOf(db.geti(1)));

			return sites;
		}
	}

	private static final ExpirationCache<String, Integer> siteIdCache = new ExpirationCache<>(1024);

	/**
	 * @param ceName
	 * @return site ID
	 */
	public static int getSiteId(final String ceName) {
		final Integer cachedValue = siteIdCache.get(ceName);

		if (cachedValue != null)
			return cachedValue.intValue();

		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return 0;

			if (logger.isLoggable(Level.FINER))
				logger.log(Level.FINER, "Going to select siteId: select siteid from SITEQUEUES where site=? " + ceName);

			db.setReadOnly(true);
			db.setQueryTimeout(60);

			if (db.query("select siteid from SITEQUEUES where site=?", false, ceName) && db.moveNext()) {
				final int value = db.geti(1);

				siteIdCache.put(ceName, Integer.valueOf(value), 1000 * 60 * 60);
				return value;
			}
		}
		return 0;
	}

	/**
	 * @param ceName
	 * @return matching queue IDs for the given CE name (full or partial)
	 */
	public static Collection<Integer> getSiteIDs(final String ceName) {
		final int idx1 = ceName.indexOf("::");
		final int idx2 = ceName.lastIndexOf("::");

		if (idx2 > idx1 && idx2 < ceName.length() - 2 && !ceName.contains("%")) {
			// it seems to be a fully qualified CE name, get it from cache
			final int singleCDid = getSiteId(ceName);

			if (singleCDid > 0)
				return Arrays.asList(Integer.valueOf(singleCDid));

			return null;
		}

		String pattern = ceName;

		if (idx1 < 0)
			pattern = "ALICE::" + ceName;
		else {
			if (idx1 == 0)
				pattern = "ALICE" + ceName;
			else if (idx1 == idx2 && !pattern.toUpperCase().startsWith("ALICE::"))
				pattern = "ALICE::" + pattern;
		}

		pattern += "%";

		final Collection<Integer> queueIDs = new ArrayList<>();

		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return null;

			final String q = "select siteid,site from SITEQUEUES where site LIKE ?;";

			if (logger.isLoggable(Level.FINER))
				logger.log(Level.FINER, "Going to find CEs matching pattern: " + pattern);

			db.setReadOnly(true);
			db.setQueryTimeout(60);

			if (db.query(q, false, pattern) && db.moveNext()) {
				do {
					final Integer siteId = Integer.valueOf(db.geti(1));
					final String siteName = db.gets(2);

					siteIdCache.put(siteName, siteId, 1000 * 60 * 60);

					queueIDs.add(siteId);
				} while (db.moveNext());
			}
		}

		return queueIDs;
	}

	private static final GenericLastValuesCache<Integer, String> siteNameCache = new GenericLastValuesCache<>() {
		private static final long serialVersionUID = 1L;

		@Override
		protected int getMaximumSize() {
			return 20000;
		}

		@Override
		protected boolean cacheNulls() {
			return false;
		}

		@Override
		protected String resolve(final Integer key) {
			try (DBFunctions db = getQueueDB()) {
				if (db == null)
					return null;

				db.setReadOnly(true);
				db.setQueryTimeout(60);

				db.query("select site from SITEQUEUES where siteId=?", false, key);

				if (db.moveNext())
					return StringFactory.get(db.gets(1));
			}

			return null;
		}
	};

	/**
	 * @param siteId
	 * @return site name
	 */
	public static String getSiteName(final int siteId) {
		if (siteId <= 0)
			return null;

		return siteNameCache.get(Integer.valueOf(siteId));
	}

	/**
	 * @param agentId
	 * @param queueId
	 * @return 0 if no action was taken, 1 if the query succeeded (though this is no guarantee that anything was actually deleted)
	 */
	public static int deleteJobAgent(final long agentId, final long queueId) {
		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return 0;

			db.setQueryTimeout(60);

			final ArrayList<Object> bindValues = new ArrayList<>();
			String oldestQueueIdQ = "";

			if (queueId > 0) {
				bindValues.add(Long.valueOf(queueId));
				oldestQueueIdQ = ",oldestQueueId=?";
			}

			bindValues.add(Long.valueOf(agentId));

			db.query("update JOBAGENT set counter=counter-1 " + oldestQueueIdQ + " where entryId=?", false, bindValues.toArray(new Object[0]));

			db.query("delete from JOBAGENT where counter<1", false);
		}

		return 1;
	}

	/**
	 * @param host
	 * @param status
	 * @param connected
	 * @param port
	 * @param version
	 * @param ceName
	 * @return <code>true</code> if the host existed and was successfully updated in the database
	 */
	public static boolean updateHost(final String host, final String status, final Integer connected, final int port, final String version, final String ceName) {
		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return false;

			db.setReadOnly(false);

			if (logger.isLoggable(Level.FINER))
				logger.log(Level.FINER, "Going to updateHost for: " + host + " status: " + status);

			if (!db.query("update HOSTS set status=?,connected=?,hostPort=?,version=?,cename=? where hostName=?", false, status, connected, Integer.valueOf(port), version, ceName, host))
				return false;

			return db.getUpdateCount() > 0;
		}
	}

	/**
	 * @param ceName
	 * @return the value of the "blocked" column in the database for this CE name
	 */
	public static String getSiteQueueBlocked(final String ceName) {
		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return null;

			if (logger.isLoggable(Level.FINER))
				logger.log(Level.FINER, "Going to select SITEQUEUES.blocked for: " + ceName);

			db.setReadOnly(true);
			db.setQueryTimeout(60);

			if (db.query("select blocked from SITEQUEUES where site=?", false, ceName) && db.moveNext()) {
				final String value = db.gets(1);
				return value;
			}
			return null;
		}
	}

	/**
	 * @param host
	 * @param ceName
	 * @return a 2 element array with the max number of jobs as the first value and the limit of queued jobs as the second one
	 */
	public static ArrayList<Integer> getNumberMaxAndQueuedCE(final String host, final String ceName) {
		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return null;

			if (logger.isLoggable(Level.FINER))
				logger.log(Level.FINER, "Going to select HOSTS.maxQueued,maxJobs for: " + host + " - " + ceName);

			db.setReadOnly(true);
			db.setQueryTimeout(60);

			final ArrayList<Integer> slots = new ArrayList<>(2);

			if (db.query("select maxJobs,maxQueued from HOSTS where hostName=? and ceName=? and maxJobs is not null and maxQueued is not null", false, host, ceName) && db.moveNext()) {
				slots.add(Integer.valueOf(db.geti(1)));
				slots.add(Integer.valueOf(db.geti(2)));
			}
			return slots;
		}
	}

	/**
	 * @param queueId
	 * @return resubmission field for the job
	 */
	public static int getResubmission(final Long queueId) {
		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return -1;

			if (logger.isLoggable(Level.FINER))
				logger.log(Level.FINER, "Going to select resubmission for: " + queueId);

			db.setReadOnly(true);
			db.setQueryTimeout(60);

			int resubmission = -1;

			if (db.query("select resubmission from QUEUE where queueId=?", false, queueId) && db.moveNext()) {
				resubmission = db.geti(1);
			}

			return resubmission;
		}
	}

	/**
	 * Resubmit a job
	 *
	 * @param user
	 * @param queueId
	 * @return 0 if resubmitted correctly, >0 error
	 */
	public static Entry<Integer, String> resubmitJob(final AliEnPrincipal user, final long queueId) {
		final Job j = getJob(queueId, true);
		final JobStatus js = j.status();

		if (AuthorizationChecker.canModifyJob(j, user)) {
			if (logger.isLoggable(Level.FINE))
				logger.log(Level.FINE, "Resubmit (authorized) for  [" + queueId + "] by user/role [" + user.getName() + "]");

			// check job quotas to see if we are allowed to submit
			final Entry<Integer, String> quota = QuotaUtilities.checkJobQuota(user.getName(), 1);
			final Integer code = quota.getKey();
			if (code.intValue() != 0) {
				return new AbstractMap.SimpleEntry<>(Integer.valueOf(ErrNo.EDQUOT.getErrorCode()), "Resubmit: job quota problem: " + quota.getValue());
			}

			if (logger.isLoggable(Level.FINE))
				logger.log(Level.FINE, "Resubmit: quotas approved: " + queueId);

			// cleanup active jobtoken
			if (!deleteJobToken(queueId)) {
				logger.info("Cannot cleanup job token: " + queueId);
				return new AbstractMap.SimpleEntry<>(Integer.valueOf(ErrNo.ENOKEY.getErrorCode()), "Resubmit: cannot cleanup job token: " + queueId);
			}

			logger.fine("Resubmit: token deleted for: " + queueId);

			// get the Path (folder of stored files) to cleanup if necessary
			JDL jdl;
			try {
				jdl = new JDL(j.getJDL());
			}
			catch (final IOException e) {
				logger.severe("Resubmit: cannot create JDL for job: " + queueId + " Exception: " + e);
				return new AbstractMap.SimpleEntry<>(Integer.valueOf(ErrNo.EILSEQ.getErrorCode()), "Resubmit: cannot create JDL for job: " + queueId);
			}

			JobStatus targetStatus = JobStatus.WAITING;

			if (j.masterjob) {
				if (j.status().isERROR_()) {
					// If the masterjob could not be split previously then it's worth trying again
					targetStatus = JobStatus.INSERTING;
				}
				else {
					// Cannot resubmit a masterjob if it has already split, what has to happen is resubmitting its subjobs
					// but that's a different command that would iterate through the subjobs and act on each of them

					return new AbstractMap.SimpleEntry<>(Integer.valueOf(ErrNo.EBADFD.getErrorCode()),
							"Resubmit: masterjob is not in an error state itself, will not split it again: " + queueId + " / " + j.status().name());
				}
			}

			final String pathFromJDL = jdl.gets("Path");

			if (!clearPathAndResultsJDL(Long.valueOf(queueId))) {
				logger.info("Cannot cleanup path and resultsJdl: " + queueId);
				return new AbstractMap.SimpleEntry<>(Integer.valueOf(ErrNo.EIO.getErrorCode()), "Resubmit: cannot cleanup path and resultsJdl: " + queueId);
			}

			logger.fine("Resubmit: cleanup of path and resultsJdl done: " + queueId);

			if (JobStatus.ERROR_I.equals(j.status())) {
				// If it could not be inserted before, retry that operation, otherwise go directly to WAITING

				targetStatus = JobStatus.INSERTING;
			}

			final int unassignedId = getSiteId("unassigned::site");

			try (DBFunctions db = getQueueDB()) {
				if (db == null)
					return new AbstractMap.SimpleEntry<>(Integer.valueOf(ErrNo.EAGAIN.getErrorCode()), "Resubmit: cannot get DB to finalise resubmission: " + queueId);

				db.setReadOnly(false);
				db.setQueryTimeout(60);

				logger.fine("Resubmit: updating status to WAITING and initialize fields of " + queueId);

				// job back to resubmitted status, restore fields
				if (!db.query("UPDATE QUEUE SET statusId=?, resubmission=resubmission+1, started=null, finished=null, exechostid=null, siteid=? where queueId=?", false,
						Integer.valueOf(targetStatus.getAliEnLevel()), Integer.valueOf(unassignedId), Long.valueOf(queueId))) {
					logger.severe("Resubmit: cannot update job to WAITING: " + queueId);
					return new AbstractMap.SimpleEntry<>(Integer.valueOf(ErrNo.EBADFD.getErrorCode()), "Resubmit: cannot update job to WAITING: " + queueId);
				}

				logger.fine("Resubmit: update SITEQUEUES of " + queueId);

				// update queue counters
				db.query(
						"UPDATE SITEQUEUES set " + j.getStatusName() + "=GREATEST(" + j.getStatusName() + "-1,0), " + targetStatus.name() + "=GREATEST(" + targetStatus.name() + ",0)+1 where siteid=?",
						false, Integer.valueOf(j.siteid));

				// if the job was attached to a node, we tell him to hara-kiri
				if (j.node != null && (js == JobStatus.STARTED || js == JobStatus.RUNNING || js == JobStatus.ASSIGNED || js == JobStatus.ZOMBIE || js == JobStatus.SAVING)) {
					logger.fine("Resubmit: sending kill message to job " + queueId);

					final String target = j.node + "-" + queueId + "-" + j.resubmission;
					final int expires = (int) (System.currentTimeMillis() / 1000) + 3600 * 3; // now + 3h

					if (!insertMessage(queueId, target, "JobAgent", "killProcess", String.valueOf(j.queueId), expires))
						logger.severe("Resubmit: could not insert kill message: " + queueId);
				}

				logger.fine("Resubmit: update or insert JOBAGENT of " + queueId);

				// update the jobagent entry
				if (targetStatus == JobStatus.WAITING) {
					final int agentId = updateOrInsertJobAgent(j, jdl);
					if (agentId == 0) {
						logger.severe("Resubmit: could not update jobagent: " + queueId);
						return new AbstractMap.SimpleEntry<>(Integer.valueOf(ErrNo.EBADFD.getErrorCode()), "Resubmit: cannot updateOrInsert jobagent: " + queueId);
					}
					else if (agentId > 0) { // we need to update the agentId entry to reflect the new JOBAGENT entry
						db.setReadOnly(false);
						db.setQueryTimeout(60);

						if (logger.isLoggable(Level.FINER))
							logger.log(Level.FINER, "Resubmit: update agentId and statusId in QUEUE");

						if (!db.query("update QUEUE set agentId=?, statusId=" + targetStatus.getAliEnLevel() + " where queueid=?", false, Integer.valueOf(agentId), Long.valueOf(j.queueId))
								|| db.getUpdateCount() == 0) {
							logger.severe("Resubmit: could not update QUEUE to update status and agentId: " + queueId + " - " + agentId);
							return new AbstractMap.SimpleEntry<>(Integer.valueOf(ErrNo.EBADFD.getErrorCode()), "Resubmit: cannot update status and agentId of job: " + queueId + " - " + agentId);
						}
					}

					// if is a subjob, the master goes to SPLIT
					if (j.split > 0) {
						if (logger.isLoggable(Level.FINER))
							logger.log(Level.FINER, "Resubmit: put masterjob of the resubmited subjob to SPLIT");

						if (!db.query("UPDATE QUEUE set statusId=? where queueId=? and statusId!=?", false, Integer.valueOf(JobStatus.SPLIT.getAliEnLevel()), Long.valueOf(j.split),
								Integer.valueOf(JobStatus.SPLIT.getAliEnLevel()))) {
							logger.severe("Resubmit: cannot put masterjob back to SPLIT: " + queueId);
							return new AbstractMap.SimpleEntry<>(Integer.valueOf(ErrNo.EBADFD.getErrorCode()), "Resubmit: cannot put masterjob back to SPLIT: " + queueId);
						}
					}

					// we need to clean up the previous output
					if (pathFromJDL != null || j.path != null) {
						final Collection<LFN> list = LFNUtils.find(pathFromJDL != null ? pathFromJDL : j.path, "*", LFNUtils.FIND_FILTER_JOBID, null, "", Long.valueOf(queueId));
						if (list != null)
							for (final LFN l : list) {
								if (l.jobid == queueId) {
									logger.info("Resubmit: removing output file: " + l.getCanonicalName());
									putJobLog(queueId, "trace", "Resubmit: removing output file: " + l.getCanonicalName(), null);
									if (!LFNUtils.rmLFN(user, l, false)) {
										logger.severe("Resubmit: could not remove output file: " + l.getCanonicalName());
										putJobLog(queueId, "trace", "Resubmit: could not remove output file: " + l.getCanonicalName(), null);
										return new AbstractMap.SimpleEntry<>(Integer.valueOf(ErrNo.EIO.getErrorCode()),
												"Resubmit: could not remove output file: " + l.getCanonicalName() + " for " + queueId);
									}
								}
							}
					}

					if (js == JobStatus.SAVING || js == JobStatus.SAVED || js == JobStatus.ERROR_E || js == JobStatus.ERROR_V || js == JobStatus.ZOMBIE) {
						CatalogueUtils.cleanLfnBookedForJob(queueId);
					}

					logger.fine("Resubmit: putting joblog and returning " + queueId);
					putJobLog(queueId, "state", "Job resubmitted (back to WAITING)", null);
					return new AbstractMap.SimpleEntry<>(Integer.valueOf(0), "Resubmit: back to WAITING: " + queueId);
				}

				// TODO: masterjob
				if (logger.isLoggable(Level.FINE))
					logger.log(Level.FINE, "Resubmit: job is a masterJob, ignore: " + queueId);

				return new AbstractMap.SimpleEntry<>(Integer.valueOf(0), "Resubmit: job is a masterJob, ignore: " + queueId);
			}
		}

		return new AbstractMap.SimpleEntry<>(Integer.valueOf(ErrNo.EPERM.getErrorCode()), "Resubmit: not authorized for : " + queueId);
	}

	private static int updateOrInsertJobAgent(final Job j, final JDL jdl) {
		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return 0;

			if (logger.isLoggable(Level.FINER))
				logger.log(Level.FINER, "Going to select updateOrInsertJobAgent for: " + j.queueId);

			db.setReadOnly(false);
			db.setQueryTimeout(60);

			if (!db.query("update JOBAGENT join QUEUE on (agentid=entryid) set counter=counter+1 where queueid=?", false, Long.valueOf(j.queueId)))
				return 0;

			// the jobagent doesn't exist anymore, reinsert
			if (db.getUpdateCount() == 0) {
				if (logger.isLoggable(Level.FINE))
					logger.log(Level.FINE, "updateOrInsertJobAgent: the jobagent doesn't exist anymore, going to extract params for : " + j.queueId);

				final HashMap<String, Object> params = extractJAParametersFromJDL(jdl);
				final int agentId = insertJobAgent(params);

				if (logger.isLoggable(Level.FINE))
					logger.log(Level.FINE, "updateOrInsertJobAgent: inserted agentId: " + agentId);

				if (agentId == 0) {
					logger.log(Level.WARNING, "updateOrInsertJobAgent: couldn't insertJobAgent : " + j.queueId);
					return 0;
				}

				return agentId;
			}
			return -1;
		}
	}

	private static int insertJobAgent(final HashMap<String, Object> params) {
		String reqs = "1=1 ";

		final ArrayList<Object> bind = new ArrayList<>();
		for (final String key : params.keySet()) {
			if ("counter".equals(key))
				continue;

			reqs += " and " + key + " = ?";
			bind.add(params.get(key));
		}

		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return 0;

			db.setReadOnly(true);
			db.setQueryTimeout(60);

			int entryId = 0;
			if (!db.query("SELECT entryId from JOBAGENT where " + reqs, false, bind.toArray())) {
				logger.severe("insertJobAgent: failed selecting from JOBAGENT!");
				return 0;
			}

			if (db.moveNext()) {
				entryId = db.geti(1);
			}

			if (entryId == 0) {
				if (logger.isLoggable(Level.FINE))
					logger.log(Level.FINE, "insertJobAgent: nothing matched request, inserting!");

				if (params.containsKey("userid")) {

					db.setReadOnly(true);
					db.setQueryTimeout(60);

					if (db.query("select computedPriority from PRIORITY where userid=?", false, params.get("userid")) && db.moveNext()) {
						params.put("priority", Integer.toString(db.geti(1)));
					}
				}

				// create INSERT query and execute
				String insert_fields = "(";
				String insert_values = " VALUES (";

				bind.clear();
				for (final String key : params.keySet()) {
					insert_fields += key + ",";
					insert_values += "?,";
					bind.add(params.get(key));
				}
				insert_fields = insert_fields.substring(0, insert_fields.length() - 1);
				insert_values = insert_values.substring(0, insert_values.length() - 1);
				insert_fields += ") ";
				insert_values += ") ";

				db.setReadOnly(false);
				db.setQueryTimeout(60);
				db.setLastGeneratedKey(true);
				if (!db.query("INSERT INTO JOBAGENT " + insert_fields + insert_values, false, bind.toArray())) {
					logger.severe("insertJobAgent: failed to insert! fields:" + insert_fields + " values " + bind.toString());
					return 0;
				}

				if (logger.isLoggable(Level.FINE))
					logger.log(Level.FINE, "insertJobAgent: insertion done: " + db.getLastGeneratedKey().intValue());

				return db.getLastGeneratedKey().intValue();
			}

			// otherwise there is an entry that matches our reqs already, update it
			db.setReadOnly(false);
			db.setQueryTimeout(60);

			if (!db.query("update JOBAGENT set counter=counter+1 where entryId=?", false, Integer.valueOf(entryId)))
				return 0;

			return entryId;
		}
	}

	/**
	 * @param jdl
	 * @return parameters needed for jobagent
	 */
	public static HashMap<String, Object> extractJAParametersFromJDL(final JDL jdl) {
		if (logger.isLoggable(Level.FINER))
			logger.log(Level.FINER, "Going to extractJAParamentersFromJDL");

		if (jdl == null)
			return null;

		final String reqs = jdl.gets("Requirements");
		if (reqs == null)
			return null;

		final HashMap<String, Object> params = new HashMap<>();
		params.put("counter", Integer.valueOf(1));
		params.put("ttl", Integer.valueOf(18000));
		params.put("disk", Integer.valueOf(0));
		params.put("packages", "%");
		params.put("`partition`", "%");
		params.put("price", Float.valueOf(1));
		params.put("revision", Integer.valueOf(0));

		// parse the other.CloseSE (and !)
		final HashSet<String> noses = new HashSet<>();
		Pattern pat = Pattern.compile("\\!member\\(other.CloseSE,\"\\w+::(\\w+)::\\w+\"\\)");
		Matcher m = pat.matcher(reqs);
		while (m.find())
			noses.add(m.group(1));

		String site = "";
		pat = Pattern.compile("\\s*member\\(other.CloseSE,\"\\w+::(\\w+)::\\w+\"\\)");
		m = pat.matcher(reqs);
		while (m.find()) {
			final String tmpsite = m.group(1);
			if (!noses.contains(tmpsite))
				site += "," + tmpsite;
		}
		if (!"".equals(site))
			site += ",";

		params.put("site", site);

		// parse the other.CE (and !)
		String noce = "";
		pat = Pattern.compile("\\!other.CE\\s*==\\s*\"([^\\\"]+)\"");
		m = pat.matcher(reqs);
		while (m.find()) {
			noce += "," + m.group(1);
		}
		if (!"".equals(noce))
			noce += ",";

		params.put("noce", noce);

		String ce = "";
		pat = Pattern.compile("\\s*other.CE\\s*==\\s*\"([^\\\"]+)\"");
		m = pat.matcher(reqs);
		while (m.find()) {
			ce += "," + m.group(1);
		}
		if (!"".equals(ce))
			ce += ",";

		params.put("ce", ce);

		// parse Packages
		final HashSet<String> packages = new HashSet<>();
		pat = Pattern.compile("other.Packages,\"([^\"]+)\""); // member(other.Packages,\"VO_ALICE@AliPhysics::vAN-20170813-1\")
		m = pat.matcher(reqs);
		while (m.find())
			packages.add(m.group(1));

		if (packages.size() > 0) {
			String packs = "%,";
			for (final String pkg : packages) {
				packs += pkg + ",%,";
			}
			packs = packs.substring(0, packs.length() - 1);

			params.put("packages", packs);
		}

		// parse TTL
		pat = Pattern.compile("other.TTL\\s*>\\s*(\\d+)");
		m = pat.matcher(reqs);
		if (m.find())
			params.put("ttl", Integer.valueOf(m.group(1)));

		// get user
		final String user = jdl.getUser();
		if (user != null)
			params.put("userid", getUserId(user));

		// parse LocalDiskSpace
		pat = Pattern.compile("other.LocalDiskSpace\\s*>\\s*(\\d+)");
		m = pat.matcher(reqs);
		if (m.find())
			params.put("disk", Integer.valueOf(m.group(1)));

		// parse LocalDiskSpace
		pat = Pattern.compile("other.GridPartitions,\"([^\"]+)\""); // other.GridPartitions,"ultrapower"
		m = pat.matcher(reqs);
		if (m.find())
			params.put("`partition`", m.group(1));

		// parse Price
		pat = Pattern.compile("other.Price\\s*<=\\s*(\\d+)");
		m = pat.matcher(reqs);
		if (m.find())
			params.put("price", Float.valueOf(m.group(1)));

		// parse CVMFS_revision
		pat = Pattern.compile("other.CVMFS_Revision\\s*>=\\s*(\\d+)");
		m = pat.matcher(reqs);
		if (m.find())
			params.put("revision", Integer.valueOf(m.group(1)));

		// parse CPU cores
		final Integer cpuCores = jdl.getInteger("CPUCores");

		params.put("cpucores", cpuCores == null || cpuCores.intValue() < 0 || cpuCores.intValue() > 100 ? Integer.valueOf(1) : cpuCores);

		if (logger.isLoggable(Level.FINER))
			logger.log(Level.FINER, "extracted params: " + params.toString());

		return params;
	}

	private static boolean clearPathAndResultsJDL(final Long queueId) {
		BookingTable.resubmitJob(queueId);

		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return false;

			if (logger.isLoggable(Level.FINE))
				logger.log(Level.FINE, "Going to clean path and resultsjdl in resubmission for: " + queueId);

			db.setReadOnly(false);
			db.setQueryTimeout(60);

			if (!db.query("update QUEUEJDL set path=null,resultsJdl=null where queueId=?", false, queueId))
				return false;

			return db.getUpdateCount() != 0;
		}
	}

	/**
	 * Set the QUEUEJDL.resultsJdl field for a completed job
	 *
	 * @param jdl
	 * @param queueId
	 * @return <code>true</code> if the update could be done
	 */
	public static boolean addResultsJdl(final JDL jdl, final Long queueId) {
		if (logger.isLoggable(Level.FINE))
			logger.log(Level.FINE, "Going to add the following resultsJdl to pid " + queueId + ": " + jdl);

		try (DBFunctions db = getQueueDB()) {
			if (db == null)
				return false;

			if (!db.query("update QUEUEJDL set resultsJdl=? where queueId=?", false, jdl.toString(), queueId))
				return false;

			return true;
		}
	}

}
