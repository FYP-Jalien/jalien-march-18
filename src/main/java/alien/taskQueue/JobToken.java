package alien.taskQueue;

import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import lazyj.DBFunctions;
import lazyj.StringFactory;

/**
 * @author ron
 * @since Nov 2, 2011
 *
 */
public class JobToken implements Comparable<JobToken> {

	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(JobToken.class.getCanonicalName());

	/**
	 * Monitoring component
	 */
	static final Monitor monitor = MonitorFactory.getMonitor(JobToken.class.getCanonicalName());

	/**
	 * jobId
	 */
	public long queueId;

	/**
	 * Username
	 */
	public String username;

	/**
	 * Resubmission
	 */
	public int resubmission;

	/**
	 * Legacy, session-ID like token
	 */
	public String legacyToken;

	/**
	 * Set to <code>true</code> if the entry existed in the database, or to <code>false</code> if not. Setting the other fields will only be permitted if this field is false.
	 */
	private boolean exists;

	/**
	 * Load one row from a TOKENS table
	 *
	 * @param db
	 */
	JobToken(final DBFunctions db) {
		init(db);

		this.exists = true;
	}

	private static final char[] tokenStreet = new char[] { 'X', 'Q', 't', '2', '!', '^', '9', '5', '3', '4', '5', 'o', 'r', 't', '{', ')', '}', '[', ']', 'h', '9', '|', 'm', 'n', 'b', 'v', 'c', 'x',
			'z', 'a', 's', 'd', 'f', 'g', 'h', 'j', 'k', 'l', ':', 'p', 'o', 'i', 'u', 'y', 'Q', 'W', 'E', 'R', 'T', 'Y', 'U', 'I', 'O', 'P', 'A', 'S', 'D', 'F', 'G', 'H', 'J', 'Z', 'X', 'C', 'V',
			'B', 'N', 'M' };

	/**
	 * Create a new JobToken object
	 *
	 * @param queueId
	 * @param username
	 */
	JobToken(final long queueId, final String username, final int resubmission) {
		this.queueId = queueId;

		this.username = username;

		this.exists = false;

		this.resubmission = resubmission;

		this.legacyToken = generateToken();
	}

	/**
	 * @return a new, hopefully unique, job token
	 */
	public static synchronized String generateToken() {
		final char[] tok = new char[32];

		for (int i = 0; i < 32; i++)
			tok[i] = tokenStreet[ThreadLocalRandom.current().nextInt(tokenStreet.length)];

		return new String(tok);
	}

	// /**
	// * Create a 32 chars long token (job token)
	// *
	// * @param db
	// * @return <code>true</code> if the token was successfully generated
	// */
	// public boolean spawnToken(final DBFunctions db) {
	// this.token = generateToken();
	//
	// update(db);
	//
	// return token.length() == 32;
	// }

	// /**
	// * The special value for when the job is in INSERTING and then a real value will be assigned by AliEn
	// *
	// * @param db
	// */
	// public void emptyToken(final DBFunctions db) {
	// this.token = "-1";
	//
	// update(db);
	// }

	private void init(final DBFunctions db) {
		this.queueId = db.geti("queueid");

		this.username = StringFactory.get(db.gets("username"));

		this.resubmission = db.geti("resubmission");

		this.exists = true;
	}

	// private static final String INSERT_QUERY = "INSERT INTO QUEUE_TOKEN (queueId, username, resubmission) VALUES (?, ?, ?);";

	// private boolean insert(final DBFunctions db) {
	// try {
	// if (db.query(INSERT_QUERY, false, Long.valueOf(queueId), username, Integer.valueOf(resubmission))) {
	// if (monitor != null)
	// monitor.incrementCounter("jobToken_db_insert");
	//
	// exists = true;
	//
	// return true;
	// }
	// } finally {
	// db.close();
	// }
	//
	// return false;
	// }

	// private static final String UPDATE_QUERY = "UPDATE QUEUE_TOKEN SET resubmission=? WHERE queueId=?;";
	private static final String REPLACE_QUERY = "REPLACE INTO QUEUE_TOKEN VALUES (?,?,?)";

	/**
	 * update the entry in the database, inserting it if necessary
	 *
	 * @param db
	 * @return <code>true</code> if successful
	 */
	boolean updateOrInsert(final DBFunctions db) {
		if (db == null)
			return false;

		logger.log(Level.INFO, "Replace JobToken for: " + queueId + " and exists: " + exists + ", known resubmission count: " + resubmission);

		final int resubmission_queue = resubmission >= 0 ? resubmission : TaskQueueUtils.getResubmission(Long.valueOf(queueId));

		if (resubmission_queue < 0) { // problem getting resubmission from QUEUE
			logger.info("JobToken updateOrInsert: cannot retrieve resubmision");
			return false;
		}

		// if (!exists) {
		// final boolean insertOK = insert(db);
		// logger.log(Level.INFO, "Insert JobToken for: " + queueId + " was " + insertOK);
		// return insertOK;
		// }

		try {
			// only the resubmission can change
			db.setReadOnly(false);
			db.setQueryTimeout(60);

			if (!db.query(REPLACE_QUERY, false, Long.valueOf(queueId), username, Integer.valueOf(resubmission_queue))) {
				// wrong table name or what?
				logger.log(Level.INFO, "Replace JobToken for queueId: " + queueId + " username: " + username + " resubmission: " + resubmission + " failed");
				return false;
			}

			if (db.getUpdateCount() == 0) {
				// the entry did not exist in fact, what's going on?
				logger.log(Level.INFO, "Replace JobToken for: " + queueId + " count 0");
				return false;
			}

			if (!db.query("REPLACE INTO JOBTOKEN (jobId, userName, jobToken) VALUES (?, ?, ?);", false, Long.valueOf(queueId), username, legacyToken)) {
				logger.log(Level.INFO, "Legacy table query failed");
				return false;
			}

			if (db.getUpdateCount() == 0) {
				// the entry did not exist in fact, what's going on?
				logger.log(Level.INFO, "Replace legacy JobToken for: " + queueId + " count 0");
				return false;
			}
		}
		finally {
			db.close();
		}

		if (monitor != null)
			monitor.incrementCounter("jobToken_db_update");

		resubmission = resubmission_queue;
		exists = true;

		return true;
	}

	@Override
	public String toString() {
		return "queueId\t\t: " + queueId + "\n" + "username\t\t: " + username + "\n" + "resubmission\t\t: " + resubmission + "\n";
	}

	@Override
	public int compareTo(final JobToken o) {
		final long diff = queueId - o.queueId;

		if (diff < 0)
			return -1;

		if (diff > 0)
			return 1;

		return resubmission - o.resubmission;
	}

	@Override
	public boolean equals(final Object obj) {
		if (!(obj instanceof JobToken))
			return false;

		return compareTo((JobToken) obj) == 0;
	}

	@Override
	public int hashCode() {
		return (int) queueId;
	}

	/**
	 * @return <code>true</code> if the guid was taken from the database, <code>false</code> if it is a newly generated one
	 */
	public boolean exists() {
		return exists;
	}

	private static final String DESTROY_QUERY = "DELETE FROM QUEUE_TOKEN where queueId=?;";

	/**
	 * Delete a jobToken in the DB
	 *
	 * @param db
	 * @return success of the deletion
	 */
	boolean destroy(final DBFunctions db) {
		try {
			db.setReadOnly(false);
			db.setQueryTimeout(60);

			if (db.query(DESTROY_QUERY, false, Long.valueOf(queueId))) {
				if (monitor != null)
					monitor.incrementCounter("jobToken_db_delete");

				exists = false;
				return true;
			}
		}
		finally {
			db.close();
		}

		return false;
	}
}
