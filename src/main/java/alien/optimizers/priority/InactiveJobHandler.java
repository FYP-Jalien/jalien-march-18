package alien.optimizers.priority;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import alien.optimizers.DBSyncUtils;
import alien.optimizers.Optimizer;
import alien.taskQueue.JobStatus;
import alien.taskQueue.TaskQueueUtils;
import lazyj.DBFunctions;

import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Jorn-Are Flaten
 * @since 2023-12-08
 */
public class InactiveJobHandler extends Optimizer {
	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(InactiveJobHandler.class.getCanonicalName());

	/**
	 * Monitoring component
	 */
	static final Monitor monitor = MonitorFactory.getMonitor(InactiveJobHandler.class.getCanonicalName());

	@Override
	public void run() {
		logger.log(Level.INFO, "InactiveJobHandler starting");
		this.setSleepPeriod(60 * 5 * 1000); // 5m
		int frequency = (int) this.getSleepPeriod();

		while (true) {
			try {
				if (DBSyncUtils.updatePeriodic(frequency, InactiveJobHandler.class.getCanonicalName(), this)) {
					moveInactiveJobStates();
					logger.log(Level.INFO, "InactiveJobHandler sleeping for " + this.getSleepPeriod() + " ms");
					sleep(this.getSleepPeriod());
				}
			}
			catch (Exception e) {
				try {
					logger.log(Level.SEVERE, "Exception executing optimizer", e);
					DBSyncUtils.registerException(InactiveJobHandler.class.getCanonicalName(), e);
				}
				catch (Exception e2) {
					logger.log(Level.SEVERE, "Cannot register exception in the database", e2);
				}
			}

			try {
				sleep(this.getSleepPeriod());
			}
			catch (InterruptedException e) {
				logger.log(Level.SEVERE, "InactiveJobHandler interrupted", e);
			}
		}

	}

	private static void moveInactiveJobStates() {
		try (DBFunctions db = TaskQueueUtils.getQueueDB()) {
			if (db == null) {
				logger.log(Level.SEVERE, "InactiveJobHandler could not get a DB connection");
				return;
			}

			db.setQueryTimeout(60);

			db.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
			String activeJobWithoutHeartbeatQuery = getActiveJobQuery();

			String inactiveJobsWithoutHeartbeatQuery = "SELECT q.queueId, q.statusId FROM QUEUE q JOIN QUEUEPROC qp\n" +
					"                                           WHERE q.queueId = qp.queueId\n" +
					"                                               AND  q.statusId IN (" + JobStatus.ZOMBIE.getAliEnLevel() + ")\n" +
					"                                               AND qp.lastupdate < NOW() - INTERVAL 2 HOUR";

			try (Timing t = new Timing(monitor, "InactiveJobHandler")) {
				t.startTiming();

				StringBuilder registerLog = new StringBuilder();
				logger.log(Level.INFO, "InactiveJobHandler starting to move inactive jobs to zombie state. ");
				moveState(db, activeJobWithoutHeartbeatQuery, JobStatus.ZOMBIE, registerLog);

				logger.log(Level.INFO, "InactiveJobHandler starting to move 2h inactive zombie state jobs to expired state. ");
				moveState(db, inactiveJobsWithoutHeartbeatQuery, JobStatus.EXPIRED, registerLog);

				t.endTiming();
				logger.log(Level.INFO, "InactiveJobHandler finished in " + t.getMillis() + " ms");
				registerLog.append("Moving inactive job states took: ").append(t.getMillis()).append(" ms");
				DBSyncUtils.registerLog(InactiveJobHandler.class.getCanonicalName(), registerLog.toString());
			}
			catch (Exception e) {
				DBSyncUtils.registerLog(InactiveJobHandler.class.getCanonicalName(), "Exception executing: " + e.getMessage());
				logger.log(Level.SEVERE, "InactiveJobHandler: Exception", e);
			}
		}
	}

	private static String getActiveJobQuery() {
		String activeStates = JobStatus.RUNNING.getAliEnLevel() + ","
				+ JobStatus.STARTED.getAliEnLevel() + ","
				+ JobStatus.SAVING.getAliEnLevel() + ","
				+ JobStatus.ASSIGNED.getAliEnLevel();
		return "SELECT q.queueId, q.statusId FROM QUEUE q JOIN QUEUEPROC qp\n" +
				"                                            WHERE q.queueId = qp.queueId\n" +
				"                                              AND  q.statusId IN (" + activeStates + ")\n" +
				"                                              AND qp.lastupdate < NOW() - INTERVAL 1 HOUR";
	}

	private static void moveState(final DBFunctions db, final String query, final JobStatus status, final StringBuilder log) {
		if (!db.query(query)) {
			logger.log(Level.SEVERE, "Failed to execute selection query `" + query + "`");
			return;
		}

		int okcounter = 0;
		int failcounter = 0;

		while (db.moveNext()) {
			if (TaskQueueUtils.setJobStatus(db.getl("queueId"), status, JobStatus.getStatusByAlien(Integer.valueOf(db.geti("statusId")))))
				okcounter++;
			else
				failcounter++;
		}

		logger.log(Level.INFO, "Moved " + okcounter + " jobs to " + status + " state" + (failcounter > 0 ? ", " + failcounter + " others failed to be moved. " : ""));
		log.append("Moved ").append(okcounter).append(" jobs to ").append(status).append(" state, while ").append(failcounter + " others failed to be moved. \n");
	}
}
