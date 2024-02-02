package alien.optimizers.sync;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import alien.optimizers.DBSyncUtils;
import alien.optimizers.Optimizer;
import alien.optimizers.priority.PriorityReconciliationService;
import alien.taskQueue.JobStatus;
import alien.taskQueue.TaskQueueUtils;
import lazyj.DBFunctions;

import java.sql.Connection;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * @author Jorn-Are Flaten
 * @since 2024-01-15
 */
public class CheckJobStatus extends Optimizer {
	static final Logger logger = ConfigUtils.getLogger(PriorityReconciliationService.class.getCanonicalName());

	static final Monitor monitor = MonitorFactory.getMonitor(PriorityReconciliationService.class.getCanonicalName());

	static int counter = 0;

	@Override
	public void run() {
		logger.log(Level.INFO, "CheckJobStatus starting");
		this.setSleepPeriod(Duration.ofMinutes(10).toMillis());
		int frequency = (int) this.getSleepPeriod();

		while (true) {
			try {
				if (DBSyncUtils.updatePeriodic(frequency, CheckJobStatus.class.getCanonicalName(), this)) {
					startCron();
					logger.log(Level.INFO, "CheckJobStatus sleeping for " + this.getSleepPeriod() + " ms");
					sleep(this.getSleepPeriod());
				}
			}
			catch (Exception e) {
				try {
					logger.log(Level.SEVERE, "Exception executing optimizer", e);
					DBSyncUtils.registerException(CheckJobStatus.class.getCanonicalName(), e);
				}
				catch (Exception e2) {
					logger.log(Level.SEVERE, "Cannot register exception in the database", e2);
				}
			}

			try {
				logger.log(Level.INFO, "CheckJobStatus sleeping for " + this.getSleepPeriod() + " ms");
				sleep(this.getSleepPeriod());
			}
			catch (InterruptedException e) {
				logger.log(Level.SEVERE, "CheckJobStatus interrupted", e);
			}
		}
	}

	private static void startCron() {
		try (DBFunctions db = TaskQueueUtils.getQueueDB(); DBFunctions dbdev = TaskQueueUtils.getProcessesDevDB(); Timing t = new Timing(monitor, "CheckJobStatus")) {
			if (db == null) {
				logger.log(Level.SEVERE, "CheckJobStatus could not get a DB connection");
				return;
			}

			db.setQueryTimeout(60);

			if (dbdev == null) {
				logger.log(Level.INFO, "CheckJobStatus(processesDev) could not get a DB connection");
				return;
			}
			dbdev.setQueryTimeout(60);

			t.startTiming();
			db.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
			Set<Long> masterJobInRunningState = getMasterJobIds(db, getQueryToFindMasterjobsInRunningState());
			Set<Long> masterJobInFinalState = getMasterJobIds(db, getQueryToFindMasterjobsInFinalState());
			StringBuilder registerLog = new StringBuilder("Number of masterJobs in runningstate: " + masterJobInRunningState.size() + "\n");
			registerLog.append("Number of masterJobs in finalstate: " + masterJobInFinalState.size() + "\n");

			updateRunningJobs(dbdev, masterJobInRunningState, registerLog);
			updateFinalJobs(dbdev, masterJobInFinalState, registerLog);

			t.endTiming();
			logger.log(Level.INFO, "CheckJobStatus executed in: " + t.getMillis());
			registerLog.append("CheckJobStatus executed in: ")
					.append(t.getMillis())
					.append("\n");
			DBSyncUtils.registerLog(CheckJobStatus.class.getCanonicalName(), registerLog.toString());
		}

	}

	private static void updateFinalJobs(DBFunctions dbdev, Set<Long> masterJobInFinalState, StringBuilder registerLog) {
		String updateFinalQuery = getUpdateFinalMasterJobsStatusQuery(masterJobInFinalState);
		if (!updateFinalQuery.isEmpty()) {
			registerLog.append("Updating final jobs: ").append(updateFinalQuery).append("\n");
			boolean res = dbdev.query(updateFinalQuery);
			if (res)
				registerLog.append("Final Master job status changes written to database for ")
						.append(counter)
						.append(" masterjobs\n");
		}
		else {
			registerLog.append("No final master jobs to update\n");
		}
	}

	private static void updateRunningJobs(DBFunctions dbdev, Set<Long> masterJobInRunningState, StringBuilder registerLog) {
		String updateRunningQuery = getUpdateRunningMasterJobsStatusQuery(masterJobInRunningState);
		if (!updateRunningQuery.isEmpty()) {
			registerLog.append("Updating running jobs: ").append(updateRunningQuery).append("\n");
			boolean res = dbdev.query(updateRunningQuery);
			if (res)
				registerLog.append("Final Master job status changes written to database for ")
						.append(counter)
						.append(" masterjobs\n");
		}
		else {
			registerLog.append("No running master jobs to update\n");
		}
	}

	private static String getUpdateRunningMasterJobsStatusQuery(Set<Long> runningMasterJobs) {
		if (runningMasterJobs.isEmpty()) {
			return "No running master jobs to update";
		}

		StringBuilder updateQuery = new StringBuilder("UPDATE QUEUE SET statusId = " + JobStatus.DONE.getAliEnLevel() + " WHERE queueId IN ( ");
		updateQuery.append(runningMasterJobs.stream().map(String::valueOf).collect(Collectors.joining(",")));
		updateQuery.deleteCharAt(updateQuery.length() - 1);
		updateQuery.append(");");

		logger.log(Level.INFO, "q: " + updateQuery.toString());
		logger.log(Level.INFO, "Running master jobs status update prepared: " + runningMasterJobs.size());
		return updateQuery.toString();
	}

	private static String getUpdateFinalMasterJobsStatusQuery(Set<Long> finalMasterJobs) {
		if (finalMasterJobs.isEmpty()) {
			return "No final master jobs to update";
		}

		StringBuilder updateQuery = new StringBuilder("UPDATE QUEUE SET statusId = " + JobStatus.SPLIT.getAliEnLevel() + " WHERE queueId IN ( ");

		updateQuery.append(finalMasterJobs.stream().map(String::valueOf).collect(Collectors.joining(",")));
		updateQuery.deleteCharAt(updateQuery.length() - 1);
		updateQuery.append(");");

		logger.log(Level.INFO, "Final master jobs status update prepared: " + finalMasterJobs.size());
		return updateQuery.toString();
	}

	private static Set<Long> getMasterJobIds(DBFunctions db, String query) {
		Set<Long> masterJobIds = new HashSet<>();
		db.query(query);
		while (db.moveNext()) {
			masterJobIds.add(Long.valueOf(db.getl("masterjobid")));
		}

		return masterJobIds;
	}

	private static String getQueryToFindMasterjobsInRunningState() {
		return "SELECT masterjobid FROM\n" +
				"    (SELECT\n" +
				"         qmaster.queueId AS masterjobid,\n" +
				"         count(NULLIF(NULLIF(qsubjob.statusId," + JobStatus.DONE.getAliEnLevel() + "), " + JobStatus.DONE_WARN.getAliEnLevel() + ")) AS active_subjobs\n" +
				"     FROM\n" +
				"         QUEUE qmaster JOIN QUEUE qsubjob ON qsubjob.split=qmaster.queueId\n" +
				"     WHERE\n" +
				"         qmaster.statusId=3\n" +
				"     GROUP BY qmaster.queueId) x\n" +
				"WHERE active_subjobs=0;";
	}

	private static String getQueryToFindMasterjobsInFinalState() {
		String runningStates = JobStatus.INSERTING.getAliEnLevel() + ","
				+ JobStatus.WAITING.getAliEnLevel() + ","
				+ JobStatus.ASSIGNED.getAliEnLevel() + ","
				+ JobStatus.STARTED.getAliEnLevel() + ","
				+ JobStatus.RUNNING.getAliEnLevel() + ","
				+ JobStatus.SAVING.getAliEnLevel() + ","
				+ JobStatus.SAVED.getAliEnLevel();
		return "SELECT qmaster.queueId AS masterjobid FROM\n" +
				" QUEUE qmaster JOIN QUEUE qsubjob ON qsubjob.split=qmaster.queueId\n" +
				" WHERE qmaster.statusId in (" + JobStatus.DONE.getAliEnLevel() + "," + JobStatus.DONE_WARN.getAliEnLevel() + ") AND\n" +
				" qsubjob.statusId IN (" + runningStates + ")\n" +
				"GROUP BY qmaster.queueId;";
	}
}
