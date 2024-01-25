package alien.optimizers.state;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import alien.optimizers.DBSyncUtils;
import alien.optimizers.Optimizer;
import alien.optimizers.priority.JobAgentUpdater;
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
/**
 * @author Jørn-Are Flaten
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
            boolean updated = DBSyncUtils.updatePeriodic(frequency, JobAgentUpdater.class.getCanonicalName());
            if (updated) {
                try {
                    startCron();
                    logger.log(Level.INFO, "CheckJobStatus sleeping for " + this.getSleepPeriod() + " ms");
                    sleep(this.getSleepPeriod());
                } catch (InterruptedException e) {
                    logger.log(Level.SEVERE, "CheckJobStatus interrupted", e);
                }
            }
        }
    }

    private void startCron() {
        DBFunctions db = TaskQueueUtils.getQueueDB();
        if (db == null) {
            logger.log(Level.SEVERE, "CheckJobStatus could not get a DB connection");
            return;
        }

        db.setQueryTimeout(60);

        DBFunctions dbdev = TaskQueueUtils.getProcessesDevDB();
        if (dbdev == null) {
            logger.log(Level.INFO, "CheckJobStatus(processesDev) could not get a DB connection");
            return;
        }
        dbdev.setQueryTimeout(60);

        try (Timing t = new Timing(monitor, "CheckJobStatus")) {
            t.startTiming();
            db.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            Set<Long> masterJobInRunningState = getMasterJobIds(db, getQueryToFindMasterjobsInRunningState());
            Set<Long> masterJobInFinalState = getMasterJobIds(db, getQueryToFindMasterjobsInFinalState());
            StringBuilder registerLog = new StringBuilder("Number of masterJobs in runningstate: " + masterJobInRunningState.size() + "\n");
            registerLog.append("Number of masterJobs in finalstate: " + masterJobInFinalState.size() + "\n");
            String query = synchronizeMasterJobStatusWithSubjobs(masterJobInRunningState, masterJobInFinalState);

            logger.log(Level.INFO, "After synchronizeMasterJobStatusWithSubjobs, before DB write " + query);
            if (!query.isEmpty()) {
                boolean result = writeToDb(query, db, dbdev);
                logger.log(Level.INFO, "Writing masterjob status update to DB result: " + result);
                if (result) {
                    registerLog.append("Master job status changes written to database for ")
                            .append(counter)
                            .append(" masterjobs\n");
                }
            } else {
                logger.log(Level.INFO, "No masterjobs to update");
            }
            t.endTiming();
            logger.log(Level.INFO, "CheckJobStatus executed in: " + t.getMillis());
            registerLog.append("CheckJobStatus executed in: ")
                    .append(t.getMillis())
                    .append("\n");
            DBSyncUtils.registerLog(CheckJobStatus.class.getCanonicalName(), registerLog.toString());
        }

    }

    private boolean writeToDb(String query, DBFunctions db, DBFunctions dbdev) {
        return dbdev.query(query);
    }


    private String synchronizeMasterJobStatusWithSubjobs(Set<Long> runningMasterjobs, Set<Long> finalMasterJobs) {
        logger.log(Level.INFO, "synchronizing master and subjobs... ");
        StringBuilder updateQuery = new StringBuilder("INSERT INTO QUEUE (queueId, statusId) VALUES ");


        if (runningMasterjobs.isEmpty() && finalMasterJobs.isEmpty()) {
            logger.log(Level.INFO, "No masterjobs in running or final state");
            return "";
        }

        if (!runningMasterjobs.isEmpty()) {
            for (Long masterjobId : runningMasterjobs) {
                appendUpdateQuery(updateQuery, masterjobId, JobStatus.DONE.getAliEnLevel());
            }
        }

        if (!finalMasterJobs.isEmpty()) {
            for (Long masterjobId : finalMasterJobs) {
                appendUpdateQuery(updateQuery, masterjobId, JobStatus.SPLIT.getAliEnLevel());
            }
        }

        updateQuery.deleteCharAt(updateQuery.length() - 2).append("ON DUPLICATE KEY UPDATE statusId = VALUES(statusId)");

        logger.log(Level.INFO, "job status updates prepared: " + counter);
        logger.log(Level.INFO, "runningMasterJobs size: " + runningMasterjobs.size());
        logger.log(Level.INFO, "finalMasterJobs size: " + finalMasterJobs.size());
        return updateQuery.toString();
    }


    private void appendUpdateQuery(StringBuilder updateQuery, Long masterJobId, int statusId) {
        updateQuery
                .append("(")
                .append(masterJobId)
                .append(", ")
                .append(statusId)
                .append("), ");
        counter++;
    }

    private Set<Long> getMasterJobIds(DBFunctions db, String query) {
        Set<Long> masterJobIds = new HashSet<>();
        db.query(query);
        while (db.moveNext()) {
            masterJobIds.add(db.getl("masterjobid"));
        }

        return masterJobIds;
    }

    private String getQueryToFindMasterjobsInRunningState() {
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

    private String getQueryToFindMasterjobsInFinalState() {
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
