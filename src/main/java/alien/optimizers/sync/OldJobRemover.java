package alien.optimizers.sync;

import java.sql.Connection;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import alien.optimizers.DBSyncUtils;
import alien.optimizers.Optimizer;
import alien.optimizers.priority.PriorityRapidUpdater;
import alien.taskQueue.JobStatus;
import alien.taskQueue.TaskQueueUtils;
import lazyj.DBFunctions;

/**
 * @author Jorn-Are Flaten
 * @since 2024-01-22
 */
public class OldJobRemover extends Optimizer {
    static final Logger logger = ConfigUtils.getLogger(PriorityRapidUpdater.class.getCanonicalName());

    static final Monitor monitor = MonitorFactory.getMonitor(PriorityRapidUpdater.class.getCanonicalName());

    @Override
    public void run() {
        this.setSleepPeriod(Duration.ofMinutes(120).toMillis()); // 120 minutes
        int frequency = (int) this.getSleepPeriod();

        while (true) {
            try {
                if (DBSyncUtils.updatePeriodic(frequency, OldJobRemover.class.getCanonicalName(), this)) {
                    startCron();
                }
            } catch (Exception e) {
                try {
                    logger.log(Level.SEVERE, "Exception executing optimizer", e);
                    DBSyncUtils.registerException(OldJobRemover.class.getCanonicalName(), e);
                } catch (Exception e2) {
                    logger.log(Level.SEVERE, "Cannot register exception in the database", e2);
                }
            }

            try {
                logger.log(Level.INFO, "OldJobRemover sleeping for " + this.getSleepPeriod() + " ms");
                sleep(this.getSleepPeriod());
            } catch (InterruptedException e) {
                logger.log(Level.SEVERE, "OldJobRemover interrupted", e);
            }
        }
    }

    private static void startCron() {
        logger.log(Level.INFO, "OldJobRemover starting");
        try (DBFunctions db = TaskQueueUtils.getQueueDB();
             DBFunctions dbdev = TaskQueueUtils.getProcessesDevDB();
             Timing t0 = new Timing(monitor, "OldJobRemover");
             Timing t1 = new Timing(monitor, "OldJobRemover select");
             Timing t2 = new Timing(monitor, "OldJobRemover getDeleteQuery");
             Timing t3 = new Timing(monitor, "OldJobRemover deleteOldJobs")) {
            if (db == null) {
                logger.log(Level.SEVERE, "OldJobRemover could not get a DB connection");
                return;
            }
            db.setQueryTimeout(60);

            // development DB

            if (dbdev == null) {
                logger.log(Level.SEVERE, "OldJobRemover(processesDev) could not get a DB connection");
                return;
            }
            dbdev.setQueryTimeout(60);
            logger.log(Level.INFO, "DB Connections established");

            t0.startTiming();

            db.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            StringBuilder registerLog = new StringBuilder();

            t1.startTiming();
            Set<Long> oldJobs = getOldJobs(db, getOldJobsQuery());
            logger.log(Level.INFO, "OldJobs size: " + oldJobs.size());
            registerLog.append("Number of old master or single jobs: ")
                    .append(oldJobs.size())
                    .append("\n");
            t1.endTiming();
            logger.log(Level.INFO, "OldJobRemover select done in: " + t1.getMillis() + " ms");

            t2.startTiming();
            String deleteQuery = getDeleteQuery(oldJobs);
            t2.endTiming();
            logger.log(Level.INFO, "OldJobRemover getDeleteQuery done in: " + t2.getMillis() + " ms");

            if (deleteQuery.isEmpty()) {
                logger.log(Level.INFO, "No old jobs to delete");
                registerLog.append("No old jobs to delete\n");
            } else {
                registerLog.append("OldJobRemover delete query included the following job IDs: ")
                        .append(deleteQuery)
                        .append("\n");
                t3.startTiming();
                boolean res = dbdev.query(deleteQuery);

                if (res) {
                    int deleted = dbdev.getUpdateCount();
                    logger.log(Level.INFO, "Deleted " + deleted + " old jobs");
                    registerLog.append("Deleted ")
                            .append(deleted)
                            .append(" old jobs\n");
                } else {
                    logger.log(Level.INFO, "Old jobs not removed");
                }

                t3.endTiming();
                logger.log(Level.INFO, "OldJobRemover finished deleting jobs in: " + t3.getMillis() + " ms");
                registerLog.append("OldJobRemover finished deleting jobs in: ")
                        .append(t3.getMillis())
                        .append(" ms\n");
            }


            t0.endTiming();
            logger.log(Level.INFO, "OldJobRemover completed in: " + t0.getMillis() + " ms");
            registerLog.append("OldJobRemover completed in: ")
                    .append(t0.getMillis())
                    .append(" ms\n");
            DBSyncUtils.registerLog(OldJobRemover.class.getCanonicalName(), registerLog.toString());
        }

    }

    private static String getDeleteQuery(Set<Long> finalJobs) {
        if (finalJobs.isEmpty())
            return "";
        StringBuilder queueIds = new StringBuilder();
        for (Long id : finalJobs) {
            queueIds.append(id)
                    .append(",");
            TaskQueueUtils.putJobLog(id.longValue(), "OldJobRemover", "Job to be removed by OldJobRemover optimizer", null);
        }
        if (queueIds.length() > 0)
            queueIds.deleteCharAt(queueIds.length() - 1);

        String query = "DELETE FROM QUEUE WHERE queueId IN (" + queueIds + ") OR split IN (" + queueIds + ")";
        logger.log(Level.INFO, "OldJobRemover query: " + query);
        return query;
    }

    private static Set<Long> getOldJobs(DBFunctions db, String query) {
        Set<Long> oldJobs = new HashSet<>();
        boolean result = db.query(query);
        if (result) {
            while (db.moveNext()) {
                oldJobs.add(Long.valueOf(db.getl(1)));
            }
        }
        return oldJobs;
    }

    private static String getOldJobsQuery() {
        StringBuilder finalstates = new StringBuilder();
        JobStatus.finalStates().stream().map(JobStatus::getAliEnLevel).forEach(s -> finalstates.append(s).append(","));
        finalstates.deleteCharAt(finalstates.length() - 1);

        StringBuilder q = new StringBuilder("SELECT qs.queueId FROM QUEUE qs\n" +
                "WHERE qs.split = 0\n" +
                "  AND qs.mtime < date_sub(now(), interval 5 day)\n" +
                "  AND qs.statusId IN (")
                .append(finalstates)
                .append(")");

        logger.log(Level.INFO, "OldJobRemover query: " + q);
        return q.toString();
    }
}
