package alien.optimizers.priority;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import alien.optimizers.DBSyncUtils;
import alien.optimizers.Optimizer;
import alien.taskQueue.TaskQueueUtils;
import lazyj.DBFunctions;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Jørn-Are Flaten
 * @since 2023-12-08
 */
public class JobAgentUpdater extends Optimizer {
    /**
     * Logger
     */
    static final Logger logger = ConfigUtils.getLogger(PriorityReconciliationService.class.getCanonicalName());

    /**
     * Monitoring component
     */
    static final Monitor monitor = MonitorFactory.getMonitor(PriorityReconciliationService.class.getCanonicalName());

    @Override
    public void run() {
        logger.log(Level.INFO, "JobAgentUpdater starting");
        this.setSleepPeriod(60 * 5 * 1000); // 5m
        int frequency = (int) this.getSleepPeriod();

        while (true) {
            final boolean updated = DBSyncUtils.updatePeriodic(frequency, JobAgentUpdater.class.getCanonicalName());
            if (updated) {
                try {
                    updateComputedPriority();
                    logger.log(Level.INFO, "JobAgentUpdater sleeping for " + this.getSleepPeriod() + " ms");
                    sleep(this.getSleepPeriod());
                } catch (InterruptedException e) {
                    logger.log(Level.SEVERE, "JobAgentUpdater interrupted", e);
                }

            }
        }
    }

    private void updateComputedPriority() {
        DBFunctions db = TaskQueueUtils.getQueueDB();
        if (db == null) {
            logger.log(Level.SEVERE, "JobAgentUpdater could not get a DB connection");
            return;
        }

        db.setQueryTimeout(60);

        DBFunctions dbdev = TaskQueueUtils.getProcessesDevDB();
        if (dbdev == null) {
            logger.log(Level.INFO, "JobAgentUpdater(processesDev) could not get a DB connection");
            return;
        }
        dbdev.setQueryTimeout(60);
        String pQuery = "SELECT userId, computedpriority from PRIORITY";
        String jQuery = "SELECT entryId, userId from JOBAGENT";


        try (Timing t = new Timing(monitor, "JobAgentUpdater")) {
            logger.log(Level.INFO, "JobAgentUpdater starting to update priority in JOBAGENT table");
            t.startTiming();
            StringBuilder sb = new StringBuilder("INSERT INTO JOBAGENT (entryId, userId, priority) VALUES ");
            Map<Integer, Double> priorityMap = new HashMap<>();

            db.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            db.query(pQuery);
            while (db.moveNext()) {
                int userId = db.geti("userId");
                double computedPriority = db.getd("computedpriority");
                priorityMap.put(userId, computedPriority);
            }

            boolean first = true;
            db.query(jQuery);
            while (db.moveNext()) {
                int entryId = db.geti("entryId");
                int userId = db.geti("userId");

                if (first) {
                    first = false;
                } else {
                    sb.append(", ");
                }
                sb.append("(")
                        .append(entryId).append(", ")
                        .append(userId).append(", ")
                        .append(priorityMap.get(userId)).append(")");
            }

            sb.append(" ON DUPLICATE KEY UPDATE userId = VALUES(userId), priority = VALUES(priority)");
            dbdev.query(sb.toString());

            t.endTiming();
            logger.log(Level.INFO, "JobAgentUpdater finished updating JOBAGENT table, took " + t.getMillis() + " ms");
            String registerLog = "Finished updating JOBAGENT table priority values, in " + t.getMillis() + " ms\n";
            DBSyncUtils.registerLog(JobAgentUpdater.class.getCanonicalName(), registerLog);
        }
    }

}
