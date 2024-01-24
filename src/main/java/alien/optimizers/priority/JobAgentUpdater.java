package alien.optimizers.priority;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import alien.optimizers.DBSyncUtils;
import alien.optimizers.Optimizer;
import alien.taskQueue.TaskQueueUtils;
import lazyj.DBFunctions;

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

        String s = "UPDATE JOBAGENT INNER JOIN PRIORITY USING(userId) SET JOBAGENT.priority = PRIORITY.computedPriority";
        try (Timing t = new Timing(monitor, "JobAgentUpdater")) {
            logger.log(Level.INFO, "2-JobAgentUpdater starting to update priority in JOBAGENT table");
            t.startTiming();
            dbdev.query(s);
            t.endTiming();
            logger.log(Level.INFO, "JobAgentUpdater finished updating JOBAGENT table, took " + t.getMillis() + " ms");

            String registerLog = "Finished updating JOBAGENT table priority values, in " + t.getMillis() + " ms\n";
            DBSyncUtils.registerLog(JobAgentUpdater.class.getCanonicalName(), registerLog);
        }
    }

}
