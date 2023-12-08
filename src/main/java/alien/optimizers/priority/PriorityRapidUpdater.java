package alien.optimizers.priority;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import alien.optimizers.DBSyncUtils;
import alien.optimizers.Optimizer;
import alien.priority.PriorityRegister;
import alien.taskQueue.TaskQueueUtils;
import lazyj.DBFunctions;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Jørn-Are Flaten
 * @since 2023-11-22
 */
public class PriorityRapidUpdater extends Optimizer {

    /**
     * Logger
     */
    static final Logger logger = ConfigUtils.getLogger(PriorityRapidUpdater.class.getCanonicalName());

    /**
     * Monitoring component
     */
    static final Monitor monitor = MonitorFactory.getMonitor(PriorityRapidUpdater.class.getCanonicalName());


    @Override
    public void run() {
        this.setSleepPeriod(60 * 5 * 1000); // 5m

        while (true) {
                try {
                    updatePriority();
                    logger.log(Level.INFO, "PriorityRapidUpdater sleeping for " + this.getSleepPeriod() + " ms");
                    sleep(this.getSleepPeriod());
                } catch (InterruptedException e) {
                    logger.log(Level.WARNING, "PriorityRapidUpdater interrupted", e);
                }
        }
    }

    /**
     * Update PRIORITY table values to keep user information in sync
     */
    public void updatePriority() {
        try (DBFunctions db = TaskQueueUtils.getQueueDB()) {
            if (db == null) {
                logger.log(Level.SEVERE, "PriorityRapidUpdater could not get a DB connection");
                return;
            }

            db.setQueryTimeout(60);

            // development DB
            DBFunctions dbdev = TaskQueueUtils.getProcessesDevDB();
            if (dbdev == null) {
                logger.log(Level.SEVERE, "PriorityRapidUpdater(processesDev) could not get a DB connection");
                return;
            }
            dbdev.setQueryTimeout(60);


            logger.log(Level.INFO, "DB Connections established");
            logger.log(Level.INFO, "PriorityRegister.JobCounter.getRegistry().size(): " + PriorityRegister.JobCounter.getRegistry().size());

            StringBuilder sb = new StringBuilder("INSERT INTO PRIORITY (userId, waiting, running, totalRunningTimeLast24h, totalCpuCostLast24h) VALUES ");
            boolean[] isFirst = {true};

            try (Timing t = new Timing(monitor, "TQ_updatePriority_ms")) {
                if (!PriorityRegister.JobCounter.getRegistry().isEmpty()) {
                    t.startTiming();

                    PriorityRegister.JobCounter.getRegistry().forEach((userId, v) -> {
                        if (v.getWaiting() != 0 || v.getRunning() != 0 || v.getCputime() != 0 || v.getCost() != 0) {
                            if (!isFirst[0]) {
                                sb.append(", ");
                            } else {
                                isFirst[0] = false;
                            }
                            sb.append("(")
                                    .append(userId).append(", ")
                                    .append(v.getWaiting()).append(", ")
                                    .append(v.getRunning()).append(", ")
                                    .append(v.getCputime()).append(", ")
                                    .append(v.getCost())
                                    .append(")");
                        } else {
                            logger.log(Level.INFO, "Removing inactive user from registry: " + userId);
                            PriorityRegister.JobCounter.getRegistry().remove(userId);
                        }
                    });

                    sb.append(" ON DUPLICATE KEY UPDATE ")
                            .append("waiting = VALUES(waiting), ")
                            .append("running = VALUES(running), ")
                            .append("totalRunningTimeLast24h = VALUES(totalRunningTimeLast24h), ")
                            .append("totalCpuCostLast24h = VALUES(totalCpuCostLast24h)");

                    logger.log(Level.INFO, "Updating priority for active users: " + PriorityRegister.JobCounter.getRegistry().size());
                    dbdev.query(sb.toString(), false);

                    // Reset counters after successful update
                    PriorityRegister.JobCounter.getRegistry().forEach((userId, v) -> {
                        PriorityRegister.JobCounter.resetUserCounters(userId);
                    });

                    t.endTiming();
                    logger.log(Level.INFO, "PriorityRapidUpdater used: " + t.getSeconds() + " seconds");

                    CalculateComputedPriority.updateComputedPriority();
                } else {
                    logger.log(Level.INFO, "Counter registry is empty - nothing to update");
                }
            }

        } catch (Exception e) {
            logger.log(Level.SEVERE, "PriorityRapidUpdater failed", e);
        }
    }

}