package alien.optimizers.priority;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import alien.optimizers.DBSyncUtils;
import alien.optimizers.Optimizer;
import alien.priority.CalculateComputedPriority;
import alien.priority.PriorityRegister;
import alien.taskQueue.TaskQueueUtils;
import lazyj.DBFunctions;

import java.util.concurrent.atomic.AtomicInteger;
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
                StringBuilder registerLog = new StringBuilder("PriorityRegister.JobCounter.getRegistry() size: " + PriorityRegister.JobCounter.getRegistry().size() + "\n");
                if (!PriorityRegister.JobCounter.getRegistry().isEmpty()) {
                    t.startTiming();
                    AtomicInteger count = new AtomicInteger();
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
                            count.getAndIncrement();
                            PriorityRegister.JobCounter.getRegistry().remove(userId);
                        }
                    });
                    if (count.get() > 0) {
                        registerLog.append("Removed ")
                                .append(count.get())
                                .append(" inactive users from registry.\n");
                    }

                    sb.append(" ON DUPLICATE KEY UPDATE ")
                            .append("waiting = waiting + VALUES(waiting), ")
                            .append("running = running + VALUES(running), ")
                            .append("totalRunningTimeLast24h = totalRunningTimeLast24h + VALUES(totalRunningTimeLast24h), ")
                            .append("totalCpuCostLast24h = totalCpuCostLast24h + VALUES(totalCpuCostLast24h)");

                    logger.log(Level.INFO, "Updating priority for active users: " + PriorityRegister.JobCounter.getRegistry().size());
                    registerLog.append("Updating PRIORITY table for active users: ")
                            .append(PriorityRegister.JobCounter.getRegistry().size())
                            .append("\n");
                    dbdev.query(sb.toString(), false);

                    // Reset counters after successful update
                    PriorityRegister.JobCounter.getRegistry().forEach((userId, v) -> {
                        PriorityRegister.JobCounter.resetUserCounters(userId);
                    });

                    t.endTiming();
                    logger.log(Level.INFO, "PriorityRapidUpdater used: " + t.getSeconds() + " seconds");
                    registerLog.append("PriorityRapidUpdater used: ")
                            .append(t.getSeconds())
                            .append(" seconds\n");

                    CalculateComputedPriority.updateComputedPriority();
                } else {
                    logger.log(Level.INFO, "Counter registry is empty - nothing to update");
                    registerLog.append(" Counter registry is empty - nothing to update\n");
                }

                DBSyncUtils.registerLog(PriorityRapidUpdater.class.getCanonicalName(), registerLog.toString());
            }

        } catch (Exception e) {
            logger.log(Level.SEVERE, "PriorityRapidUpdater failed", e);
        }
    }

}