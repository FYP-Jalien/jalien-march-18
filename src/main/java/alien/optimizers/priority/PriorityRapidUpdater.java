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

import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Jorn-Are Flaten
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
            // Ignore the returned value, each server has to flush at the same frequency, independently of each other.
            // It is called in the loop just to feed back the frequency from the database, in case we tune it != default value
            DBSyncUtils.updatePeriodic((int) getSleepPeriod(), PriorityRapidUpdater.class.getCanonicalName(), this);

            try {
                updatePriority();
            } catch (Exception e) {
                try {
                    logger.log(Level.SEVERE, "Exception executing optimizer", e);
                    DBSyncUtils.registerException(PriorityRapidUpdater.class.getCanonicalName(), e);
                } catch (Exception e2) {
                    logger.log(Level.SEVERE, "Cannot register exception in the database", e2);
                }
            }

            try {
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
    public static void updatePriority() {
        try (DBFunctions db = TaskQueueUtils.getQueueDB(); DBFunctions dbdev = TaskQueueUtils.getProcessesDevDB()) {
            if (db == null) {
                logger.log(Level.SEVERE, "PriorityRapidUpdater could not get a DB connection");
                return;
            }
            // development DB

            if (dbdev == null) {
                logger.log(Level.SEVERE, "PriorityRapidUpdater(processesDev) could not get a DB connection");
                return;
            }

            db.setQueryTimeout(60);
            dbdev.setQueryTimeout(60);
            logger.log(Level.INFO, "DB Connections established");

            Map<Integer, PriorityRegister.JobCounter> registrySnapshot = PriorityRegister.JobCounter.getRegistrySnapshot();

            try (Timing t = new Timing(monitor, "TQ_updatePriority_ms")) {
                StringBuilder registerLog = new StringBuilder("PriorityRegister.JobCounter.getRegistry() size: " + registrySnapshot.size() + "\n");
                if (!PriorityRegister.JobCounter.getRegistry().isEmpty()) {
                    t.startTiming();
                    AtomicInteger count = new AtomicInteger();
                    int registrySize = PriorityRegister.JobCounter.getRegistry().size();
                    logger.log(Level.INFO, "Preparing to update PRIORITY for active users. Total registry size is: " + registrySize);
                    registerLog.append("Preparing to update PRIORITY for active users. Total registry size is:")
                            .append(registrySize)
                            .append("\n");

                    try (Timing t2 = new Timing(monitor, "TQ_updatePriority_db_ms")) {
                        PriorityRegister.JobCounter.getRegistry().forEach((userId, v) -> {
                            if (isUserInactive(v)) {
                                logger.log(Level.INFO, "Removing inactive user from registry: " + userId);
                                count.getAndIncrement();
                                PriorityRegister.JobCounter.getRegistry().remove(userId);
                            } else {
                                executeQueryAndUpdateUserCounter(userId, v, registrySnapshot, registerLog, dbdev);
                            }
                            t2.endTiming();
                            registerLog.append("Flushing values row by row to the database and subtracted ")
                                    .append(registrySize)
                                    .append(" counters completed successfully in ")
                                    .append(t2.getMillis())
                                    .append(" \n");
                        });
                    }

                    if (count.get() > 0) {
                        registerLog.append("Removed ")
                                .append(count.get())
                                .append(" inactive users from registry.\n");
                    }

                    t.endTiming();
                    logger.log(Level.INFO, "PriorityRapidUpdater used: " + t.getSeconds() + " seconds");
                    registerLog.append("PriorityRapidUpdater used: ")
                            .append(t.getSeconds())
                            .append(" seconds\n");

                } else {
                    logger.log(Level.INFO, "Counter registry is empty - nothing to update");
                    registerLog.append(" Counter registry is empty - nothing to update\n");
                }

                DBSyncUtils.updateManual(PriorityRapidUpdater.class.getCanonicalName(), registerLog.toString());
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "PriorityRapidUpdater failed", e);
        }
    }

    private static void executeQueryAndUpdateUserCounter(Integer userId, PriorityRegister.JobCounter v, Map<Integer, PriorityRegister.JobCounter> registrySnapshot, StringBuilder registerLog, DBFunctions dbdev) {
        Timing rtimer = new Timing(monitor, "TQ_single_row_update_ms");
        rtimer.startTiming();
        boolean res = dbdev.query(updateUserQuery(userId, v), false);
        rtimer.endTiming();
        logQueryTiming(rtimer, userId, registerLog);

        if (res) {
            PriorityRegister.JobCounter userCounter = registrySnapshot.get(userId);
            if (userCounter != null) {
                logAndSubtractUserCounter(userId, v, userCounter);
            }
        }
    }

    private static void logQueryTiming(Timing rtimer, Integer userId, StringBuilder registerLog) {
        logger.log(Level.INFO, "Flushing values to the database for user: " + userId + " completed in " + rtimer.getMillis() + " ms");
        registerLog.append("Flushing values to the database for user: ")
                .append(userId)
                .append(" completed in ")
                .append(rtimer.getMillis())
                .append(" ms\n");
    }

    private static void logAndSubtractUserCounter(Integer userId, PriorityRegister.JobCounter v, PriorityRegister.JobCounter userCounter) {
        logger.log(Level.INFO, "Registry values for user: " + userId
                + ", waiting: " + v.getWaiting()
                + ", running: " + v.getRunning()
                + ", cputime: " + v.getCputime()
                + ", cost: " + v.getCost());

        v.subtractValues(userCounter);

        logger.log(Level.INFO, "Subtracting snapshotted values for user: " + userId
                + ", waiting: " + userCounter.getWaiting()
                + ", running: " + userCounter.getRunning()
                + ", cputime: " + userCounter.getCputime()
                + ", cost: " + userCounter.getCost());

        logger.log(Level.INFO, "values after subtraction for " + userId
                + ", waiting: " + v.getWaiting()
                + ", running: " + v.getRunning()
                + ", cputime: " + v.getCputime()
                + ", cost: " + v.getCost());
    }

    private static String updateUserQuery(Integer userId, PriorityRegister.JobCounter v) {
        return "UPDATE PRIORITY SET waiting = waiting + " + v.getWaiting()
                + ", running = running + " + v.getRunning()
                + ", totalRunningTimeLast24h = totalRunningTimeLast24h + " + v.getCputime()
                + ", totalCpuCostLast24h = totalCpuCostLast24h + " + v.getCost()
                + ", active = 1"
                + " WHERE userId = " + userId;
    }

    private static boolean isUserInactive(PriorityRegister.JobCounter v) {
        return (v.getWaiting() == 0 && v.getRunning() == 0 && v.getCputime() == 0 && v.getCost() == 0);
    }
}