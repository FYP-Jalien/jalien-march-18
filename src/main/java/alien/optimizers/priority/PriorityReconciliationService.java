package alien.optimizers.priority;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import alien.optimizers.DBSyncUtils;
import alien.optimizers.Optimizer;
import alien.priority.CalculateComputedPriority;
import alien.priority.QueueProcessingDto;
import alien.taskQueue.JobStatus;
import alien.taskQueue.TaskQueueUtils;
import lazyj.DBFunctions;

import java.sql.Connection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Jørn-Are Flaten
 * @since 2023-11-23
 */
public class PriorityReconciliationService extends Optimizer {
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
        this.setSleepPeriod(3600 * 1000); // 1h
        int frequency = (int) this.getSleepPeriod();

        while (true) {
            final boolean updated = DBSyncUtils.updatePeriodic(frequency, PriorityReconciliationService.class.getCanonicalName());
            try {
                if (updated) {
                    reconcilePriority();

                    logger.log(Level.INFO, "ReconcilePriority sleeps " + this.getSleepPeriod() + " ms");
                    sleep(this.getSleepPeriod());
                } else {
                    sleep(this.getSleepPeriod());

                }
            } catch (InterruptedException e) {
                logger.log(Level.WARNING, "PriorityReconciliationService interrupted", e);
            }
        }

    }

    private void reconcilePriority() {
        DBFunctions db = TaskQueueUtils.getQueueDB();
        if (db == null) {
            logger.log(Level.SEVERE, "ReconcilePriority could not get a DB connection");
            return;
        }

        db.setQueryTimeout(60);

        DBFunctions dbdev = TaskQueueUtils.getProcessesDevDB();
        if (dbdev == null) {
            logger.log(Level.SEVERE, "ReconcilePriority(processesDev) could not get a DB connection");
            return;
        }

        Set<String> uniqueStates = Stream.of(JobStatus.runningStates(), JobStatus.errorneousStates(), JobStatus.finalStates())
                .flatMap(Set::stream)
                .map(JobStatus::getAliEnLevel)
                .map(String::valueOf)
                .collect(Collectors.toSet());
        String states = String.join(", ", uniqueStates);

        String findActiveUsersQuery = "SELECT q.userId, p.cost, p.cputime FROM QUEUE q " +
                "join QUEUEPROC p on q.queueId = p.queueId " +
                "WHERE q.statusId IN (" + states + ") " +
                "AND p.lastupdate > NOW() - INTERVAL 1 DAY;";

        try (Timing t = new Timing(monitor, "TQ_reconcilePriority_ms")) {
            t.startTiming();
            logger.log(Level.INFO, "Retrieving active users");
            Timing t2 = new Timing(monitor, "TQ_reconcilePriority_db_ms");
            t2.startTiming();
            db.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            db.query(findActiveUsersQuery);
            t2.endTiming();
            logger.log(Level.INFO, "Retrieving active users took " + t2.getMillis() + " ms");
            StringBuilder registerLog = new StringBuilder("Retrieving active users in ").append(t2.getMillis()).append(" ms\n");


            Map<Integer, QueueProcessingDto> activeUsersGroupedById = new HashMap<>();
            while (db.moveNext()) {
                int userId = db.geti("userId");
                double cost = db.getd("cost");
                long cputime = db.getl("cputime");

                activeUsersGroupedById
                        .computeIfAbsent(
                                userId, k -> new QueueProcessingDto(
                                        userId))
                        .addAccounting(cost, cputime);
            }

            boolean first = true;
            if (!activeUsersGroupedById.isEmpty()) {
                logger.log(Level.INFO, "Updating priority for active users: " + activeUsersGroupedById.size());
                Timing t5 = new Timing(monitor, "TQ_update_active_ms");
                t5.startTiming();

                StringBuilder updateActiveUsersQuery = getUpdatePriorityQuery();
                for (QueueProcessingDto dto : activeUsersGroupedById.values()) {
                    if (first) {
                        first = false;
                    } else {
                        updateActiveUsersQuery.append(", ");
                    }
                    updateActiveUsersQuery.append("(").append(dto.getUserId()).append(", ").append(dto.getCost()).append(", ").append(dto.getCputime()).append(")");
                }

                final String onDuplicateKey = " ON DUPLICATE KEY UPDATE totalCpuCostLast24h = VALUES(totalCpuCostLast24h), totalRunningTimeLast24h = VALUES(totalRunningTimeLast24h)";
                updateActiveUsersQuery.append(onDuplicateKey);
                dbdev.query(updateActiveUsersQuery.toString(), false);

                t5.endTiming();
                registerLog.append("Updating priority for ")
                        .append(activeUsersGroupedById.size())
                        .append(" active users took ")
                        .append(t5.getMillis())
                        .append(" ms\n");
                logger.log(Level.INFO, "Updating priority for active users took " + t5.getMillis() + " ms");

            } else {
                logger.log(Level.INFO, "No active users to update");
            }

            String selectPriorityQuery = "select userid from PRIORITY where totalCpuCostLast24h > 0 OR totalRunningTimeLast24h > 0";

            logger.log(Level.INFO, "Retrieving all users");
            Timing t3 = new Timing(monitor, "TQ_reconcilePriority_db2_ms");
            t3.startTiming();
            db.query(selectPriorityQuery);
            t3.endTiming();
            logger.log(Level.INFO, "Retrieving all users took " + t3.getMillis() + " ms");

            Set<Integer> nonActiveUsersLast24H = new HashSet<>();

            logger.log(Level.INFO, "Filtering out non active users");
            Timing t6 = new Timing(monitor, "TQ_filter_non_active_ms");
            while (db.moveNext()) {
                int userId = db.geti("userId");
                if (!activeUsersGroupedById.containsKey(userId)) {
                    nonActiveUsersLast24H.add(userId);
                }

            }
            t6.endTiming();
            logger.log(Level.INFO, "Filtering out non active users took " + t6.getMillis() + " ms");


            Timing t4 = new Timing(monitor, "TQ_update_non_active_ms");
            t4.startTiming();
            if (!nonActiveUsersLast24H.isEmpty()) {
                logger.log(Level.INFO, "Updating priority for non active users " + nonActiveUsersLast24H.size());
                StringBuilder updateNonActiveUsersQuery = getUpdatePriorityQuery();
                first = true;
                for (Integer userId : nonActiveUsersLast24H) {
                    if (first) {
                        first = false;
                    } else {
                        updateNonActiveUsersQuery.append(", ");
                    }
                    updateNonActiveUsersQuery.append("(").append(userId).append(", ").append(0).append(", ").append(0).append(")");
                }

                updateNonActiveUsersQuery.append(" ON DUPLICATE KEY UPDATE totalCpuCostLast24h = VALUES(totalCpuCostLast24h), totalRunningTimeLast24h = VALUES(totalRunningTimeLast24h)");
                dbdev.query(updateNonActiveUsersQuery.toString(), false);

                registerLog.append(" Updating priority for ")
                        .append(nonActiveUsersLast24H.size())
                        .append(" non active users took ")
                        .append(t4.getMillis())
                        .append(" ms\n");
                logger.log(Level.INFO, "Updating priority for non active users took " + t4.getMillis() + " ms");
            } else {
                logger.log(Level.INFO, "No non active users to update");
            }

            t4.endTiming();
            t.endTiming();
            logger.log(Level.INFO, "ReconcilePriority finished after running for " + t.getMillis() + " ms");
            registerLog.append(" ReconcilePriority finished after running for ").append(t.getMillis()).append(" ms\n");
            DBSyncUtils.registerLog(PriorityReconciliationService.class.getCanonicalName(), registerLog.toString());

            CalculateComputedPriority.updateComputedPriority();

        } finally {
            db.close();
            dbdev.close();
        }
    }

    private StringBuilder getUpdatePriorityQuery() {
        return new StringBuilder("INSERT INTO PRIORITY (userId, totalCpuCostLast24h, totalRunningTimeLast24h) VALUES ");
    }

}
