package alien.priority;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import alien.optimizers.DBSyncUtils;
import alien.optimizers.Optimizer;
import alien.optimizers.priority.PriorityRapidUpdater;
import alien.priority.PriorityDto;
import alien.taskQueue.TaskQueueUtils;
import lazyj.DBFunctions;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Jørn-Are Flaten
 * @since 2023-12-04
 */
public class CalculateComputedPriority {
    /**
     * Logger
     */
    static final Logger logger = ConfigUtils.getLogger(PriorityRapidUpdater.class.getCanonicalName());

    /**
     * Monitoring component
     */
    static final Monitor monitor = MonitorFactory.getMonitor(PriorityRapidUpdater.class.getCanonicalName());

    public static void updateComputedPriority() {
        DBFunctions db = TaskQueueUtils.getQueueDB();
        if (db == null) {
            logger.log(Level.SEVERE, "CalculatePriority could not get a DB connection");
            return;
        }

        db.setQueryTimeout(60);

        DBFunctions dbdev = TaskQueueUtils.getProcessesDevDB();
        if (dbdev == null) {
            logger.log(Level.SEVERE, "CalculatePriority(processesDev) could not get a DB connection");
            return;
        }

        String q = "SELECT userId, priority, running, maxParallelJobs, totalRunningTimeLast24h, maxTotalRunningTime from PRIORITY";

        Map<Integer, PriorityDto> dtos = new HashMap<>();
        try (Timing t = new Timing(monitor, "calculateComputedPriority")) {
            t.startTiming();
            logger.log(Level.INFO, "Calculating computed priority");
            db.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            db.query(q);

            while (db.moveNext()) {
                int userId = db.geti("userId");
                dtos.computeIfAbsent(
                        userId,
                        k -> new PriorityDto(db)
                );

                updateComputedPriority(dtos.get(userId));
            }

            logger.log(Level.INFO, "Finished calculating, updating PRIORITY table");
            StringBuilder sb = new StringBuilder("INSERT INTO PRIORITY (userId, userload, computedPriority) VALUES ");
            logger.log(Level.INFO, "Elements in DTO map: " + dtos.size());

            boolean first = true;
            for (PriorityDto dto : dtos.values()) {
                if (!first) {
                    sb.append(", ");
                } else {
                    first = false;
                }

                sb.append("(")
                        .append(dto.getUserId()).append(", ")
                        .append(dto.getUserload()).append(", ")
                        .append(dto.getComputedPriority())
                        .append(")");
            }


            sb.append(" ON DUPLICATE KEY UPDATE ")
                    .append("userload = VALUES(userload), ")
                    .append("computedPriority = VALUES(computedPriority)");

            dbdev.query(sb.toString());
            t.endTiming();
            logger.log(Level.INFO, "Finished updating PRIORITY table, took " + t.getMillis() + " ms");
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Exception thrown while calculating computedPriority", e);
        }
    }

    private static void updateComputedPriority(PriorityDto dto) {
        int activeCpuCores = dto.getRunning();
        int maxCpuCores = dto.getMaxParallelJobs();
        long historicalUsage = dto.getTotalRunningTimeLast24h() / dto.getMaxTotalRunningTime();

        if (activeCpuCores < maxCpuCores) {
            double coreUsageCost = activeCpuCores == 0 ? 1 : (activeCpuCores * Math.exp(-historicalUsage));
            float userLoad = (float) activeCpuCores / maxCpuCores;
            dto.setUserload(userLoad);
            double adjustedPriorityFactor = (2.0 - userLoad) * (dto.getPriority() / coreUsageCost);

            if (adjustedPriorityFactor > 0) {
                dto.setComputedPriority((float) (50.0 * adjustedPriorityFactor));
            } else {
                dto.setComputedPriority(1);
            }
        } else {
            dto.setComputedPriority(1);
        }
    }
}



