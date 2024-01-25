package alien.optimizers.state;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import alien.optimizers.Optimizer;
import alien.optimizers.priority.PriorityRapidUpdater;
import alien.taskQueue.JobStatus;
import alien.taskQueue.TaskQueueUtils;
import lazyj.DBFunctions;

import java.time.Duration;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Jørn-Are Flaten
 * @since 2024-01-24
 */
public class OverwaitingJobHandler extends Optimizer {

    static final Logger logger = ConfigUtils.getLogger(PriorityRapidUpdater.class.getCanonicalName());

    static final Monitor monitor = MonitorFactory.getMonitor(PriorityRapidUpdater.class.getCanonicalName());

    @Override
    public void run() {
        this.setSleepPeriod(Duration.ofHours(6).toMillis());
        int frequency = (int) this.getSleepPeriod();
        while (true) {
//            boolean updated = DBSyncUtils.updatePeriodic(frequency, OverwaitingJobHandler.class.getCanonicalName());
            boolean updated = true;
            if (updated) {
                try {
                    startCron();
                    logger.log(Level.INFO, "OverwaitingJobHandler sleeping for " + this.getSleepPeriod() + " ms");
                    sleep(this.getSleepPeriod());
                } catch (InterruptedException e) {
                    logger.log(Level.WARNING, "OverwaitingJobHandler interrupted", e);
                }
            }
        }
    }

    private void startCron() {
        logger.log(Level.INFO, "OverwaitingJobHandler starting");
        StringBuilder registerlog = new StringBuilder();
        try(Timing t1 = new Timing(monitor, "OverwaitingJobHandler")) {
            DBFunctions db = TaskQueueUtils.getQueueDB();
            if (db == null) {
                logger.log(Level.SEVERE, "OverwaitingJobHandler could not get a DB connection");
                return;
            }
            t1.startTiming();
            db.setReadOnly(false);
            boolean query = db.query(getQuery());
            if (query) {
                logger.log(Level.INFO, "OverwaitingJobHandler finished and updated" + db.getUpdateCount() + " rows");
                registerlog.append("OverwaitingJobHandler finished and updated ").append(db.getUpdateCount()).append(" rows");
            } else {
                logger.log(Level.INFO, "OverwaitingJobHandler failed");
            }
            t1.endTiming();
            logger.log(Level.INFO, "OverwaitingJobHandler took " + t1.getMillis() + " ms");
            registerlog.append("OverwaitingJobHandler took ").append(t1.getMillis()).append(" ms");

        }
    }

    private String getQuery() {
        StringBuilder q = new StringBuilder("UPDATE QUEUE SET statusId = ")
                .append(JobStatus.OVER_WAITING.getAliEnLevel())
                .append(" WHERE statusId = ")
                .append(JobStatus.WAITING.getAliEnLevel())
                .append(" AND (UNIX_TIMESTAMP() - UNIX_TIMESTAMP(mtime)) > ")
                .append(3600 * 24 * 3); // 72 hours


        return q.toString();
    }
}
