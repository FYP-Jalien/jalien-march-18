package alien.optimizers.priority;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import alien.optimizers.DBSyncUtils;
import alien.optimizers.Optimizer;
import alien.taskQueue.JobStatus;
import alien.taskQueue.TaskQueueUtils;
import lazyj.DBFunctions;

import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;

public class InactiveJobHandler extends Optimizer {
    /**
     * Logger
     */
    static final Logger logger = ConfigUtils.getLogger(InactiveJobHandler.class.getCanonicalName());

    /**
     * Monitoring component
     */
    static final Monitor monitor = MonitorFactory.getMonitor(InactiveJobHandler.class.getCanonicalName());


    @Override
    public void run() {
        logger.log(Level.INFO, "InactiveJobHandler starting");
        this.setSleepPeriod(60 * 5 * 1000); // 5m
        int frequency = (int) this.getSleepPeriod();

        while (true) {
//            final boolean updated = DBSyncUtils.updatePeriodic(frequency, InactiveJobHandler.class.getCanonicalName());
            try {
                if (true) {
                    moveInactiveJobStates();
                    logger.log(Level.INFO, "InactiveJobHandler sleeping for " + this.getSleepPeriod() + " ms");
                    sleep(this.getSleepPeriod());
                } else {
                    sleep(this.getSleepPeriod());
                }
            } catch (InterruptedException e) {
                logger.log(Level.SEVERE, "InactiveJobHandler interrupted", e);
            }
        }
    }

    private void moveInactiveJobStates() {
        DBFunctions db = TaskQueueUtils.getQueueDB();
        if (db == null) {
            logger.log(Level.SEVERE, "InactiveJobHandler could not get a DB connection");
            return;
        }

        db.setQueryTimeout(60);

        db.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        String activeJobWithoutHeartbeatQuery = "SELECT q.queueId, q.statusId FROM QUEUE q JOIN QUEUEPROC qp\n" +
                "                                            WHERE q.queueId = qp.queueId\n" +
                "                                              AND  q.statusId IN (10, 7, 11)\n" + // RUNNING, STARTED, SAVING
                "                                              AND qp.lastupdate < NOW() - INTERVAL 1 HOUR";

        String inactiveJobsWithoutHeartbeatQuery = "SELECT q.queueId, q.statusId FROM QUEUE q JOIN  QUEUEPROC qp\n" +
                "                                           WHERE q.queueId = qp.queueId\n" +
                "                                               AND  q.statusId IN (-15)\n" + // ZOMBIE
                "                                               AND qp.lastupdate < NOW() - INTERVAL 2 HOUR";

        try (Timing t = new Timing(monitor, "InactiveJobHandler")) {
            t.startTiming();

            StringBuilder registerLog = new StringBuilder();
            logger.log(Level.INFO, "InactiveJobHandler starting to move inactive jobs to zombie state");
            moveState(db, activeJobWithoutHeartbeatQuery, JobStatus.ZOMBIE, registerLog);

            logger.log(Level.INFO, "InactiveJobHandler starting to move 2h inactive zombie state jobs to expired state");
            moveState(db, inactiveJobsWithoutHeartbeatQuery, JobStatus.EXPIRED, registerLog);

            t.endTiming();
            logger.log(Level.INFO, "InactiveJobHandler finished in " + t.getMillis() + " ms");
            registerLog.append("Moving inactive job states took: ").append(t.getMillis()).append(" ms");
            DBSyncUtils.registerLog(InactiveJobHandler.class.getCanonicalName(), registerLog.toString());
        } catch (Exception e) {
            logger.log(Level.SEVERE, "InactiveJobHandler: Exception", e);
        }
    }


    private void moveState(DBFunctions db, String query, JobStatus status, StringBuilder log) {
        db.query(query);

        int counter = 0;
        while (db.moveNext()) {
            TaskQueueUtils.setJobStatus(db.getl("queueId"), status, JobStatus.getStatus(db.geti("statusId")));
            counter++;
        }
        logger.log(Level.INFO, "Moved " + counter + " jobs to " + status + " state");
        log.append("Moved ").append(counter).append(" jobs to ").append(status).append(" state\n");
    }
}
