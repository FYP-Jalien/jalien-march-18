package alien.optimizers.priority;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import alien.optimizers.Optimizer;
import alien.priority.JobDto;
import alien.taskQueue.TaskQueueUtils;
import lazyj.DBFunctions;

import java.sql.Connection;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

//TODO rename
public class CheckJobStatus extends Optimizer {
    static final Logger logger = ConfigUtils.getLogger(PriorityReconciliationService.class.getCanonicalName());

    static final Monitor monitor = MonitorFactory.getMonitor(PriorityReconciliationService.class.getCanonicalName());

    @Override
    public void run() {
        logger.log(Level.INFO, "CheckJobStatus starting");
        this.setSleepPeriod(Duration.ofMinutes(30).toMillis());

        while (true) {
            try {
                startCron();
                logger.log(Level.INFO, "CheckJobStatus sleeping for " + this.getSleepPeriod() + " ms");
                // TODO: check job status - move subjob to done if siblings are done and update master
                // TODO: check job status - move master to SPLIT status if subjob is reactivated

                sleep(this.getSleepPeriod());
            } catch (InterruptedException e) {
                logger.log(Level.SEVERE, "CheckJobStatus interrupted", e);
            }
        }
    }

    private void startCron() {
        DBFunctions db = TaskQueueUtils.getQueueDB();
        if (db == null) {
            logger.log(Level.SEVERE, "CheckJobStatus could not get a DB connection");
            return;
        }

        db.setQueryTimeout(60);

        DBFunctions dbdev = TaskQueueUtils.getProcessesDevDB();
        if (dbdev == null) {
            logger.log(Level.INFO, "CheckJobStatus(processesDev) could not get a DB connection");
            return;
        }
        dbdev.setQueryTimeout(60);

        try (Timing t = new Timing(monitor, "CheckJobStatus")) {
            t.startTiming();
            getMasterAndSubjobs(db);
            t.endTiming();
            logger.log(Level.INFO, "CheckJobStatus timing: " + t.getMillis());

        }

    }
    // subjob is set to active, master is set to SPLIT
    private void activeMasterJob(){}
    private Map<Integer, Map<Integer, JobDto>> getMasterAndSubjobs(DBFunctions db) {

        db.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        Map<Integer, Map<Integer, JobDto>> relatedMasterAndSubjobs = new HashMap<>();
        db.query("select split, queueId, statusId from QUEUE where masterjob = 1 or split > 0");
        Set<Integer> test = new HashSet<>();
        while (db.moveNext()) {
            int split = db.geti("split");
            int queueId = db.geti("queueId");
            int statusId = db.geti("statusId");
            JobDto jobDto = new JobDto(queueId, split, statusId);

            test.add(split);
            relatedMasterAndSubjobs.computeIfAbsent(split, k -> new HashMap<>()).putIfAbsent(queueId, jobDto);

        }
        logger.log(Level.INFO, "relatedMasterAndSubjobs: " + relatedMasterAndSubjobs.keySet());
        logger.log(Level.INFO, "relatedMasterAndSubjobs map size: " + relatedMasterAndSubjobs.keySet().size());
        logger.log(Level.INFO, "test: " + test.size());
        return relatedMasterAndSubjobs;
    }


}
