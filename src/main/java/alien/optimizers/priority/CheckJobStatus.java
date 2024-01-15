package alien.optimizers.priority;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import alien.optimizers.Optimizer;
import alien.priority.JobDto;
import alien.taskQueue.JobStatus;
import alien.taskQueue.TaskQueueUtils;
import lazyj.DBFunctions;

import java.sql.Connection;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

//TODO rename
public class CheckJobStatus extends Optimizer {
    static final Logger logger = ConfigUtils.getLogger(PriorityReconciliationService.class.getCanonicalName());

    static final Monitor monitor = MonitorFactory.getMonitor(PriorityReconciliationService.class.getCanonicalName());

    static int counter = 0;

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
            StringBuilder query = synchronizeMasterJobStatusWithSubjobs(getMasterAndSubjobs(db));
            logger.log(Level.INFO, "After synchronizeMasterJobStatusWithSubjobs, before DB write ");
            boolean result = writeToDb(query, db, dbdev);
            t.endTiming();
            logger.log(Level.INFO, "CheckJobStatus executed in: " + t.getMillis());

        }

    }

    private boolean writeToDb(StringBuilder query, DBFunctions db, DBFunctions dbdev) {
        //TODO
        return false;
    }

    private StringBuilder synchronizeMasterJobStatusWithSubjobs(Map<Long, Map<Long, JobDto>> masterAndSubJobs) {
        logger.log(Level.INFO, "synchronizing master and subjobs... ");
        StringBuilder updateQuery = new StringBuilder("INSERT INTO QUEUE (queueId, statusId) VALUES ");

        for (Map.Entry<Long, Map<Long, JobDto>> entry : masterAndSubJobs.entrySet()) {
            Long masterJobId = entry.getKey();
            logger.log(Level.INFO, "masterJobId: " + masterJobId);
            Map<Long, JobDto> jobs = entry.getValue();
            JobDto jobDto = jobs.get(masterJobId);
            if(jobDto == null){
                logger.log(Level.INFO, "jobDto is null");
                continue;
            }
            int masterStatusId = jobDto.getStatusId();
            boolean allSubjobsFinal = isSubjobsFinal(jobs, masterJobId);

            if (masterStatusId == JobStatus.SPLIT.getAliEnLevel()) {
                logger.log(Level.INFO, "first IF. Master = SPLIT: " + masterStatusId);
                if (allSubjobsFinal) {
                    jobDto.setStatusId(JobStatus.DONE.getAliEnLevel());
                    appendUpdateQuery(updateQuery, masterJobId, JobStatus.DONE.getAliEnLevel());

                }
            } else if (masterStatusId == JobStatus.DONE.getAliEnLevel() || masterStatusId == JobStatus.KILLED.getAliEnLevel()) {
                if (!allSubjobsFinal) {
                    logger.log(Level.INFO, "master is final, but subjobs are not. moving to split: " + masterStatusId);
                    jobDto.setStatusId(JobStatus.SPLIT.getAliEnLevel());
                    appendUpdateQuery(updateQuery, masterJobId, JobStatus.SPLIT.getAliEnLevel());
                }
                // Subjobs and master are final - Nothing happens
            } else {
                if (allSubjobsFinal) {
                    logger.log(Level.INFO, "subjobs are all final: " + masterStatusId);
                    jobDto.setStatusId(JobStatus.DONE.getAliEnLevel());
                    appendUpdateQuery(updateQuery, masterJobId, JobStatus.DONE.getAliEnLevel());
                } else {
                    logger.log(Level.INFO, "subjobs have running, moving master to split: " + masterStatusId);
                    jobDto.setStatusId(JobStatus.SPLIT.getAliEnLevel());
                    appendUpdateQuery(updateQuery, masterJobId, JobStatus.SPLIT.getAliEnLevel());
                }
            }
        }

        updateQuery.deleteCharAt(updateQuery.length() - 2).append("ON DUPLICATE KEY UPDATE statusId = VALUES(statusId)");

        logger.log(Level.INFO, "updateQuery: " + updateQuery);
        logger.log(Level.INFO, "counter: " + counter);
        logger.log(Level.INFO, "masterAndSubJobs size: " + masterAndSubJobs.size());
        return updateQuery;
    }

    private void appendUpdateQuery(StringBuilder updateQuery, Long masterJobId, int statusId) {
        updateQuery
                .append("(")
                .append(masterJobId)
                .append(", ")
                .append(statusId)
                .append("), ");
        counter++;
    }

    private boolean isSubjobsFinal(Map<Long, JobDto> jobs, long masterJobId) {
        return jobs.entrySet().parallelStream()
                .filter(s -> !Objects.equals(s.getKey(), masterJobId) && s.getValue().getSplit() > 0)
                .allMatch(s -> s.getValue().getStatusId() == JobStatus.DONE.getAliEnLevel() || s.getValue().getStatusId() == JobStatus.KILLED.getAliEnLevel());
    }

    private Map<Long, Map<Long, JobDto>> getMasterAndSubjobs(DBFunctions db) {
        db.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        Map<Long, Map<Long, JobDto>> relatedMasterAndSubjobs = new HashMap<>();
        db.query("select split, queueId, statusId from QUEUE where masterjob = 1 or split > 0");
        while (db.moveNext()) {
            long split = db.getl("split");
            long queueId = db.getl("queueId");
            int statusId = db.geti("statusId");

            JobDto jobDto = new JobDto(queueId, split, statusId);
            long key = (split == 0) ? queueId : split;
            relatedMasterAndSubjobs.computeIfAbsent(key, k -> new HashMap<>()).putIfAbsent(queueId, jobDto);

        }
        logger.log(Level.INFO, "relatedMasterAndSubjobs map size: " + relatedMasterAndSubjobs.keySet().size());
        return relatedMasterAndSubjobs;
    }


}
