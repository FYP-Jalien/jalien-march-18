package alien.optimizers.priority;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import alien.optimizers.DBSyncUtils;
import alien.optimizers.Optimizer;
import alien.priority.JobDto;
import alien.taskQueue.JobStatus;
import alien.taskQueue.TaskQueueUtils;
import lazyj.DBFunctions;

import java.sql.Connection;
import java.time.Duration;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class OldJobRemover extends Optimizer {
    static final Logger logger = ConfigUtils.getLogger(PriorityRapidUpdater.class.getCanonicalName());

    static final Monitor monitor = MonitorFactory.getMonitor(PriorityRapidUpdater.class.getCanonicalName());

    @Override
    public void run() {
        this.setSleepPeriod(Duration.ofMinutes(10).toMillis()); //10m
        int frequency = (int) this.getSleepPeriod();


        while (true) {
//            boolean updated = DBSyncUtils.updatePeriodic(frequency, JobAgentUpdater.class.getCanonicalName());
            boolean updated = true;
            if (updated) {
                try {
                    startCron();
                    logger.log(Level.INFO, "OldJobRemover sleeping for " + this.getSleepPeriod() + " ms");
                    sleep(this.getSleepPeriod());
                } catch (InterruptedException e) {
                    logger.log(Level.WARNING, "OldJobRemover interrupted", e);
                }
            }
        }
    }

    private void startCron() {
        logger.log(Level.INFO, "OldJobRemover starting");
        try (Timing t0 = new Timing(monitor, "OldJobRemover")) {
            t0.startTiming();

            DBFunctions db = TaskQueueUtils.getQueueDB();
            if (db == null) {
                logger.log(Level.SEVERE, "OldJobRemover could not get a DB connection");
                return;
            }
            db.setQueryTimeout(60);

            // development DB
            DBFunctions dbdev = TaskQueueUtils.getProcessesDevDB();
            if (dbdev == null) {
                logger.log(Level.SEVERE, "OldJobRemover(processesDev) could not get a DB connection");
                return;
            }
            dbdev.setQueryTimeout(60);
            logger.log(Level.INFO, "DB Connections established");

            db.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            StringBuilder registerLog = new StringBuilder();

            Timing t1 = new Timing(monitor, "OldJobRemover select");
            t1.startTiming();
            Map<Long, Map<Long, JobDto>> oldJobs = getOldJobs(db, getOldJobsQuery());
            logger.log(Level.INFO, "OldJobs size: " + oldJobs.size());
            registerLog.append("Number of old master or single jobs: ")
                    .append(oldJobs.size())
                    .append("\n");
            t1.endTiming();
            logger.log(Level.INFO, "OldJobRemover select done in: " + t1.getMillis() + " ms");

            Timing t2 = new Timing(monitor, "OldJobRemover findMasterAndSubJobsInFinalState");
            t2.startTiming();
            Set<Long> finalStateJobs = findMasterAndSubJobsInFinalState(oldJobs);
            logger.log(Level.INFO, "FinalStateJobs size: " + finalStateJobs.size());
            t2.endTiming();
            logger.log(Level.INFO, "OldJobRemover findMasterAndSubJobsInFinalState done in: " + t2.getMillis() + " ms");

            Timing t3 = new Timing(monitor, "OldJobRemover removeOldJobs");
            t3.startTiming();
            if (removeOldJobs(dbdev, getRemoveQuery(finalStateJobs))) {
                logger.log(Level.INFO, "Removed " + finalStateJobs.size() + " old jobs");
                registerLog.append("Removed ")
                        .append(finalStateJobs.size())
                        .append(" old jobs\n");
            } else {
                logger.log(Level.INFO, "Old jobs not removed");
            }
            t3.endTiming();
            logger.log(Level.INFO, "OldJobRemover removeOldJobs done in: " + t3.getMillis() + " ms");
            registerLog.append("OldJobRemover removeOldJobs done in: ")
                    .append(t3.getMillis())
                    .append(" ms\n");


            t0.endTiming();
            logger.log(Level.INFO, "OldJobRemover done in: " + t0.getMillis() + " ms");
            registerLog.append("OldJobRemover done in: ")
                    .append(t0.getMillis())
                    .append(" ms\n");
            DBSyncUtils.registerLog(OldJobRemover.class.getCanonicalName(), registerLog.toString());
        }

    }


    private boolean removeOldJobs(DBFunctions db, String query) {
        return db.query(query);
//        return false;
    }

    private String getRemoveQuery(Set<Long> finalJobs) {
        StringBuilder q = new StringBuilder("DELETE FROM QUEUE WHERE queueId IN (");
        for (Long id : finalJobs) {
            q.append(id).append(",");
        }
        q.deleteCharAt(q.length() - 1);
        q.append(");");

        return q.toString();
    }

    private Set<Long> findMasterAndSubJobsInFinalState(Map<Long, Map<Long, JobDto>> oldJobs) {
        Set<Long> jobsInFinalState = new HashSet<>();

        for (Map.Entry<Long, Map<Long, JobDto>> entry : oldJobs.entrySet()) {
            Map<Long, JobDto> jobsMap = entry.getValue();

            if (isJobFinal(jobsMap, entry.getKey())) {
                jobsInFinalState.addAll(jobsMap.keySet());
            }
        }

        return jobsInFinalState;
    }

    private boolean isJobFinal(Map<Long, JobDto> jobs, long masterJobId) {
        if (isSingleJob(jobs, masterJobId)) {
            JobDto masterJob = jobs.get(masterJobId);
            return masterJob != null && (masterJob.getStatusId() == JobStatus.DONE.getAliEnLevel()
                    || masterJob.getStatusId() == JobStatus.DONE_WARN.getAliEnLevel());
        }

        return jobs.entrySet().parallelStream()
                .filter(s -> !Objects.equals(s.getKey(), masterJobId) && s.getValue().getSplit() > 0)
                .allMatch(s -> s.getValue().getStatusId() == JobStatus.DONE.getAliEnLevel()
                        || s.getValue().getStatusId() == JobStatus.DONE_WARN.getAliEnLevel());
    }

    private boolean isSingleJob(Map<Long, JobDto> jobs, long masterJobId) {
        return jobs.entrySet().parallelStream()
                .noneMatch(s -> s.getValue().getSplit() == masterJobId);
    }

    private Map<Long, Map<Long, JobDto>> getOldJobs(DBFunctions db, String query) {
        Map<Long, Map<Long, JobDto>> oldJobs = new HashMap<>();
        boolean result = db.query(query);
        if (result) {
            while (db.moveNext()) {
                long split = db.getl("split");
                long queueId = db.getl("queueId");
                int statusId = db.geti("statusId");

                JobDto jobDto = new JobDto(queueId, split, statusId);
                long key = (split == 0) ? queueId : split;
                oldJobs.computeIfAbsent(key, k -> new HashMap<>()).putIfAbsent(queueId, jobDto);
            }
        }
        return oldJobs;
    }

    private String getOldJobsQuery() {
        return "select qs.queueId, qs.split, qs.statusId "
                + "FROM QUEUE qs "
                + "left outer join QUEUE qm on qs.split = qm.queueId "
                + "WHERE qs.received < unix_timestamp() - 60 * 60 * 24 * 5 "
                + "and greatest(qs.mtime, coalesce(qm.mtime, qs.mtime)) < date_sub(now(), interval 5 day) "
                + "and (qm.statusId is null or qm.statusId != 3) "
                + "and (qs.split != 0 or qs.statusId != 3) "
                + "limit 100000;";
    }
}
