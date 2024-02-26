package alien.optimizers.site;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import alien.optimizers.DBSyncUtils;
import alien.optimizers.Optimizer;
import alien.optimizers.priority.PriorityReconciliationService;
import alien.site.SiteStatusDTO;
import alien.taskQueue.JobStatus;
import alien.taskQueue.TaskQueueUtils;
import lazyj.DBFunctions;

import java.sql.Connection;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * @author Jorn-Are Flaten
 * @since 2024-02-20
 */
public class SitequeueReconciler extends Optimizer {
    static final Logger logger = ConfigUtils.getLogger(SitequeueReconciler.class.getCanonicalName());
    static final Monitor monitor = MonitorFactory.getMonitor(SitequeueReconciler.class.getCanonicalName());

    @Override
    public void run() {
        this.setSleepPeriod(3600 * 1000); // 1h
        int frequency = (int) this.getSleepPeriod();

        while (true) {
//            final boolean updated = DBSyncUtils.updatePeriodic(frequency, SitequeueReconciler.class.getCanonicalName(), this);
            try {
                if (true) {
                    reconcileSitequeue();
                }
            } catch (Exception e) {
                try {
                    logger.log(Level.SEVERE, "Exception executing optimizer", e);
                    DBSyncUtils.registerException(SitequeueReconciler.class.getCanonicalName(), e);
                } catch (Exception e2) {
                    logger.log(Level.SEVERE, "Cannot register exception in the database", e2);
                }
            }

            try {
                logger.log(Level.INFO, "SitequeueReconciler sleeps " + this.getSleepPeriod() + " ms");
                sleep(this.getSleepPeriod());
            } catch (InterruptedException e) {
                logger.log(Level.WARNING, "SitequeueReconciler interrupted", e);
            }
        }
    }

    private void reconcileSitequeue() {
        logger.log(Level.INFO, "Reconciling sitequeue... trying to establish database connections");
        try (DBFunctions db = TaskQueueUtils.getQueueDB(); DBFunctions dbdev = TaskQueueUtils.getProcessesDevDB()) {
            if (db == null) {
                logger.log(Level.SEVERE, "ReconcilePriority could not get a DB connection");
                return;
            }

            db.setQueryTimeout(60);

            if (dbdev == null) {
                logger.log(Level.SEVERE, "ReconcilePriority(processesDev) could not get a DB connection");
                return;
            }
            logger.log(Level.INFO, "Reconciling sitequeue obtained database connections");

            StringBuilder registerlog = new StringBuilder();
            Set<SiteStatusDTO> totalCostForSites = getTotalCostForSites(db, getTotalCostQuery(), registerlog);




//            DBSyncUtils.registerLog(SitequeueReconciler.class.getCanonicalName(), registerLog.toString());

        }
    }

    private Set<SiteStatusDTO> getTotalCostForSites(DBFunctions db, String totalCostQuery, StringBuilder registerlog) {
            db.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            db.setQueryTimeout(60);
            Timing t = new Timing(monitor, "SitequeueReconciler");
            t.startTiming();
            boolean res = db.query(totalCostQuery);
            logger.log(Level.INFO, "ReconcileSitequeue result: " + res);
            t.endTiming();
            logger.log(Level.INFO, "ReconcileSitequeue took " + t.getMillis() + " ms");
            registerlog.append("ReconcileSitequeue took ").append(t.getMillis()).append(" ms\n");

            Set<SiteStatusDTO> dtos = new HashSet<>();
            while(db.moveNext()) {
                String siteId = db.gets(1);
                String statusId = db.gets(2);
                long count = db.getl(3);
                long totalCost = db.getl(4);
                dtos.add(new SiteStatusDTO(siteId, statusId, count, totalCost));
                logger.log(Level.INFO, "SiteId: " + siteId + ", statusId: " + statusId + ", count: " + count + ", totalCost: " + totalCost);
            }

            registerlog.append("ReconcileSitequeue retrieved: ").append(dtos.size()).append(" elements\n");
            logger.log(Level.INFO, "ReconcileSitequeue retrieved: " + dtos.size() + " elements");
            return dtos;
    }

    private String getTotalCostQuery() {
        Set<JobStatus> allStates = new HashSet<>();
        allStates.addAll(JobStatus.finalStates());
        allStates.addAll(JobStatus.runningStates());
        allStates.addAll(JobStatus.waitingStates());
        allStates.addAll(JobStatus.errorneousStates());
        allStates.addAll(JobStatus.queuedStates());
        allStates.addAll(JobStatus.doneStates());

        String statusIds = allStates.stream()
                .map(status -> String.valueOf(status.getAliEnLevel()))
                .collect(Collectors.joining(", "));


        StringBuilder query = new StringBuilder();
        query.append("SELECT Q.siteid, Q.statusId, COUNT(*) AS count, SUM(QP.cost) AS totalCost ")
                .append("FROM QUEUE Q ")
                .append("JOIN QUEUEPROC QP ON Q.queueid = QP.queueid ")
                .append("WHERE Q.statusId IN (")
                .append(statusIds)
                .append(") ")
                .append("GROUP BY Q.siteid, Q.statusId;");

        logger.log(Level.INFO, "Total cost query: " + query.toString());
        return query.toString();

    }
}
