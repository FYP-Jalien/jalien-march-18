package alien.optimizers.site;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import alien.optimizers.DBSyncUtils;
import alien.optimizers.Optimizer;
import alien.site.SiteStatusDTO;
import alien.taskQueue.JobStatus;
import alien.taskQueue.TaskQueueUtils;
import lazyj.DBFunctions;

import java.sql.Connection;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
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
            final boolean updated = DBSyncUtils.updatePeriodic(frequency, SitequeueReconciler.class.getCanonicalName(), this);
            try {
                if (updated) {
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
        logger.log(Level.INFO, "SitequeueReconciler... trying to establish database connections");
        try (DBFunctions db = TaskQueueUtils.getQueueDB(); DBFunctions dbdev = TaskQueueUtils.getProcessesDevDB()) {
            if (db == null) {
                logger.log(Level.SEVERE, "SitequeueReconciler could not get a DB connection");
                return;
            }

            db.setQueryTimeout(60);

            if (dbdev == null) {
                logger.log(Level.SEVERE, "SitequeueReconciler(processesDev) could not get a DB connection");
                return;
            }
            logger.log(Level.INFO, "Reconciling sitequeue obtained database connections");

            StringBuilder registerLog = new StringBuilder();
            Set<SiteStatusDTO> sites = executeTotalCostQuery(db, getTotalCostQuery(), registerLog);

            Map<String, Double> totalCostBySite = sites.stream()
                    .collect(Collectors.groupingBy(SiteStatusDTO::getSiteId, Collectors.summingDouble(SiteStatusDTO::getTotalCost)));

            Map<String, Map<String, Double>> countBySiteAndStatus = sites.stream()
                    .collect(Collectors.groupingBy(SiteStatusDTO::getSiteId, Collectors.groupingBy(SiteStatusDTO::getStatusId, Collectors.summingDouble(SiteStatusDTO::getCount))));


            updateCountAndCost(countBySiteAndStatus, totalCostBySite, dbdev, registerLog);

            DBSyncUtils.registerLog(SitequeueReconciler.class.getCanonicalName(), registerLog.toString());

        }
    }

    private void updateCountAndCost(Map<String, Map<String, Double>> countBySiteAndStatus,Map<String, Double> totalCostBySite, DBFunctions db, StringBuilder registerlog) {
        AtomicInteger counter = new AtomicInteger();

        try (Timing t = new Timing(monitor, "SitequeueReconciler_updateCount")) {
            t.startTiming();
            countBySiteAndStatus.forEach((siteId, statusCount) -> {
                StringBuilder query = new StringBuilder("UPDATE SITEQUEUES SET ");
                int size = statusCount.size();
                Object[] parameters = new Object[size + 2]; // +2 for cost and siteId
                AtomicInteger index = new AtomicInteger(0);
                statusCount.forEach((statusId, count) -> {
                    query.append(JobStatus.getStatusByAlien(Integer.valueOf(statusId))).append(" = ?, ");
                    parameters[index.getAndIncrement()] = count;
                    counter.getAndIncrement();
                });
                addCostToUpdateQuery(totalCostBySite, query, parameters, size, siteId);
                query.delete(query.length() - 2, query.length());
                query.append(" WHERE siteId = ?");
                parameters[size +1 ] = siteId;
                db.query(query.toString(), false, parameters);
                logFullQuery(query, parameters);
            });
            t.endTiming();
            logger.log(Level.INFO, "Updated " + counter.get() + " counts");
            logger.log(Level.INFO, "SitequeueReconciler updateCountAndCost executed in " + t.getMillis() + " ms");
            registerlog.append("Updated ").append(counter.get()).append(" counts\n");
            registerlog.append("SitequeueReconciler updateCountAndCost executed in ").append(t.getMillis()).append(" ms\n");
        }
    }

    private void logFullQuery(StringBuilder query, Object[] parameters) {
        for (Object parameter : parameters) {
            query.replace(query.indexOf("?"), query.indexOf("?") + 1, parameter.toString());
        }
        logger.log(Level.INFO, "SitequeueReconciler executing query: " + query.toString());
    }

    private void addCostToUpdateQuery(Map<String, Double> totalCostBySite, StringBuilder query, Object[] parameters, int size, String siteId) {
        Double cost = totalCostBySite.get(siteId);
        query.append("cost = ?, ");
        parameters[size] = cost;
    }

    private Set<SiteStatusDTO> executeTotalCostQuery(DBFunctions db, String totalCostQuery, StringBuilder registerlog) {
        db.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        db.setQueryTimeout(60);
        Timing t = new Timing(monitor, "SitequeueReconciler");
        t.startTiming();
        boolean res = db.query(totalCostQuery);
        logger.log(Level.INFO, "SitequeueReconciler result: " + res);
        t.endTiming();
        logger.log(Level.INFO, "SitequeueReconciler select executed in " + t.getMillis() + " ms");
        registerlog.append("SitequeueReconciler select executed in ")
                .append(t.getMillis()).append(" ms\n");

        Set<SiteStatusDTO> dtos = new HashSet<>();
        while (db.moveNext()) {
            String siteId = db.gets(1);
            String statusId = db.gets(2);
            long count = db.getl(3);
            long totalCost = db.getl(4);
            dtos.add(new SiteStatusDTO(siteId, statusId, count, totalCost));
        }

        registerlog.append("SitequeueReconciler retrieved: ").append(dtos.size()).append(" elements\n");
        logger.log(Level.INFO, "SitequeueReconciler retrieved: " + dtos.size() + " elements");
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
