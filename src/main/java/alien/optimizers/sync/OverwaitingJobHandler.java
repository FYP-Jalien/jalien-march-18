package alien.optimizers.sync;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import alien.optimizers.DBSyncUtils;
import alien.optimizers.Optimizer;
import alien.optimizers.priority.PriorityRapidUpdater;
import alien.taskQueue.JobStatus;
import alien.taskQueue.TaskQueueUtils;
import lazyj.DBFunctions;

import java.time.Duration;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Jorn-Are Flaten
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
			try {
				if (DBSyncUtils.updatePeriodic(frequency, OverwaitingJobHandler.class.getCanonicalName(), this)) {
					startCron();
				}
			}
			catch (Exception e) {
				try {
					logger.log(Level.SEVERE, "Exception executing optimizer", e);
					DBSyncUtils.registerException(OverwaitingJobHandler.class.getCanonicalName(), e);
				}
				catch (Exception e2) {
					logger.log(Level.SEVERE, "Cannot register exception in the database", e2);
				}
			}

			try {
				logger.log(Level.INFO, "OverwaitingJobHandler sleeping for " + this.getSleepPeriod() + " ms");
				sleep(this.getSleepPeriod());
			}
			catch (InterruptedException e) {
				logger.log(Level.SEVERE, "OverwaitingJobHandler interrupted", e);
			}
		}
	}

	private static void startCron() {
		logger.log(Level.INFO, "OverwaitingJobHandler starting");
		StringBuilder registerlog = new StringBuilder();
		try (Timing t1 = new Timing(monitor, "OverwaitingJobHandler"); DBFunctions db = TaskQueueUtils.getQueueDB();) {
			if (db == null) {
				logger.log(Level.SEVERE, "OverwaitingJobHandler could not get a DB connection");
				return;
			}
			
			t1.startTiming();
			db.setReadOnly(false);

			TaskQueueUtils.moveState(db, getQuery(), JobStatus.OVER_WAITING, registerlog);
			t1.endTiming();
			logger.log(Level.INFO, "OverwaitingJobHandler finished in " + t1.getMillis() + " ms");
			registerlog.append("OverwaitingJobHandler finished in ")
					.append(t1.getMillis())
					.append(" ms\n");
		}
	}

	private static String getQuery() {
		return "SELECT queueId, statusId from QUEUE where statusId = " + JobStatus.WAITING.getAliEnLevel() + " and (UNIX_TIMESTAMP() - UNIX_TIMESTAMP(mtime)) > 3600 * 24 * 3";
	}
}
