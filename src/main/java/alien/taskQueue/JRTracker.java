package alien.taskQueue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;
import lazyj.DBFunctions;

/**
 * @author marta
 */
public class JRTracker implements Runnable {

	static final Object pinningTrackerSync = new Object();

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(JRTracker.class.getCanonicalName());

	public static Set<UUID> uuidToUpdate;

	private long aliveThreshold;
	public static long lockLeasingThreshold;

	private static int MAX_QUERY_LENGTH;

	public JRTracker() {
		logger.log(Level.INFO, "Starting new JRTracker");
		JRTracker.uuidToUpdate = new HashSet<>();
		aliveThreshold = 15*60;
		MAX_QUERY_LENGTH = 1000;
		lockLeasingThreshold = 5000; //ms We give the lock 5s max
	}

	@Override
	public void run() {
		logger.log(Level.INFO, "Entering RUN method of JRTRacker");
		try (DBFunctions db = TaskQueueUtils.getQueueDB()) {
			if (db == null)
				return;
			db.setReadOnly(false);
			db.setQueryTimeout(60);

			while (true) {
				//First update the recent requests
				try {
					updateJRTimestamps(db);

					deleteUnactiveJRs(db);
				} catch (Exception e) {
					logger.log(Level.SEVERE, "Caugth exception on JRTracker",e);
				}
				try {
					Thread.sleep(60000);
				}
				catch (InterruptedException e) {
					logger.log(Level.INFO, "Interrupting JRTracker thread", e);
				}
			}
		}
	}


	private void deleteUnactiveJRs(DBFunctions db) {
		String q = "delete from COREPINNING where TIMESTAMPDIFF(SECOND,updateTs,current_timestamp)>" + aliveThreshold + ";";
		db.query(q);

		q = "delete from COREPINNING_LOCK where TIMESTAMPDIFF(SECOND,lock_timestamp,current_timestamp)>" + lockLeasingThreshold + ";";
		db.query(q);
	}

	private static void updateJRTimestamps(DBFunctions db) {
		final StringBuilder sb = new StringBuilder();
		if (uuidToUpdate.size() > 0) {
			synchronized(pinningTrackerSync) {
				final ArrayList<UUID> allUUIDs = new ArrayList<>();
				allUUIDs.addAll(uuidToUpdate);
				for (int i = 0; i < allUUIDs.size(); i += MAX_QUERY_LENGTH) {
					sb.setLength(0);
					final List<UUID> sublist = allUUIDs.subList(i, Math.min(i + MAX_QUERY_LENGTH, allUUIDs.size()));

					for (final UUID u : sublist) {
						if (sb.length() > 0)
							sb.append(',');

						sb.append("string2binary('").append(u.toString()).append("')");
					}

					final String q = "update COREPINNING set updateTs = current_timestamp where uuid in (" + sb.toString() + ");";

					if (!db.query(q))
						throw new IllegalStateException("Failed executing query: " + q);
				}
				uuidToUpdate.clear();
			}
		}

	}
}
