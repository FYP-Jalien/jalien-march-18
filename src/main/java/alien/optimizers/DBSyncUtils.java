package alien.optimizers;

import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import lazyj.DBFunctions;

/**
 * LFN utilities
 *
 * @author costing
 *
 */
public class DBSyncUtils {

	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(DBSyncUtils.class.getCanonicalName());

	/**
	 * Monitoring component
	 */
	static final Monitor monitor = MonitorFactory.getMonitor(DBSyncUtils.class.getCanonicalName());

	static DBFunctions db;

	/**
	 * Creates the OPTIMIZERS table if it did not exist before
	 */
	public static void checkLdapSyncTable(DBFunctions dbSync) {
		db = dbSync;
		String sqlLdapDB = "CREATE TABLE IF NOT EXISTS `OPTIMIZERS` (`class` varchar(100) COLLATE latin1_general_cs NOT NULL, "
				+ "`lastUpdate` bigInt NOT NULL, `frequency` int(11) NOT NULL, `lastUpdatedLog` text COLLATE latin1_general_ci DEFAULT NULL, "
				+ "PRIMARY KEY (`class`)) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;";
		if (!db.query(sqlLdapDB))
			logger.log(Level.SEVERE, "Could not create table: OPTIMIZERS " + sqlLdapDB);
	}

	/**
	 * Performs the database update operations when instructed from a periodic call
	 *
	 * @param db Database to update
	 * @param frequency Frequency of updates
	 * @param classname Class to update
	 * @return Success of update query
	 */
	public static boolean updatePeriodic(int frequency, String classname) {
		boolean updated = false;
		Long timestamp = Long.valueOf(System.currentTimeMillis());
		db.query("SELECT count(*) from `OPTIMIZERS` WHERE class = ?", false, classname);
		if (db.moveNext()) {
			int valuecounts = db.geti(1);
			if (valuecounts == 0) {
				updated = registerClass(frequency, classname, timestamp);
			}
			else {
				Long lastUpdated = Long.valueOf(System.currentTimeMillis() - frequency);
				updated = db.query("UPDATE OPTIMIZERS SET lastUpdate = ? WHERE class = ? AND lastUpdate < ?",
						false, timestamp, classname, lastUpdated) && db.getUpdateCount() > 0;
			}
		}
		return updated;
	}

	private static boolean registerClass(int frequency, String classname, Long timestamp) {
		boolean updated;
		updated = db.query("INSERT INTO OPTIMIZERS (class, lastUpdate, frequency) VALUES (?, ?, ?)",
				false, classname, timestamp, Integer.valueOf(frequency));
		return updated;
	}

	/**
	 * Performs the database update operations when instructed from the user's shell call
	 *
	 * @param db Database to update
	 * @param classname Class to update
	 * @return Success of the update query
	 */
	public static boolean updateManual(String classname) {
		Long timestamp = Long.valueOf(System.currentTimeMillis());
		boolean updated = db.query("UPDATE OPTIMIZERS SET lastUpdate = ? WHERE class = ?",
				false, timestamp, classname) && db.getUpdateCount() > 0;
		return updated;
	}

	/**
	 * Registers the log output from the DB synchronization
	 *
	 * @param db Database to update
	 * @param classname Class to update
	 * @param logOutput Log to register
	 * @return Success of the update query
	 */
	public static boolean registerLog(String classname, String logOutput) {
		logger.log(Level.INFO, "Registering log output in DB");
		boolean updated = db.query("UPDATE OPTIMIZERS set lastUpdatedLog = ? WHERE class = ?", false, logOutput, classname);
		return updated;
	}

	public static String getLastLog() {
		boolean querySuccess = db.query("SELECT * FROM `OPTIMIZERS`");
		if (!querySuccess) {
			logger.log(Level.SEVERE, "Could not get the last updated logs from the OPTIMIZERS db");
			return "ERROR in getting last log";
		}
		String log = "";
		while (db.moveNext()) {
			log = log + db.gets("lastUpdatedLog") + "\n";
		}
		return log;
	}
}
