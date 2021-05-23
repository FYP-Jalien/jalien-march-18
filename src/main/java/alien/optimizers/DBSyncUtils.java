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

	/**
	 * Creates the LDAP_SYNC table if it did not exist before
	 */
	public static void checkLdapSyncTable(DBFunctions db) {
		String sqlLdapDB = "CREATE TABLE IF NOT EXISTS `LDAP_SYNC` (`class` varchar(100) COLLATE latin1_general_cs NOT NULL, "
				+ "`lastUpdate` bigInt NOT NULL, `frequency` int(11) NOT NULL, PRIMARY KEY (`class`)) "
				+ "ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;";
		if (!db.query(sqlLdapDB))
			logger.log(Level.SEVERE, "Could not create table: LDAP_SYNC " + sqlLdapDB);
	}

	/**
	 * Performs the database update operations when instructed from a periodic call
	 *
	 * @param db Database to update
	 * @param frequency Frequency of updates
	 * @param classname Class to update
	 * @return Success of update query
	 */
	public static boolean updatePeriodic(DBFunctions db, int frequency, String classname) {
		boolean updated = false;
		Long timestamp = Long.valueOf(System.currentTimeMillis());
		logger.log(Level.INFO, "DBG: Called update periodic method");
		db.query("SELECT count(*) from `LDAP_SYNC` WHERE class = ?", false, classname);
		if (db.moveNext()) {
			int valuecounts = db.geti(1);
			if (valuecounts == 0) {
				updated = registerClass(db, frequency, classname, timestamp);
			}
			else {
				Long lastUpdated = Long.valueOf(System.currentTimeMillis() - frequency);
				updated = db.query("UPDATE LDAP_SYNC SET lastUpdate = ? WHERE class = ? AND lastUpdate < ?",
						false, timestamp, classname, lastUpdated) && db.getUpdateCount() > 0;
			}
		}
		return updated;
	}

	private static boolean registerClass(DBFunctions db, int frequency, String classname, Long timestamp) {
		boolean updated;
		updated = db.query("INSERT INTO LDAP_SYNC (class, lastUpdate, frequency) VALUES (?, ?, ?)",
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
	public static boolean updateManual(DBFunctions db, String classname) {
		logger.log(Level.INFO, "DBG: Called update manual method");
		Long timestamp = Long.valueOf(System.currentTimeMillis());
		boolean updated = db.query("UPDATE LDAP_SYNC SET lastUpdate = ? WHERE class = ?",
				false, timestamp, classname) && db.getUpdateCount() > 0;
		return updated;
	}

}
