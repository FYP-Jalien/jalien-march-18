package alien.optimizers;

import java.util.ArrayList;
import java.util.List;
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
	 * Creates the OPTIMIZERS table if it did not exist before
	 */
	public static void checkLdapSyncTable() {
		try (DBFunctions db = ConfigUtils.getDB("alice_users");) {
			if (db == null) {
				logger.log(Level.INFO, "Could not get DBs!");
				return;
			}
			String sqlLdapDB = "CREATE TABLE IF NOT EXISTS `OPTIMIZERS` (`class` varchar(100) COLLATE latin1_general_cs NOT NULL, "
					+ "`lastUpdate` bigInt NOT NULL, `frequency` int(11) NOT NULL, `lastUpdatedLog` text COLLATE latin1_general_ci DEFAULT NULL, "
					+ "PRIMARY KEY (`class`)) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;";
			if (!db.query(sqlLdapDB))
				logger.log(Level.SEVERE, "Could not create table: OPTIMIZERS " + sqlLdapDB);
		}
	}

	/**
	 * Performs the database update operations when instructed from a periodic call
	 *
	 * @param db Database to update
	 * @param frequency Frequency of updates
	 * @param classname Class to update
	 * @return Success of update query
	 */
	public static boolean updatePeriodic(int initFrequency, String classname) {
		boolean updated = false;
		Long timestamp = Long.valueOf(System.currentTimeMillis());
		try (DBFunctions db = ConfigUtils.getDB("alice_users");) {
			if (db == null) {
				logger.log(Level.INFO, "Could not get DBs!");
				return false;
			}
			db.query("SELECT count(*) from `OPTIMIZERS` WHERE class = ?", false, classname);
			if (db.moveNext()) {
				int valuecounts = db.geti(1);
				if (valuecounts == 0) {
					updated = registerClass(initFrequency, classname, timestamp);
				}
				else {
					db.query("SELECT frequency from `OPTIMIZERS` WHERE class = ?", false, classname);
					if (db.moveNext()) {
						int frequency = db.geti("frequency");
						// If the frequency is set to -1 do not run
						if (frequency != -1) {
							Long lastUpdated = Long.valueOf(System.currentTimeMillis() - frequency);
							updated = db.query("UPDATE OPTIMIZERS SET lastUpdate = ? WHERE class = ? AND lastUpdate < ?",
									false, timestamp, classname, lastUpdated) && db.getUpdateCount() > 0;
						}
					}
				}
			}
			return updated;
		}
	}

	/**
	 * Registers a new class into the OPTIMIZERS database
	 *
	 * @param frequency Frequency of updates
	 * @param classname Classname to introduce
	 * @param timestamp Last update timestamp
	 * @return Success of the registration
	 */
	private static boolean registerClass(int frequency, String classname, Long timestamp) {
		boolean updated;
		try (DBFunctions db = ConfigUtils.getDB("alice_users");) {
			if (db == null) {
				logger.log(Level.INFO, "Could not get DBs!");
				return false;
			}
			updated = db.query("INSERT INTO OPTIMIZERS (class, lastUpdate, frequency) VALUES (?, ?, ?)",
					false, classname, timestamp, Integer.valueOf(frequency));
			return updated;
		}
	}

	/**
	 * Performs the database update operations when instructed from the user's shell call
	 *
	 * @param db Database to update
	 * @param classname Class to update
	 * @return Success of the update query
	 */
	public static boolean updateManual(String classname, String log) {
		Long timestamp = Long.valueOf(System.currentTimeMillis());
		try (DBFunctions db = ConfigUtils.getDB("alice_users");) {
			if (db == null) {
				logger.log(Level.INFO, "Could not get DBs!");
				return false;
			}
			boolean updated = db.query("UPDATE OPTIMIZERS SET lastUpdate = ?, lastUpdatedLog = ? WHERE class = ?",
					false, timestamp, log, classname) && db.getUpdateCount() > 0;
			return updated;
		}
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
		try (DBFunctions db = ConfigUtils.getDB("alice_users");) {
			if (db == null) {
				logger.log(Level.INFO, "Could not get DBs!");
				return false;
			}
			boolean updated = db.query("UPDATE OPTIMIZERS set lastUpdatedLog = ? WHERE class = ?", false, logOutput, classname);
			return updated;
		}
	}

	/**
	 * Returns last log stored in the database for the class
	 *
	 * @param classname Class to get the last log from
	 * @param verbose Boolean for the verbose output
	 * @param exactMatch Boolean for the exact matching of the classname in the db
	 * @return String containing the last log
	 */
	public static String getLastLog(String classname, boolean verbose, boolean exactMatch) {
		try (DBFunctions db = ConfigUtils.getDB("alice_users");) {
			if (db == null) {
				logger.log(Level.INFO, "Could not get DBs!");
				return "";
			}
			boolean querySuccess = false;
			if (exactMatch)
				querySuccess = db.query("SELECT * FROM `OPTIMIZERS` WHERE class = ?", false, classname);
			else {
				String log = "";
				querySuccess = db.query("SELECT * FROM `OPTIMIZERS` WHERE class LIKE concat('%', ?, '%')", false, classname);
				while (db.moveNext())
					log = log + getLastLog(db.gets("class"), verbose, true);
				return log;
			}
			if (!querySuccess) {
				logger.log(Level.SEVERE, "Could not get the last updated logs from the OPTIMIZERS db");
				return "ERROR in getting last log";
			}
			if (db.count() <= 0)
				return "Introduced class (" + classname + ") did not match any of the contained in the database or allowed patterns";
			if (verbose)
				return "Frequency: " + db.gets("frequency") + " - Timestamp: " + db.gets("lastUpdate") + "\nLog: " + db.gets("lastUpdatedLog") + "\n";
			return "Timestamp: " + db.gets("lastUpdate") + "\nLog: " + db.gets("lastUpdatedLog") + "\n";
		}
	}

	/**
	 * Modifies the stored frequency in the database
	 *
	 * @param updatedFrequency New frequency value
	 * @param classnames List of classes to change the frequency for
	 * @return Boolean stating the success of the update
	 */
	public static boolean modifyFrequency(int updatedFrequency, List<String> classnames) {
		try (DBFunctions db = ConfigUtils.getDB("alice_users");) {
			if (db == null) {
				logger.log(Level.INFO, "Could not get DBs!");
				return false;
			}
			for (String classname : classnames) {
				boolean updated = db.query("UPDATE OPTIMIZERS SET frequency = ? WHERE class = ?",
						false, Integer.valueOf(updatedFrequency), classname) && db.getUpdateCount() > 0;
				if (updated == false)
					return false;
			}
		}
		return true;
	}

	/**
	 * Returns a list of all the registered classes in the database
	 *
	 * @return List of registered classes
	 */
	public static ArrayList<String> getRegisteredClasses() {
		try (DBFunctions db = ConfigUtils.getDB("alice_users");) {
			if (db == null) {
				logger.log(Level.INFO, "Could not get DBs!");
				return null;
			}
			boolean querySuccess = db.query("SELECT class FROM `OPTIMIZERS`");
			if (!querySuccess) {
				logger.log(Level.SEVERE, "Could not get the classnames from the OPTIMIZERS db");
				return null;
			}
			ArrayList<String> classes = new ArrayList<>();
			while (db.moveNext())
				classes.add(db.gets("class"));
			return classes;
		}
	}

	/**
	 * Gets the full name of a class given a keyword
	 *
	 * @param classname Class to get full name
	 * @return The full name of the class
	 */
	public static String getFullClassName(String classname) {
		try (DBFunctions db = ConfigUtils.getDB("alice_users");) {
			if (db == null) {
				logger.log(Level.INFO, "Could not get DBs!");
				return "";
			}
			boolean querySuccess = db.query("SELECT class FROM `OPTIMIZERS` WHERE class LIKE concat('%', ?, '%')", false, classname);
			if (!querySuccess) {
				logger.log(Level.SEVERE, "Could not get the classnames from the OPTIMIZERS db");
				return "";
			}
			if (db.moveNext())
				return db.gets("class");
			return "";
		}
	}
}
