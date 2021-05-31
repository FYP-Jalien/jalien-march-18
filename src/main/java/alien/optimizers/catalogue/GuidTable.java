package alien.optimizers.catalogue;

import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.catalogue.GUIDUtils;
import alien.config.ConfigUtils;
import alien.optimizers.DBSyncUtils;
import alien.optimizers.Optimizer;
import lazyj.DBFunctions;
import lazyj.mail.Mail;
import lazyj.mail.Sendmail;

/**
 * @author Miguel
 * @since Aug 9, 2016
 */
public class GuidTable extends Optimizer {

	/**
	 * Logging facility
	 */
	static final Logger logger = ConfigUtils.getLogger(GuidTable.class.getCanonicalName());

	/**
	 * When to switch to a new G table
	 */
	final static int maxCount = 50000000; // 50M

	/**
	 * At what point to send a warning about the current G table
	 */
	final static int warnCount = 40000000; // 40M

	/**
	 * When to update the lastUpdateTimestamp in the OPTIMIZERS db
	 */
	final static int updateDBCount = 10000;

	/**
	 * Number of rows at the last iteration
	 */
	int count;

	@Override
	public void run() {
		this.setSleepPeriod(3600 * 1000); // 1h

		logger.log(Level.INFO, "GuidTable optimizer starts");

		DBSyncUtils.checkLdapSyncTable();
		final int frequency = 3600 * 1000; // 1 hour default

		try (DBFunctions db = ConfigUtils.getDB("alice_users")) {
			if (db == null) {
				logger.log(Level.INFO, "GuidTable could not get a DB!");
				return;
			}

			while (true) {
				logger.log(Level.INFO, "GuidTable wakes up!: going to get G tables counts with max: " + maxCount);

				final boolean updated = DBSyncUtils.updatePeriodic(frequency, GuidTable.class.getCanonicalName());

				if (updated) {
					// Get count of latest G tables
					db.setReadOnly(true);

					if (!db.query("select max(tableName) from GUIDINDEX where guidTime is not null"))
						continue;

					final int tableNumber = db.geti(1);

					db.setReadOnly(true);
					if (!db.query("select count(1) from G" + tableNumber + "L_PFN"))
						continue;

					count = db.geti(1);

					if (count > maxCount) {
						// new G table
						createNewGTable(db, tableNumber + 1);
						continue;
					}

					if (!db.query("select count(1) from G" + tableNumber + "L"))
						count = db.geti(1);

					if (count > maxCount) {
						createNewGTable(db, tableNumber + 1);
						continue;
					}

					if (count > updateDBCount)
						DBSyncUtils.setLastActive(GuidTable.class.getCanonicalName());

					if (count > warnCount) {
						final String admins = ConfigUtils.getConfig().gets("mail_admins", "grigoras@cern.ch"); // comma separated list of emails in config.properties 'mail_admins'
						final Mail m = new Mail();
						m.sBody = "The table G" + tableNumber + "L has passed " + warnCount + " entries and will be soon renewed!";

						DBSyncUtils.registerLog(GuidTable.class.getCanonicalName(), m.sBody);

						if (admins != null && admins.length() > 0) {
							m.sSubject = "JAliEn CS: G table filling up";
							m.sFrom = "JAliEnMaster@cern.ch";
							m.sTo = admins;
							final Sendmail s = new Sendmail(m.sFrom, "cernmx.cern.ch");

							if (!s.send(m))
								logger.log(Level.SEVERE, "Could not send notification email: " + s.sError);
						}

						continue;
					}

					final String dbLog = "The table G" + tableNumber + "L has passed " + count + " entries.";
					DBSyncUtils.registerLog(GuidTable.class.getCanonicalName(), dbLog);
				}

				try {
					logger.log(Level.INFO, "GuidTable sleeps " + this.getSleepPeriod());
					sleep(this.getSleepPeriod());
				}
				catch (final InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

	}

	private void createNewGTable(final DBFunctions db, final int newTable) {
		logger.log(Level.INFO, "GuidTable: creating new table: " + newTable);
		final String admins = ConfigUtils.getConfig().gets("mail_admins", "grigoras@cern.ch"); // comma separated list of emails in config.properties 'mail_admins'

		final Mail m = new Mail();
		m.sBody = "The table G" + newTable + "L has been inserted now.\n\n" + "The previous table had " + count + " entries.";

		if (admins != null && admins.length() > 0) {
			m.sSubject = "JAliEn CS: new G" + newTable + "L tables";
			m.sFrom = "JAliEnMaster@cern.ch";
			m.sTo = admins;
			final Sendmail s = new Sendmail(m.sFrom, "cernmx.cern.ch");

			if (!s.send(m))
				logger.log(Level.SEVERE, "Could not send notification email: " + s.sError);
		}

		DBSyncUtils.registerLog(GuidTable.class.getCanonicalName(), m.sBody);

		String sql = "CREATE TABLE `G" + newTable + "L` (" + " " + "`guidId` int(11) NOT NULL AUTO_INCREMENT," + " `ctime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
				+ " `owner` varchar(20) COLLATE latin1_general_cs DEFAULT NULL," + " `ref` int(11) DEFAULT '0'," + " `jobid` bigint DEFAULT NULL,"
				+ " `seStringlist` varchar(255) COLLATE latin1_general_cs NOT NULL DEFAULT ','," + " `seAutoStringlist` varchar(255) COLLATE latin1_general_cs NOT NULL DEFAULT ',',"
				+ " `aclId` int(11) DEFAULT NULL," + " `expiretime` datetime DEFAULT NULL," + " `size` bigint(20) NOT NULL DEFAULT '0',"
				+ " `gowner` varchar(20) COLLATE latin1_general_cs DEFAULT NULL," + " `guid` binary(16) DEFAULT NULL," + " `type` char(1) COLLATE latin1_general_cs DEFAULT NULL,"
				+ " `md5` varchar(32) COLLATE latin1_general_cs DEFAULT NULL," + " `perm` char(3) COLLATE latin1_general_cs DEFAULT NULL," + " PRIMARY KEY (`guidId`)," + " UNIQUE KEY `guid` (`guid`),"
				+ " KEY `seStringlist` (`seStringlist`)," + " KEY `ctime` (`ctime`)" + ") ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;";

		db.setReadOnly(false);
		if (!db.query(sql)) {
			logger.log(Level.SEVERE, "Could not create table: G" + newTable + "L: " + sql);
			return;
		}
		logger.log(Level.INFO, sql);

		sql = "CREATE TABLE `G" + newTable + "L_PFN` (" + "  `guidId` int(11) NOT NULL," + "  `pfn` varchar(255) COLLATE latin1_general_cs DEFAULT NULL," + "  `seNumber` int(11) NOT NULL,"
				+ "  KEY `guid_ind` (`guidId`)," + "  KEY `seNumber` (`seNumber`)," + "  CONSTRAINT `G" + newTable
				+ "L_PFN_ibfk_1` FOREIGN KEY (`seNumber`) REFERENCES `SE` (`seNumber`) ON DELETE CASCADE," + "  CONSTRAINT `G" + newTable + "L_PFN_ibfk_2` FOREIGN KEY (`guidId`) REFERENCES `G"
				+ newTable + "L` (`guidId`) ON DELETE CASCADE) " + " ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;";

		db.setReadOnly(false);
		if (!db.query(sql)) {
			logger.log(Level.SEVERE, "Could not create table: G" + newTable + "L_PFN: " + sql);
			db.query("DROP TABLE IF EXISTS G" + newTable + "L");
			return;
		}
		logger.log(Level.INFO, sql);

		sql = "CREATE TABLE `G" + newTable + "L_QUOTA` (" + "  `user` varchar(64) COLLATE latin1_general_cs NOT NULL," + "  `nbFiles` int(11) NOT NULL," + "  `totalSize` bigint(20) NOT NULL,"
				+ "  KEY `user_ind` (`user`)" + ") ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;";

		db.setReadOnly(false);
		if (!db.query(sql)) {
			logger.log(Level.SEVERE, "Could not create table: G" + newTable + "L_QUOTA: " + sql);
			db.query("DROP TABLE IF EXISTS G" + newTable + "L_PFN, G" + newTable + "L");
			return;
		}
		logger.log(Level.INFO, sql);

		sql = "CREATE TABLE `G" + newTable + "L_REF` (" + "  `guidId` int(11) NOT NULL," + "  `lfnRef` varchar(20) COLLATE latin1_general_cs NOT NULL," + "  KEY `guidId` (`guidId`),"
				+ "  KEY `lfnRef` (`lfnRef`)," + "  CONSTRAINT `G" + newTable + "L_REF_ibfk_1` FOREIGN KEY (`guidId`) REFERENCES `G" + newTable + "L` (`guidId`) ON DELETE CASCADE"
				+ ") ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;";

		db.setReadOnly(false);
		if (!db.query(sql)) {
			logger.log(Level.SEVERE, "Could not create table: G" + newTable + "L_REF: " + sql);
			db.query("DROP TABLE IF EXISTS G" + newTable + "L_PFN, G" + newTable + "L_QUOTA, G" + newTable + "L");
			return;
		}
		logger.log(Level.INFO, sql);

		// Inserting in 10min from now in the new tables
		final UUID uuid = GUIDUtils.generateTimeUUID(System.currentTimeMillis() + 10 * 60 * 1000);
		final String toInsert = GUIDUtils.getIndexTime(uuid).concat("00000000");

		sql = "INSERT INTO GUIDINDEX values (0, 8, " + newTable + ", '" + toInsert + "', NULL)";
		db.setReadOnly(false);
		if (!db.query(sql)) {
			logger.log(Level.SEVERE, "Could not insert into GUIDINDEX: " + sql);
			db.query("DROP TABLE IF EXISTS G" + newTable + "L_REF, G" + newTable + "L_PFN, G" + newTable + "L_QUOTA, G" + newTable + "L");
			return;
		}
		logger.log(Level.INFO, sql);

	}

}
