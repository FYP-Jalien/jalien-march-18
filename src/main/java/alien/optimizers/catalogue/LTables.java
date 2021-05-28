package alien.optimizers.catalogue;

import java.util.logging.Level;
import java.util.logging.Logger;

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
public class LTables extends Optimizer {

	/**
	 * Logging facility
	 */
	static final Logger logger = ConfigUtils.getLogger(LTables.class.getCanonicalName());

	/**
	 * At which point to send a warning by mail
	 */
	final static int maxCount = 50000000; // 50M

	/**
	 * Number of rows in the last table
	 */
	int count;

	@Override
	public void run() {
		this.setSleepPeriod(24 * 3600 * 1000); // 24h

		logger.log(Level.INFO, "LTables optimizer starts");

		DBSyncUtils.checkLdapSyncTable();
		final int frequency = 24 * 3600 * 1000; // 1 day default

		try (DBFunctions db = ConfigUtils.getDB("alice_users"); DBFunctions db2 = ConfigUtils.getDB("alice_users")) {
			if (db == null || db2 == null) {
				logger.log(Level.INFO, "LTables could not get a DB!");
				return;
			}

			while (true) {
				logger.log(Level.INFO, "LTables wakes up!: going to get L tables counts with max: " + maxCount);
				boolean found = false;
				String body = "";
				String dbLog = "";

				boolean updated = DBSyncUtils.updatePeriodic(frequency, LTables.class.getCanonicalName());
				if (updated) {
					// Get count of latest L8 tables
					db.setReadOnly(true);
					db.query("select TABLE_NAME tn,TABLE_ROWS tr from information_schema.tables where TABLE_NAME like 'L%L%' and TABLE_SCHEMA like 'alice_users' order by tr desc");
					while (db.moveNext()) {
						String tableNumber = db.gets(1);
						final long tableRows = db.getl(2);
						tableNumber = tableNumber.replace("L", "");

						if (tableRows > maxCount) {
							found = true;
							db2.setReadOnly(true);
							db2.query("SELECT lfn from INDEXTABLE WHERE hostIndex=8 and tableName=?;", false, tableNumber);
							db2.moveNext();
							body += "Table L" + tableNumber + "L - " + db2.gets(1) + " - " + tableRows + " entries \n";
						}
					}

					// Get count of latest L7 tables
					db.setReadOnly(true);
					db.query("select TABLE_NAME tn,TABLE_ROWS tr from information_schema.tables where TABLE_NAME like 'L%L%' and TABLE_SCHEMA like 'alice_data' order by tr desc");
					while (db.moveNext()) {
						String tableNumber = db.gets(1);
						final long tableRows = db.getl(2);
						tableNumber = tableNumber.replace("L", "");

						if (tableRows > maxCount) {
							found = true;
							db2.setReadOnly(true);
							db2.query("SELECT lfn from INDEXTABLE WHERE hostIndex=7 and tableName=?;", false, tableNumber);
							db2.moveNext();
							body += "Table L" + tableNumber + "L - " + db2.gets(1) + " - " + tableRows + " entries \n";
						}
					}

					if (found) {
						final String admins = ConfigUtils.getConfig().gets("mail_admins"); // comma separated list of emails in config.properties 'mail_admins'
						if (admins != null && admins.length() > 0) {
							final Mail m = new Mail();
							m.sSubject = "JAliEn CS: L tables passed limits";
							m.sBody = "There are LFN tables that passed the " + maxCount + " entries limit: \n\n" + body;
							m.sFrom = "JAliEnMaster@cern.ch";
							m.sTo = admins;
							final Sendmail s = new Sendmail(m.sFrom, "cernmx.cern.ch");

							if (!s.send(m))
								logger.log(Level.SEVERE, "Could not send notification email: " + s.sError);


							dbLog = "There are LFN tables that passed the " + maxCount + " entries limit: \n\n" + body;
						}
					}

					dbLog = "There are no LFN tables that passed the " + maxCount + " entries limit.";
					DBSyncUtils.registerLog(LTables.class.getCanonicalName(), dbLog);
				}

				try {
					logger.log(Level.INFO, "LTables sleeps " + this.getSleepPeriod());
					sleep(this.getSleepPeriod());
				}
				catch (final InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

}
