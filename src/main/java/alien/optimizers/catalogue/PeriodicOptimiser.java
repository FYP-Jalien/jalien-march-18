package alien.optimizers.catalogue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;
import alien.optimizers.Optimizer;
import alien.user.LDAPHelper;
import lazyj.DBFunctions;

/**
 * @author Marta
 * @since May 3, 2021
 */
public class PeriodicOptimiser extends Optimizer {

	/**
	 * Optimizer synchronisations
	 */
	static final Object requestSync = new Object();

	/**
	 * Logging facility
	 */
	static final Logger logger = ConfigUtils.getLogger(PeriodicOptimiser.class.getCanonicalName());

	/**
	 * Periodic synchronization boolean
	 */
	private static AtomicBoolean periodic = new AtomicBoolean(true);

	String[] classnames = { "users", "roles", "SEs" };

	@Override
	public void run() {
		this.setSleepPeriod(20 * 1000); // 1min
		logger.log(Level.INFO, "DB periodic optimizer starts");
		try (DBFunctions usersdb = ConfigUtils.getDB("alice_users"); DBFunctions admindb = ConfigUtils.getDB("ADMIN");) {
			if (usersdb == null || admindb == null) {
				logger.log(Level.INFO, "Could not get DBs!");
				return;
			}
			checkLdapSyncTable(usersdb);
			while (true) {

				resyncLDAP(usersdb, admindb);

				try {
					synchronized (requestSync) {
						logger.log(Level.INFO, "Periodic sleeps " + this.getSleepPeriod());
						requestSync.wait(this.getSleepPeriod());
					}
				}
				catch (final InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * Creates the LDAP_SYNC table if it did not exist before
	 */
	public static void checkLdapSyncTable(DBFunctions db) {
		String sqlLdapDB = "CREATE TABLE IF NOT EXISTS `LDAP_SYNC` (`class` varchar(15) COLLATE latin1_general_cs NOT NULL, "
				+ "`lastUpdate` double NOT NULL, `frequency` int(11) NOT NULL, PRIMARY KEY (`class`)) "
				+ "ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;";
		if (!db.query(sqlLdapDB)) {
			logger.log(Level.SEVERE, "Could not create table: LDAP_SYNC " + sqlLdapDB);
			return;
		}
	}

	/**
	 * Manual instruction for the resyncLDAP
	 */
	public static void manualResyncLDAP() {
		try (DBFunctions usersdb = ConfigUtils.getDB("alice_users"); DBFunctions admindb = ConfigUtils.getDB("ADMIN");) {
			if (usersdb == null || admindb == null) {
				logger.log(Level.INFO, "Could not get DBs!");
				return;
			}
			checkLdapSyncTable(usersdb);
			synchronized (requestSync) {
				logger.log(Level.INFO, "Started manual resyncLDAP");
				periodic.set(false);
				requestSync.notifyAll();
			}
		}
	}

	/**
	 * Performs the resyncLDAP for the users, roles and SEs
	 *
	 * @param usersdb Database instance for SE, SE_VOLUMES and LDAP_SYNC tables
	 * @param admindb Database instance for USERS_LDAP and USERS_LDAP_ROLE tables
	 */
	private void resyncLDAP(DBFunctions usersdb, DBFunctions admindb) {
		Long timestamp = Long.valueOf(System.currentTimeMillis());
		final int frequency = 60000; // To change

		logger.log(Level.INFO, "Checking if an LDAP resynchronisation is needed");
		usersdb.setReadOnly(false);
		for (String classname : classnames) {
			usersdb.query("SELECT count(*) from `LDAP_SYNC` WHERE class = ?", false, classname);
			if (usersdb.moveNext()) {
				int valuecounts = usersdb.geti(1);
				boolean updated = false;
				if (valuecounts == 0) {
					updated = usersdb.query("INSERT INTO LDAP_SYNC (class, lastUpdate, frequency) VALUES (?, ?, ?)",
							false, classname, timestamp, Integer.valueOf(frequency));
				}
				else {
					Long lastUpdated = Long.valueOf(System.currentTimeMillis() - frequency);
					if (periodic.get())
						updated = usersdb.query("UPDATE LDAP_SYNC SET lastUpdate = ? WHERE class = ? AND lastUpdate < ?",
								false, timestamp, classname, lastUpdated) && usersdb.getUpdateCount() > 0;
					else
						updated = usersdb.query("UPDATE LDAP_SYNC SET lastUpdate = ? WHERE class = ?",
								false, timestamp, classname) && usersdb.getUpdateCount() > 0;
				}
				if (updated) {
					switch (classname) {
						case "users":
							updateUsers(admindb);
							break;
						case "roles":
							updateRoles(admindb);
							break;
						case "SEs":
							updateSEs(usersdb);
							break;
						default:
							break;
					}
					deleteInactive(admindb);
				}
			}
		}
		periodic.set(true);
	}

	/**
	 * Updates the users in the LDAP database
	 *
	 * @param Database instance
	 */
	public static void updateUsers(DBFunctions db) {
		logger.log(Level.INFO, "Synchronising DB users with LDAP");
		String ouHosts = "ou=People,";
		db.query("UPDATE USERS_LDAP SET up=0");

		// Insertion of the users
		final Set<String> uids = LDAPHelper.checkLdapInformation("(objectClass=pkiUser)", ouHosts, "uid", false);
		int length = uids.size();
		logger.log(Level.INFO, "Inserting " + length + " users");
		if (length == 0)
			logger.log(Level.WARNING, "No users gotten from LDAP");

		for (String user : uids) {
			final Set<String> dns = LDAPHelper.checkLdapInformation("uid=" + user, ouHosts, "subject", false);
			for (String dn : dns) {
				db.query("REPLACE INTO USERS_LDAP (user, dn, up) VALUES (?, ?, 1)", false, user, dn);
			}
		}
	}

	/**
	 * Updates the roles in the LDAP database
	 *
	 * @param Database instance
	 */
	public static void updateRoles(DBFunctions db) {
		logger.log(Level.INFO, "Synchronising DB roles with LDAP");
		String ouRoles = "ou=Roles,";
		db.query("UPDATE USERS_LDAP_ROLE SET up=0");

		// Insertion of the roles
		final Set<String> roles = LDAPHelper.checkLdapInformation("(objectClass=AliEnRole)", ouRoles, "uid", false);
		int length = roles.size();
		logger.log(Level.INFO, "Inserting " + length + " roles");
		if (length == 0)
			logger.log(Level.WARNING, "No roles gotten from LDAP");

		for (String user : roles) {
			final Set<String> dns = LDAPHelper.checkLdapInformation("uid=" + user, ouRoles, "users", false);
			for (String dn : dns) {
				db.query("REPLACE INTO USERS_LDAP_ROLE (user, role, up) VALUES (?, ?, 1)", false, dn, user);
			}
		}
		//Here it performs something of adding users ... Needed?
		/* $self->info("And let's add the new users");
    	my $newUsers=$addbh->queryColumn("select a.". $addbh->reservedWord("user")." from USERS_LDAP a left join USERS_LDAP b on b.up=0 and
 			a.". $addbh->reservedWord("user")."=b.". $addbh->reservedWord("user")." where a.up=1 and b.". $addbh->reservedWord("user")." is null");
    	foreach my $u (@$newUsers){
      	$self->info("Adding the user $u");
      	$self->f_addUser($u);
    }*/
	}

	/**
	 * Updates the SEs and SE_VOLUMES in the LDAP database
	 *
	 * @param Database instance
	 */
	public static void updateSEs(DBFunctions db) {
		logger.log(Level.INFO, "Synchronising DB SEs and volumes with LDAP");
		String ouSE = "ou=Sites,";
//		ArrayList<String> oldEntries = new ArrayList<>();
		ArrayList<String> newSEs = new ArrayList<>();

		final Set<String> dns = LDAPHelper.checkLdapInformation("(objectClass=AliEnSE)", ouSE, "dn", true);
		ArrayList<String> seNames = new ArrayList<>();
		ArrayList<String> sites = new ArrayList<>();

		//From the dn we get the seName and site
		Iterator<String> itr = dns.iterator();
		while (itr.hasNext()) {
			String dn = itr.next();
			if (dn.contains("disabled")) {
				logger.log(Level.WARNING, "Skipping " + dn + " (it is disabled)");
				continue;
			}
			String[] entries = dn.split("[=,]");
			if (entries.length >= 8) {
				seNames.add(entries[1]);
				sites.add(entries[7]);
			}
		}
		int length = seNames.size();
		if (length == 0)
			logger.log(Level.WARNING, "No ses gotten from LDAP");

		for (int ind = 0; ind < sites.size(); ind++) {
			String site = sites.get(ind);
			String se = seNames.get(ind);

			//This will be the base dn for the SE
			String ouSe = "ou=SE,ou=Services,ou=" + site + ",ou=Sites,";

			String vo = "ALICE";
			String seName = vo + "::" + site + "::" + se;

			checkSEDescription(db, ouSe, se, seName);
			checkIODaemons(db, ouSe, se, seName);
			// checkFTDProtocol(ouSE, se, db);

			final String t = getLdapContentSE(ouSE, se, "mss");
			final String host = getLdapContentSE(ouSE, se, "host");
			newSEs.add(seName);

			final Set<String> savedir = LDAPHelper.checkLdapInformation("name=" + se, ouSE, "savedir");
			for (String path : savedir) {
				boolean found = false;
				String size = "-1";
				logger.log(Level.INFO, "Checking the path of " + path);
				if (path.matches(".*,\\d+")) {
					size = path.split(",")[1];
					path = path.split(",")[0];
				}

				db.query("select * from SE_VOLUMES where upper(seName)=upper(?)", true, seName);
				while (db.moveNext()) {
					String mountpoint = db.gets("mountpoint");
					newSEs.remove(seName);
					if (!mountpoint.equals(path))
						continue;
					found = true;
					logger.log(Level.INFO, "The path already existed");
					String sizedb = db.gets("sizedb");
					if (sizedb.equals(size))
						continue;
					logger.log(Level.INFO, "The size is different (" + size + " and " + sizedb + ")");
					db.query("update SE_VOLUMES set size=? where mountpoint=? and sename=?", false, size, mountpoint, seName);
				}

				if (found)
					continue;

				/*
				 * if (host == null || host == "") {
				 * logger.log(Level.INFO, "The host did not exist. Going to check if it can be gotten from its parent");
				 * // do I have to check that? And from where do I take the dn?
				 * }
				 */
				logger.log(Level.INFO, "Need to add the volume " + path);
				String method = t.toLowerCase() + "://" + host;
				db.query("insert into SE_VOLUMES(sename,volume,method,mountpoint,size) values (?,?,?,?,?)", false,
						seName, path, method, path, size);
			}
			/*
			 * for (String oldEntry : oldEntries) {
			 * logger.log(Level.INFO, "The path " + oldEntry + " is not used any more");
			 * db.query("update SE_VOLUMES set size=usedspace where mountpoint=? and upper(sename)=upper(?)",
			 * false, oldEntry, seName);
			 * }
			 */

		}
		for (String newSE : newSEs) {
			logger.log(Level.INFO, "The SE " + newSE + " is new. We have to add it");
			db.query("SELECT max(seNumber)+1 FROM SE", false);
			int seNumber = 0;
			if (db.moveNext()) {
				seNumber = db.geti(1);
				logger.log(Level.INFO, "SE number " + seNumber + " for se " + newSE);
			}

			if (!db.query("insert into SE(seName, seNumber) values (?, ?)", false, newSE, Integer.valueOf(seNumber))) {
				logger.log(Level.INFO, "Error adding the entry");
				return;
			}
			logger.log(Level.INFO, "Added entry");
		}
		db.query("update SE_VOLUMES set usedspace=0 where usedspace is null");
		db.query("update SE_VOLUMES set freespace=size-usedspace where size <> -1");
		db.query("update SE_VOLUMES set freespace=size-usedspace where size <> -1");
	}

	/*
	 * private static void checkFTDProtocol(String ouSE, String se, DBFunctions db) {
	 * final Set<String> ftdprotocol = LDAPHelper.checkLdapInformation("name="+se, ouSE, "ftdprotocol");
	 * logger.log(Level.INFO, "DBG: Gotten the ldap ftdprotocol info " + se + " " + ftdprotocol.toString());
	 * for (String protocol : ftdprotocol) {
	 * logger.log(Level.INFO, "DBG: Protocol " + protocol);
	 * String[] splitted = protocol.split(" +");
	 * logger.log(Level.INFO, "DBG: Protocol array length" + splitted.length);
	 * String name = splitted[0];
	 * logger.log(Level.INFO, "DBG: Protocol array " + splitted[0]);
	 * String options = "";
	 * if (splitted.length > 1)
	 * options = splitted[1];
	 * logger.log(Level.INFO, "Inserting " + name + " and " + options);
	 * String maxTransfers = "0";
	 * if (options != null && options != "") {
	 * if (options.matches("\\s*transfers=(\\d+)\\s*")) {
	 * maxTransfers = options.split("=")[1];
	 * //Does something more with options
	 * }
	 * }
	 * //insert Protocol function
	 * //db.query("insert into PROTOCOLS (sename, protocol, maxtransfers) values (?, ?, ?)", false, se, name, maxTransfers);
	 * }
	 * final Set<String> deleteprotocol = LDAPHelper.checkLdapInformation("name="+se, ouSE, "deleteprotocol");
	 * logger.log(Level.INFO, "DBG: Gotten the ldap deleteprotocol info " + se + " " + deleteprotocol.toString());
	 * for (String protocol : deleteprotocol) {
	 * String name = protocol.split(" ")[0];
	 * //db.query("insert into PROTOCOLS (sename, protocol, deleteprotocol) values (?, ?, ?)", false, se, name, 1);
	 * //insert Protocol function -- WHERE IS THIS FUNCTION
	 * }
	 * }
	 */

	/**
	 * Gets the IODaemons from LDAP and adds them to the database
	 *
	 * @param Database instance
	 * @param ouSE Prefix for the DN of the SE
	 * @param se SE name in the LDAP system
	 * @param seName SE identifier in the database (vo:site:se)
	 */
	private static void checkIODaemons(DBFunctions db, String ouSE, String se, String seName) {
		final String iodaemons = getLdapContentSE(ouSE, se, "iodaemons");
		String[] temp = iodaemons.split(":");
		String seioDaemons = "NULL";
		if (temp.length > 2) {
			String proto = temp[0];
			String host = "";
			String port = "";
			host = temp[1];
			if (!host.matches("host=([^:]+)(:.*)?$")) {
				logger.log(Level.INFO, "Error getting the host name from " + seName);
				return;
			}
			host = host.split("=")[1];

			port = temp[2];
			if (!port.matches("port=(\\d+)")) {
				logger.log(Level.INFO, "Error getting the port for " + seName);
				return;
			}
			port = port.split("=")[1];

			// Missing something with the proto variable
			seioDaemons = proto + "://" + host + ":" + port;
			logger.log(Level.INFO, "Using proto = " + proto + " host = " + host + " and port = " + port + " for " + seName);
		}

		final String savedir = getLdapContentSE(ouSE, se, "savedir");
		if (savedir.equals("")) {
			logger.log(Level.INFO, "Error getting the savedir for " + seName);
			return;
		}

		logger.log(Level.INFO, "And the update for " + seName + "should be: " + seioDaemons + ", " + savedir);

		// Missing something with the path variable ( $path=~ s/,.*$//; )
		String path = savedir;

		//db.query("SELECT sename,seioDaemons,sestoragepath from SE where upper(seName)=upper(?)",
		//		false, seName);

		if (path.matches(".*,\\d+")) {
			path = path.split(",")[0];
		}
		// Is anything missing in path2 variable?
		String path2 = "/";

		db.query("SELECT count(*) from SE where upper(seName)=upper(?) and seioDaemons=? "
				+ "and ( seStoragePath=? or sestoragepath=?)", false, seName, seioDaemons, path, path2);

		if (db.moveNext()) {
			int valuecounts = db.geti(1);
			logger.log(Level.INFO, "***Updating the information of " + seName + " with " + valuecounts +
					" counts");
			db.query("SELECT seNumber from SE where upper(seName)=upper(?)", false, seName);

			if (db.moveNext()) {
				int seNumber = db.geti(1);
				if (seNumber == -1) {
					logger.log(Level.INFO, "The se " + seName + " does not exist");
				} else {
					if (!db.query("UPDATE SE set seName=?, seStoragePath=?, seioDaemons=? WHERE upper(seName)=upper(?)",
							false, seName, path, seioDaemons, seName)) {
						logger.log(Level.INFO, "Error updating " + seName + " with seStoragePath " + path
								+ " and seioDaemons " + seioDaemons);
					}
				}
			}
		}
	}

	/**
	 * Gets the SE description from LDAP and adds it to the database
	 *
	 * @param Database instance
	 * @param ouSE Prefix for the DN of the SE
	 * @param se SE name in the LDAP system
	 * @param seName SE identifier in the database (vo:site:se)
	 */
	private static void checkSEDescription(DBFunctions db, String ouSE, String se, String seName) {
		String minSize = "0";

		final Set<String> options = LDAPHelper.checkLdapInformation("name=" + se, ouSE, "options");
		for (String option : options) {
			if (option.matches("min_size\\s*=\\s*(\\d+)")) {
				minSize = option.split("=")[1];
			}
		}

		final String mss = getLdapContentSE(ouSE, se, "mss");
		final String qos = getLdapContentSE(ouSE, se, "Qos");
		final String seExclusiveWrite = getLdapContentSE(ouSE, se, "seExclusiveWrite");
		final String seExclusiveRead = getLdapContentSE(ouSE, se, "seExclusiveRead");
		final String seVersion = getLdapContentSE(ouSE, se, "seVersion");

		logger.log(Level.INFO, "The se " + seName + " has " + minSize + " and " + mss + " and " + qos +
				" and ex-write: " + seExclusiveWrite + " and ex-rad: " + seExclusiveRead);

		db.query("select count(*) from SE where upper(sename)=upper(?) and seMinSize=? and seType=? and seQoS=? "
				+ "and seExclusiveWrite=? and seExclusiveRead=? and seVersion=?", false, seName, minSize, mss, qos, seExclusiveWrite,
				seExclusiveRead, seVersion);
		if (db.moveNext()) {
			int counts = db.geti(1);
			if (counts == 0) {
				logger.log(Level.INFO, "We have to update the entry");
				db.query("update SE set seMinSize=?, seType=?, seQoS=?, seExclusiveWrite=?, seExclusiveRead=? , seVersion=? where seName=?",
						false, minSize, mss, qos, seExclusiveWrite, seExclusiveRead, seVersion, seName);
			}
		}
	}

	private static String getLdapContentSE(String ouSE, String se, String parameter) {
		final Set<String> param = LDAPHelper.checkLdapInformation("name=" + se, ouSE, parameter);
		String joined = "";
		if (param.size() > 1) {
			joined = String.join(",", param);
		}
		else if (param.size() == 1) {
			joined = param.iterator().next();
		}
		else if (param.size() == 0) {
			joined = "NULL";
		}
		return joined;
	}

	/**
	 * Deletes non-active users and roles from the LDAP database
	 *
	 * @param Database instance
	 */
	public static void deleteInactive(DBFunctions db) {
		logger.log(Level.INFO, "Deleting inactive users and roles");
		db.query("DELETE FROM USERS_LDAP WHERE up = 0");
		db.query("DELETE FROM USERS_LDAP_ROLE WHERE up = 0");
		// Missing to delete inactive SEs
	}

}
