package alien.optimizers.catalogue;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;
import alien.optimizers.Optimizer;
import alien.user.LDAPHelper;
import lazyj.DBFunctions;

/**
 * @author DBG
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

	boolean periodic = true;

	String[] classnames = { "users", "roles", "SEs" };

	@Override
	public void run() {
		this.setSleepPeriod(60 * 1000); // 1min

		logger.log(Level.INFO, "DB periodic optimizer starts");

		logger.log(Level.INFO, "LDAP optimiser wakes up for the resyncLDAP!");
		try (DBFunctions db = ConfigUtils.getDB("alice_users"); DBFunctions db2 = ConfigUtils.getDB("ADMIN");) {
			if (db == null || db2 == null) {
				logger.log(Level.INFO, "Could not get DBs!");
				return;
			}

			checkLdapSyncTable(db);
			while (true) {
				synchronized (requestSync) {

					resyncLDAP(db, db2);

					try {
						logger.log(Level.INFO, "Periodic sleeps " + this.getSleepPeriod());
						//sleep(this.getSleepPeriod());
						requestSync.wait(this.getSleepPeriod());
					}
					catch (final InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}

	public static void checkLdapSyncTable(DBFunctions db) {
		String sqlLdapDB = "CREATE TABLE IF NOT EXISTS `LDAP_SYNC` (`class` varchar(15) COLLATE latin1_general_cs NOT NULL, "
				+ "`lastUpdate` timestamp NOT NULL, `frequency` int(11) NOT NULL, PRIMARY KEY (`class`)) "
				+ "ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;";
		if (!db.query(sqlLdapDB)) {
			logger.log(Level.SEVERE, "Could not create table: LDAP_SYNC " + sqlLdapDB);
			return;
		}
	}

	public void manualResyncLDAP() {
		try (DBFunctions db = ConfigUtils.getDB("alice_users"); DBFunctions db2 = ConfigUtils.getDB("ADMIN");) {
			if (db == null || db2 == null) {
				logger.log(Level.INFO, "Could not get DBs!");
				return;
			}

			checkLdapSyncTable(db);
			synchronized (requestSync) {
				logger.log(Level.INFO, "Started manual resyncLDAP");
				periodic=false;
				requestSync.notifyAll();
			}
		}
	}
	private void resyncLDAP(DBFunctions db, DBFunctions db2) {
		String timestampString = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Long.valueOf(System.currentTimeMillis()));
		final int frequency = 60000;

		db.setReadOnly(false);
		for (String classname : classnames) {
			db.query("SELECT count(*) from `LDAP_SYNC` WHERE class = ?", false, classname);
			if (db.moveNext()) {
				int valuecounts = Integer.parseInt(db.gets(1));
				boolean updated = false;
				if (valuecounts == 0) {
					db.setQueryTimeout(10);
					updated = db.query("INSERT INTO LDAP_SYNC (class, lastUpdate, frequency) VALUES (?, ?, ?)",
							false, classname, timestampString, Integer.valueOf(frequency));
				}
				else {
					String lastUpdated = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Long.valueOf(System.currentTimeMillis() - frequency));
					if (periodic)
						updated = db.query("UPDATE LDAP_SYNC SET lastUpdate = ? WHERE class = ? AND lastUpdate < ?",
							false, timestampString, classname, lastUpdated);
					else
						updated = db.query("UPDATE LDAP_SYNC SET lastUpdate = ? WHERE class = ?",
								false, timestampString, classname);
				}
				if (updated) {
					//if (classname == "users")
						//updateUsers(db2);
					//if (classname == "roles")
						//updateRoles(db2);
					if (classname == "SEs") {
						logger.log(Level.INFO, "DBG: Updating SEs");
						updateSEs(db);
					}
					//deleteInactive(db2);
				}
			}
		}
		periodic = true;
	}

	/**
	 * Updates the users in the LDAP database
	 *
	 * @param Database instance
	 */
	public static void updateUsers(DBFunctions db) {
		logger.log(Level.INFO, "Synchronising DB users with LDAP");
		String ouHosts = "ou=People,";

		//Insertion of the users
		final Set<String> uids = LDAPHelper.checkLdapInformation("(objectClass=pkiUser)", ouHosts, "uid", false);
		int length = uids.size();
		if (length == 0)
			logger.log(Level.WARNING, "No users gotten from LDAP");

		for (String user : uids) {
			final Set<String> dns = LDAPHelper.checkLdapInformation("uid="+user, ouHosts, "subject", false);
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

		//Insertion of the roles
		final Set<String> roles = LDAPHelper.checkLdapInformation("(objectClass=AliEnRole)", ouRoles, "uid", false);
		int length = roles.size();
		if (length == 0)
			logger.log(Level.WARNING, "No roles gotten from LDAP");

		for (String user : roles) {
			final Set<String> dns = LDAPHelper.checkLdapInformation("uid="+user, ouRoles, "users", false);
			for (String dn : dns) {
				db.query("REPLACE INTO USERS_LDAP_ROLE (user, role, up) VALUES (?, ?, 1)", false, user, dn);
			}
		}
	}

	/**
	 * Updates the SEs and SE_VOLUMES in the LDAP database
	 *
	 * @param Database instance
	 */
	public static void updateSEs(DBFunctions db) {
		logger.log(Level.INFO, "Synchronising DB SEs and volumes with LDAP");
		String ouSE = "ou=SE,ou=Services,ou=CERN,ou=Sites,";

		ArrayList<String> oldEntries = new ArrayList<>();
		ArrayList<String> newSEs = new ArrayList<>();


		//Insertion of the roles
		final Set<String> ses = LDAPHelper.checkLdapInformation("(objectClass=AliEnMSS)", ouSE, "name", false);
		int length = ses.size();
		if (length == 0)
			logger.log(Level.WARNING, "No roles gotten from LDAP");

		//updates PROTOCOLS db

		for (String se : ses) {
			String seEntry = "name=" + se + "," + ouSE;
			HashMap<String, Object> ldapTree = LDAPHelper.checkLdapTree("(objectClass=AliEnMSS)", seEntry);
			for (String key : ldapTree.keySet()) {
				logger.log(Level.INFO, "DBG: Inspecting tree for key " + key + " value " + ldapTree.get(key));
			}

			//Don't know how to get the vo and site parameters, the dn key is not returned. Is the "alice" constant?
			//String seEntry = vo + "::" + site + "::" + se;
			logger.log(Level.INFO, "DBG: Final ouSE is " + seEntry);
			checkSEDescription(db, ouSE, se);
			checkIODaemons(db, ouSE, se);
 			checkFTDProtocol(ouSE, se, db);

			final Set<String> savedir = LDAPHelper.checkLdapInformation("name="+se, ouSE, "savedir");
			logger.log(Level.INFO, "DBG: After functions completition ldap savedir info " + se + " " + savedir.toString());

			db.query("select * from SE_VOLUMES where upper(sename)=upper(?)", false, se);
			final String t = getLdapContentSE(ouSE, se, "mss");
			final String host = getLdapContentSE(ouSE, se, "host");

			logger.log(Level.INFO, "DBG: Going to iterate through the savedir paths");

			for (String path : savedir) {
				logger.log(Level.INFO, "DBG: Path = " + path);
				oldEntries.clear();
				boolean found = false;
				String size = "-1";
				logger.log(Level.INFO, "DBG: Path matches " + path.matches(".*,\\d+"));
				if (path.matches(".*,\\d+")) {
					size = path.split(",")[1];
					path = path.split(",")[0];
					logger.log(Level.INFO, "DBG: Size of path is " + size);
				}
				logger.log(Level.INFO, "Checking the path of " + path);
				while (db.moveNext()) {
					String mountpoint = db.gets("mountpoint");
					if (mountpoint != path) {
						logger.log(Level.INFO, "The path already existed");
						found = true;
						continue;
					}
					oldEntries.add(mountpoint);
					String sizedb = db.gets("sizedb");
					if (sizedb == path) {
						continue;
					}

					logger.log(Level.INFO, "The size is different (" + size + " and " + sizedb);
					db.query("update SE_VOLUMES set size=? where mountpoint=? and sename=?",
							false, size, mountpoint, se);
				}
				if (found) {
					continue;
				}

				logger.log(Level.INFO, "Need to add the se " + path);

				if (host == null || host == "") {
					logger.log(Level.INFO, "The host did not exist. Going to check if it can be gotten from its parent");
					// do I have to check that? And from where do I take the dn?
				}
				String method = t.toLowerCase() + "://" + host;
				db.query("insert into SE_VOLUMES(sename,volume,method, mountpoint, size) values (?,?,?,?,?)", false,
						se, path, method, path, size);

				if(!newSEs.contains(se))
					newSEs.add(se);
			}
			for (String oldEntry : oldEntries) {
				logger.log(Level.INFO, "The path " + oldEntry + " is not used any more");
				db.query("update SE_VOLUMES set size=usedspace where mountpoint=? and upper(sename)=upper(?)",
						false, oldEntry, se);
			}

		}
		for (String newSE : newSEs) {
			logger.log(Level.INFO, "The SE " + newSE + " is new. We have to add it");
			boolean success = db.query("SELECT max(seNumber)+1 FROM SE", false);
			logger.log(Level.INFO, "DBG: The SE query was successful? " + success);
			int seNumber = 0;
			if (db.moveNext()) {
				seNumber = db.geti(1);
				logger.log(Level.INFO, "SE number " + seNumber + " for se " + newSE);
			}
			logger.log(Level.INFO, "DBG: Before inserting SE");
			if (!db.query("insert into SE(seName, seNumber) values (?, ?)", false, newSE, Integer.valueOf(seNumber))) {
				logger.log(Level.INFO, "Error adding the entry");
				db.query("delete from SE where upper(seName)=upper(?) and seNumber=?", false, newSE, Integer.valueOf(seNumber));
			}
			logger.log(Level.INFO, "Added entry");
		}
		db.query("update SE_VOLUMES set usedspace=0 where usedspace is null");
		db.query("update SE_VOLUMES set freespace=size-usedspace where size <> -1");
		db.query("update SE_VOLUMES set freespace=size-usedspace where size <> -1");

		//Changes in PROTOCOLS db
	}

	private static String findDnField(Set<String> dnSet, String param, int count) {
		int n = 0;
		logger.log(Level.INFO, "DBG: Iterating in " + dnSet);
		for (Iterator<String> it = dnSet.iterator(); it.hasNext(); ) {
		    String f = it.next();
		    logger.log(Level.INFO, "DBG: Param iter " + f);
		    if (f.contains(param) && n == count) {
		    	logger.log(Level.INFO, "DBG: Returning output " + f + " for param " + param);
		        return f;
		    } else if (f.contains(param) && n < count) {
		    	logger.log(Level.INFO, "DBG: Not yet in count");
		    	n += 1;
		    }
		}
		logger.log(Level.INFO, "DBG: Returning empty output for param " + param);
		return "";
	}

	private static void checkFTDProtocol(String ouSE, String se, DBFunctions db) {
		final Set<String> ftdprotocol = LDAPHelper.checkLdapInformation("name="+se, ouSE, "ftdprotocol");
		logger.log(Level.INFO, "DBG: Gotten the ldap ftdprotocol info " + se + " " + ftdprotocol.toString());
		for (String protocol : ftdprotocol) {
			logger.log(Level.INFO, "DBG: Protocol " + protocol);
			String[] splitted = protocol.split(" +");
			logger.log(Level.INFO, "DBG: Protocol array length" + splitted.length);
			String name = splitted[0];
			logger.log(Level.INFO, "DBG: Protocol array " + splitted[0]);
			String options =  "";
			if (splitted.length > 1)
				options = splitted[1];
			logger.log(Level.INFO, "Inserting " + name + " and " + options);
			String maxTransfers = "0";
			if (options != null && options != "") {
				if (options.matches("\\s*transfers=(\\d+)\\s*")) {
					maxTransfers = options.split("=")[1];
					//Does something more with options
				}
			}
			//insert Protocol function
			//db.query("insert into PROTOCOLS (sename, protocol, maxtransfers) values (?, ?, ?)", false, se, name, maxTransfers);
		}
		final Set<String> deleteprotocol = LDAPHelper.checkLdapInformation("name="+se, ouSE, "deleteprotocol");
		logger.log(Level.INFO, "DBG: Gotten the ldap deleteprotocol info " + se + " " + deleteprotocol.toString());
		for (String protocol : deleteprotocol) {
			String name = protocol.split(" ")[0];
			//db.query("insert into PROTOCOLS (sename, protocol, deleteprotocol) values (?, ?, ?)", false, se, name, 1);
			//insert Protocol function -- WHERE IS THIS FUNCTION
		}
	}

	private static void checkIODaemons(DBFunctions db, String ouSE, String se) {
		final String iodaemons = getLdapContentSE(ouSE, se, "iodaemons");
		logger.log(Level.INFO, "DBG: Gotten the ldap options info " + se + " " + iodaemons);
		String[] temp = iodaemons.split(":");
		String proto = temp[0];
		String host = "";
		if (temp.length > 1) {
			host = temp[1];
			if (!host.matches("host=([^:]+)(:.*)?$"))
				logger.log(Level.INFO, "Error getting the host name from " + se);
			host=host.split("=")[1];
			logger.log(Level.INFO, "DBG: Gotten the ldap se iod host " + host);
		}
		String port = temp[2];
		if (!port.matches("port=(\\d+)"))
			logger.log(Level.INFO, "Error getting the port for " + se);
		port=port.split("=")[1];
		logger.log(Level.INFO, "DBG: Gotten the ldap se iod port " + port);

		logger.log(Level.INFO, "Using proto = " + proto + " host = " + host + " and port = " + port + " for " + se);

		//Missing something with the proto variable

		final String savedir = getLdapContentSE(ouSE, se, "savedir");
		if (savedir == "")
			logger.log(Level.INFO, "Error getting the savedir for " + se);

		String seioDaemons = proto + "://" + host + ":" + port;
		logger.log(Level.INFO, "And the update should " + se + " be: " + seioDaemons + ", " + savedir);

		//Missing something with the path variable
		String path = savedir;

		db.query("SELECT sename,seioDaemons,sestoragepath from SE where upper(seName)=upper(?)",
				false, se);

		if (path.matches(".*,\\d+")) {
			path = path.split(",")[0];
		}
		//Missing path2 variable
		String path2 = "/";


		db.query("SELECT count(*) from SE where upper(seName)=upper(?) and seioDaemons=? "
				+ "and ( seStoragePath=? or sestoragepath=?)", false, se, seioDaemons, path, path2);

		if (db.moveNext()) {
			int valuecounts = Integer.parseInt(db.gets(1));
			logger.log(Level.INFO, "***Updating the information of " + se + " with " + valuecounts +
					" counts");
			db.query("SELECT seNumber from SE where upper(seName)=upper(?)", false, se);
			int seNumber = -1;
			if (db.moveNext()) {
				seNumber = Integer.parseInt(db.gets(1));
				if (seNumber == -1)	{
					logger.log(Level.INFO, "The se " + se + " does not exist");
				} else {
					if (!db.query("UPDATE SE set seName=?, seStoragePath=?, seioDaemons=? WHERE upper(seName)=upper(?)",
							false, se, path, seioDaemons, se)) {
						logger.log(Level.INFO, "Error updating " + se + " with seStoragePath " + path
								+ " and seioDaemons " + seioDaemons);
					}
				}
			}
		}
	}


	private static void checkSEDescription(DBFunctions db, String ouSE, String se) {
		String minSize = "0";

		final Set<String> options = LDAPHelper.checkLdapInformation("name="+se, ouSE, "options");
		logger.log(Level.INFO, "DBG: Gotten the ldap options info " + se + " " + options.toString());
		for (String option : options) {
			logger.log(Level.INFO, "DBG: Gotten the ldap option " + option);
			if (option.matches("min_size\\s*=\\s*(\\d+)")) {
				minSize = option;
			}
		}
		logger.log(Level.INFO, "DBG: Gotten minsize " + minSize);

		final String mss = getLdapContentSE(ouSE, se, "mss");
		final String qos = getLdapContentSE(ouSE, se, "Qos");
		final String seExclusiveWrite = getLdapContentSE(ouSE, se, "seExclusiveWrite");
		final String seExclusiveRead = getLdapContentSE(ouSE, se, "seExclusiveRead");
		final String seVersion = getLdapContentSE(ouSE, se, "seVersion");


		db.query("select count(*) from SE where upper(sename)=upper(?) and seminsize=? and setype=? and seqos=? "
				+ "and seExclusiveWrite=? and seExclusiveRead=? and seVersion=?", false, se, minSize, mss, qos, seExclusiveWrite,
				seExclusiveRead, seVersion);
		if (db.moveNext()) {
			int counts =db.geti(1);

			if (counts == 0) {
				logger.log(Level.INFO, "We have to update the entry");
				db.query("update SE set seminsize=?, setype=?, seqos=?, seExclusiveWrite=?, seExclusiveRead=? , seVersion=? where sename=?",
						false, se, minSize, mss, qos, seExclusiveWrite, seExclusiveRead, seVersion);
				//TODO: Check if this is being updated
			}
		}
	}

	private static String getLdapContentSE(String ouSE, String se, String parameter) {
		final Set<String> param = LDAPHelper.checkLdapInformation("name="+se, ouSE, parameter);
		String joined = "";
		if (param.size() > 1) {
			joined = String.join(",", param);
		} else if (param.size() == 1){
			joined = param.iterator().next();
		} else if (param.size() == 0) {
			joined = "";
		}
		logger.log(Level.INFO, "DBG: Gotten the ldap " + parameter + " info " + se + " " + joined);
		return joined;
	}

	/**
	 * Deletes non-active users and roles from the LDAP database
	 *
	 * @param Database instance
	 */
	public static void deleteInactive(DBFunctions db) {
		db.query("DELETE FROM USERS_LDAP WHERE up = 0");
		db.query("DELETE FROM USERS_LDAP_ROLE WHERE up = 0");
	}

}
