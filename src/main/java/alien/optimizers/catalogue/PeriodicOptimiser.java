package alien.optimizers.catalogue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.config.ConfigUtils;
import alien.optimizers.DBSyncUtils;
import alien.optimizers.Optimizer;
import alien.user.AliEnPrincipal;
import alien.user.LDAPHelper;
import alien.user.UsersHelper;
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
	static final Object backRequestSync = new Object();

	/**
	 * Logging facility
	 */
	static final Logger logger = ConfigUtils.getLogger(PeriodicOptimiser.class.getCanonicalName());

	/**
	 * Periodic synchronization boolean
	 */
	private static AtomicBoolean periodic = new AtomicBoolean(true);

	private static String[] classnames = { "users", "roles", "SEs" };

	private static String logOutput = "";
	private static int updates = 0;

	@Override
	public void run() {
		this.setSleepPeriod(20 * 1000); // 1min
		logger.log(Level.INFO, "DB periodic optimizer starts");
		try (DBFunctions usersdb = ConfigUtils.getDB("alice_users"); DBFunctions admindb = ConfigUtils.getDB("ADMIN");) {
			if (usersdb == null || admindb == null) {
				logger.log(Level.INFO, "Could not get DBs!");
				return;
			}
			DBSyncUtils.checkLdapSyncTable(usersdb);
			while (true) {
				try {
					resyncLDAP(usersdb, admindb);
					synchronized (backRequestSync) {
						backRequestSync.notifyAll();
					}
					periodic.set(true);
				}
				catch (Exception e) {
					e.printStackTrace();
				}
				try {
					synchronized (requestSync) {
						logger.log(Level.INFO, "Periodic sleeps " + this.getSleepPeriod());
						requestSync.wait(this.getSleepPeriod());
					}
				}
				catch (final InterruptedException e) {
					logger.log(Level.SEVERE, "The periodic optimiser has been forced to exit");
					break;
				}
			}
		}
	}

	/**
	 * Manual instruction for the resyncLDAP
	 */
	public static String manualResyncLDAP(boolean lastLog) {
		synchronized (requestSync) {
			logger.log(Level.INFO, "Started manual resyncLDAP");
			periodic.set(false);
			requestSync.notifyAll();
		}

		while (!periodic.get()){
			try {
				synchronized (backRequestSync) {
					backRequestSync.wait(1000);
				}

			}
			catch (final InterruptedException e) {
				logger.log(Level.SEVERE, "The periodic optimiser has been forced to exit");
			}
		}
		if (lastLog && updates == 0)
			return DBSyncUtils.getLastLog();
		return logOutput;
	}

	/**
	 * Performs the resyncLDAP for the users, roles and SEs
	 *
	 * @param usersdb Database instance for SE, SE_VOLUMES and LDAP_SYNC tables
	 * @param admindb Database instance for USERS_LDAP and USERS_LDAP_ROLE tables
	 */
	private static void resyncLDAP(DBFunctions usersdb, DBFunctions admindb) {
		final int frequency = 60000; // To change (1 hour default)
		logOutput = "";
		updates = 0;

		logger.log(Level.INFO, "Checking if an LDAP resynchronisation is needed");
		usersdb.setReadOnly(false);
		boolean updated = false;
		for (String classname : classnames) {
			if (periodic.get())
				updated = DBSyncUtils.updatePeriodic(frequency, PeriodicOptimiser.class.getCanonicalName() + "." + classname);
			else
				updated = DBSyncUtils.updateManual(PeriodicOptimiser.class.getCanonicalName() + "." + classname);
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
				logger.log(Level.INFO, logOutput);
			}
		}
	}

	/**
	 * Updates the users in the LDAP database
	 *
	 * @param Database instance
	 */
	public static void updateUsers(DBFunctions db) {
		logger.log(Level.INFO, "Synchronising DB users with LDAP");
		String ouHosts = "ou=People,";
		HashMap<String, String> modifications = new HashMap<>();

		// Insertion of the users
		final Set<String> uids = LDAPHelper.checkLdapInformation("(objectClass=pkiUser)", ouHosts, "uid", false);
		int length = uids.size();
		logger.log(Level.INFO, "Inserting " + length + " users");
		if (length == 0)
			logger.log(Level.WARNING, "No users gotten from LDAP");

		boolean querySuccess = db.query("SELECT user from `USERS_LDAP`", false);
		if (!querySuccess) {
			logger.log(Level.SEVERE, "Error getting users from DB");
			return;
		}
		while (db.moveNext()) {
			String user = db.gets("user");
			if (!uids.contains(user)) {
				modifications.put(user, user + ": deleted account \n");
			}
		}

		// TODO: To be done with replace into
		db.query("UPDATE USERS_LDAP SET up=0");
		for (String user : uids) {
			ArrayList<String> originalDns = new ArrayList<>();
			querySuccess = db.query("SELECT * from `USERS_LDAP` WHERE user = ?", false, user);
			if (!querySuccess) {
				logger.log(Level.SEVERE, "Error getting DB entry for user " + user);
				return;
			}
			while (db.moveNext())
				originalDns.add(db.gets("dn"));
			if (originalDns.isEmpty())
				modifications.put(user, user + ": new account, ");
			final Set<String> dns = LDAPHelper.checkLdapInformation("uid=" + user, ouHosts, "subject", false);
			ArrayList<String> currentDns = new ArrayList<>();
			for (String dn : dns) {
				currentDns.add(dn);
				// db.query("REPLACE INTO USERS_LDAP (user, dn, up) VALUES (?, ?, 1)", false, user, dn);
				db.query("INSERT INTO USERS_LDAP (user, dn, up) VALUES (?, ?, 1)", false, user, dn);
			}

			printModifications(modifications, currentDns, originalDns, user, "added", "DNs");
			printModifications(modifications, originalDns, currentDns, user, "removed", "DNs");

			String homeDir = UsersHelper.getHomeDir(user);
			LFN userHome = LFNUtils.getLFN(homeDir);
			if (userHome == null || !userHome.exists) {
				AliEnPrincipal adminUser = new AliEnPrincipal("admin");
				userHome = LFNUtils.mkdirs(adminUser, homeDir);
			}
			if (userHome != null) {
				AliEnPrincipal newUser = new AliEnPrincipal(user);
				userHome.chown(newUser);
			}
		}
		db.query("select a.user from USERS_LDAP a left join USERS_LDAP b on b.up=0 and a.user=b.user where a.up=1 and b.user is null");
		while (db.moveNext()) {
			String userToDelete = db.gets("user");
			db.query("SELECT count(*) from `USERS_LDAP` WHERE user = ?", false, userToDelete);
			if (db.moveNext()) {
				int count = db.geti(1);
				if (count == 0) {
					logger.log(Level.WARNING, "The user " + userToDelete + " is no longer listed in LDAP. It will be deleted from the database");
				}
			}
		}

		logger.log(Level.INFO, "Deleting inactive users");
		db.query("DELETE FROM USERS_LDAP WHERE up = 0");
		// TODO: Delete home dir of inactive users

		String usersLog = "Users: " + length + " synchronized. " + modifications.keySet().size() + " changes. \n";
		if (modifications.size() > 0) {
			for (String user : modifications.keySet())
				usersLog = usersLog + modifications.get(user) + "\n";
		}
		logOutput = logOutput + "\n" + usersLog;
		updates = updates + modifications.keySet().size();
		if (modifications.keySet().size() != 0)
			DBSyncUtils.registerLog(PeriodicOptimiser.class.getCanonicalName() + ".users", usersLog);
	}

	/**
	 * Updates the roles in the LDAP database
	 *
	 * @param Database instance
	 */
	public static void updateRoles(DBFunctions db) {
		logger.log(Level.INFO, "Synchronising DB roles with LDAP");
		String ouRoles = "ou=Roles,";
		HashMap<String, String> modifications = new HashMap<>();

		// Insertion of the roles
		final Set<String> roles = LDAPHelper.checkLdapInformation("(objectClass=AliEnRole)", ouRoles, "uid", false);
		int length = roles.size();
		logger.log(Level.INFO, "Inserting " + length + " roles");
		if (length == 0)
			logger.log(Level.WARNING, "No roles gotten from LDAP");

		boolean querySuccess = db.query("SELECT role from `USERS_LDAP_ROLE`", false);
		if (!querySuccess) {
			logger.log(Level.SEVERE, "Error getting roles from DB");
			return;
		}
		while (db.moveNext()) {
			String role = db.gets("role");
			if (!roles.contains(role)) {
				modifications.put(role, role + ": deleted role \n");
			}
		}

		// TODO: To be done with replace into
		db.query("UPDATE USERS_LDAP_ROLE SET up=0");
		for (String role : roles) {
			ArrayList<String> originalUsers = new ArrayList<>();
			querySuccess = db.query("SELECT * from `USERS_LDAP_ROLE` WHERE role = ?", false, role);
			if (!querySuccess) {
				logger.log(Level.SEVERE, "Error getting DB entry for role " + role);
				return;
			}
			while (db.moveNext())
				originalUsers.add(db.gets("user"));
			if (originalUsers.isEmpty())
				modifications.put(role, role + ": new role, ");
			final Set<String> users = LDAPHelper.checkLdapInformation("uid=" + role, ouRoles, "users", false);
			ArrayList<String> currentUsers = new ArrayList<>();
			for (String user : users) {
				querySuccess = db.query("SELECT count(*) from `USERS_LDAP` WHERE user = ?", false, user);
				if (!querySuccess) {
					logger.log(Level.SEVERE, "Error getting user count from DB");
					return;
				}
				if (db.moveNext()) {
					int userInstances = db.geti(1);
					if (userInstances == 0) {
						logger.log(Level.WARNING, "An already deleted user is still associated with role " + role + ". Consider cleaning ldap");
						if (originalUsers.contains(user))
							originalUsers.remove(user);
					}
					else {
						db.query("INSERT INTO USERS_LDAP_ROLE (user, role, up) VALUES (?, ?, 1)", false, user, role);
						currentUsers.add(user);
					}
				}
			}

			if (currentUsers.isEmpty())
				modifications.remove(role);
			printModifications(modifications, currentUsers, originalUsers, role, "added", "users");
			printModifications(modifications, originalUsers, currentUsers, role, "removed", "users");
		}

		db.query("select a.role from USERS_LDAP_ROLE a left join USERS_LDAP_ROLE b on b.up=0 and a.role=b.role where a.up=1 and b.role is null");
		while (db.moveNext()) {
			String roleToDelete = db.gets("role");
			logger.log(Level.WARNING, "The role " + roleToDelete + " is no longer listed in LDAP. It will be deleted from the database");
		}

		logger.log(Level.INFO, "Deleting inactive roles");
		db.query("DELETE FROM USERS_LDAP_ROLE WHERE up = 0");

		String rolesLog = "Roles: " + length + " synchronized. " + modifications.keySet().size() + " changes. \n";
		if (modifications.size() > 0) {
			for (String role : modifications.keySet())
				rolesLog = rolesLog + modifications.get(role) + "\n";
		}
		logOutput = logOutput + "\n" + rolesLog;
		updates = updates + modifications.keySet().size();
		if (modifications.keySet().size() != 0)
			DBSyncUtils.registerLog(PeriodicOptimiser.class.getCanonicalName() + ".roles", rolesLog);
	}

	/**
	 * Updates the SEs and SE_VOLUMES in the LDAP database
	 *
	 * @param Database instance
	 */
	public static void updateSEs(DBFunctions db) {
		logger.log(Level.INFO, "Synchronising DB SEs and volumes with LDAP");
		String ouSites = "ou=Sites,";

		final Set<String> dns = LDAPHelper.checkLdapInformation("(objectClass=AliEnSE)", ouSites, "dn", true);
		ArrayList<String> seNames = new ArrayList<>();
		ArrayList<String> sites = new ArrayList<>();
		HashMap<String, String> modifications = new HashMap<>();

		// From the dn we get the seName and site
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
				sites.add(entries[entries.length - 1]);
			}
		}

		int length = seNames.size();
		if (length == 0)
			logger.log(Level.WARNING, "No SEs gotten from LDAP");

		for (int ind = 0; ind < sites.size(); ind++) {
			String site = sites.get(ind);
			String se = seNames.get(ind);

			// This will be the base dn for the SE
			String ouSE = "ou=SE,ou=Services,ou=" + site + ",ou=Sites,";

			String vo = "ALICE";
			String seName = vo + "::" + site + "::" + se;

			HashMap<String, String> originalSEs = new HashMap<>();
			boolean querySuccess = db.query("SELECT * from `SE` WHERE seName = ?", false, seName);
			if (!querySuccess) {
				logger.log(Level.SEVERE, "Error getting SEs from DB");
				return;
			}
			while (db.moveNext()) {
				originalSEs = populateSERegistry(db.gets("seName"), db.gets("seioDaemons"), db.gets("seStoragePath"), db.gets("seMinSize"), db.gets("seType"), db.gets("seQoS"),
						db.gets("seExclusiveWrite"), db.gets("seExclusiveRead"), db.gets("seVersion"));
			}
			if (originalSEs.isEmpty())
				modifications.put(seName, seName + " : new storage element, ");

			final String t = getLdapContentSE(ouSE, se, "mss");
			final String host = getLdapContentSE(ouSE, se, "host");

			final Set<String> savedir = LDAPHelper.checkLdapInformation("name=" + se, ouSE, "savedir");
			for (String path : savedir) {
				HashMap<String, String> originalSEVolumes = new HashMap<>();
				querySuccess = db.query("SELECT * from `SE_VOLUMES` WHERE seName = ?", false, seName);
				if (!querySuccess) {
					logger.log(Level.SEVERE, "Error getting SE volumes from DB");
					return;
				}
				while (db.moveNext()) {
					originalSEVolumes = populateSEVolumesRegistry(db.gets("sename"), db.gets("volume"), db.gets("method"), db.gets("mountpoint"), db.gets("size"));
				}

				String size = "-1";
				logger.log(Level.INFO, "Checking the path of " + path);
				if (path.matches(".*,\\d+")) {
					size = path.split(",")[1];
					path = path.split(",")[0];
				}

				logger.log(Level.INFO, "Need to add the volume " + path);
				String method = t.toLowerCase() + "://" + host;
				db.query("REPLACE INTO SE_VOLUMES(sename,volume,method,mountpoint,size) values (?,?,?,?,?)", false,
						seName, path, method, path, size);

				HashMap<String, String> currentSEVolumes = populateSEVolumesRegistry(seName, path, method, path, size);
				printModificationsSEs(modifications, originalSEVolumes, currentSEVolumes, seName, "SE Volumes");

			}

			final String iodaemons = getLdapContentSE(ouSE, se, "iodaemons");
			String[] temp = iodaemons.split(":");
			String seioDaemons = "";
			if (temp.length > 2) {
				String proto = temp[0];
				proto = proto.replace("xrootd", "root");
				String hostName = "";
				String port = "";
				hostName = temp[1];
				if (!hostName.matches("host=([^:]+)(:.*)?$")) {
					logger.log(Level.INFO, "Error getting the host name from " + seName);
					seioDaemons = "NULL";
				}
				hostName = hostName.split("=")[1];

				port = temp[2];
				if (!port.matches("port=(\\d+)")) {
					logger.log(Level.INFO, "Error getting the port for " + seName);
					seioDaemons = "NULL";
				}
				port = port.split("=")[1];

				if (!seioDaemons.equals("NULL")) {
					seioDaemons = proto + "://" + hostName + ":" + port;
					logger.log(Level.INFO, "Using proto = " + proto + " host = " + hostName + " and port = " + port + " for " + seName);
				}
			}

			String path = getLdapContentSE(ouSE, se, "savedir");
			if (path.equals("")) {
				logger.log(Level.INFO, "Error getting the savedir for " + seName);
				return;
			}

			if (path.matches(".*,\\d+")) {
				path = path.split(",")[0];
			}

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

			HashMap<String, String> currentSEs = populateSERegistry(seName, seioDaemons, path, minSize, mss, qos, seExclusiveWrite, seExclusiveRead, seVersion);
			printModificationsSEs(modifications, originalSEs, currentSEs, seName, "SEs");

			db.query("REPLACE INTO SE (seName,seMinSize,seType,seQoS,seExclusiveWrite,seExclusiveRead,seVersion,seStoragePath,seioDaemons) "
					+ "values (?,?,?,?,?,?,?,?,?)", false, seName, minSize, mss, qos, seExclusiveWrite, seExclusiveRead, seVersion, path,
					seioDaemons);
			logger.log(Level.INFO, "Added or updated entry for SE " + seName);
		}

		db.query("update SE_VOLUMES set usedspace=0 where usedspace is null");
		db.query("update SE_VOLUMES set freespace=size-usedspace where size <> -1");
		db.query("update SE_VOLUMES set freespace=size-usedspace where size <> -1");

		// TODO: Delete inactive SEs

		String sesLog = "SEs: " + length + " synchronized. " + modifications.keySet().size() + " changes. \n";
		if (modifications.size() > 0) {
			for (String seName : modifications.keySet())
				sesLog = sesLog + modifications.get(seName) + "\n";
		}
		logOutput = logOutput + "\n" + sesLog;
		updates = updates + modifications.keySet().size();
		if (modifications.keySet().size() != 0)
			DBSyncUtils.registerLog(PeriodicOptimiser.class.getCanonicalName() + ".SEs", sesLog);
	}

	/**
	 * Method for building the output for the manual resyncLDAP for Users and Roles
	 *
	 * @param modifications
	 * @param original
	 * @param current
	 * @param element
	 * @param action
	 * @param entity
	 */
	private static void printModifications(HashMap<String, String> modifications, ArrayList<String> original, ArrayList<String> current, String element, String action, String entity) {
		ArrayList<String> entities = new ArrayList<>(original);
		entities.removeAll(current);
		if (entities.size() > 0) {
			addEntityLog(modifications, element);
			modifications.put(element, modifications.get(element) + " " + action + " " + entities.size() + " " + entity + " (" + entities.toString() + ")");
		}
	}

	/**
	 * Method for building the output for the manual resyncLDAP for Storage Elements
	 *
	 * @param modifications
	 * @param original
	 * @param current
	 * @param se
	 */
	private static void printModificationsSEs(HashMap<String, String> modifications, HashMap<String, String> original, HashMap<String, String> current, String se, String entity) {
		ArrayList<String> updatedSEs = new ArrayList<>();
		Set<String> keySet = null;
		if (original.keySet().size() > current.keySet().size())
			keySet = original.keySet();
		else
			keySet = current.keySet();
		for (String param : keySet) {
			if (original.get(param) == null || original.get(param) == "")
				original.put(param, "null");
			if (current.get(param) == null)
				current.put(param, "null");
			if (!original.get(param).equals(current.get(param))) {
				updatedSEs.add(param + " (new value = " + current.get(param) + ")");
			}
		}

		if (updatedSEs.size() > 0) {
			addEntityLog(modifications, se);
			modifications.put(se, modifications.get(se) + "\n \t " + entity + " updated " + updatedSEs.size() + " parameters " + updatedSEs.toString());
		}
	}

	private static HashMap<String, String> populateSEVolumesRegistry(String seName, String volume, String method, String mountpoint, String size) {
		HashMap<String, String> seVolumes = new HashMap<>();
		seVolumes.put("seName", seName);
		seVolumes.put("volume", volume);
		seVolumes.put("method", method);
		seVolumes.put("mountpoint", mountpoint);
		seVolumes.put("size", size);
		return seVolumes;
	}

	private static HashMap<String, String> populateSERegistry(String seName, String seioDaemons, String path, String minSize, final String mss, final String qos, final String seExclusiveWrite,
			final String seExclusiveRead, final String seVersion) {
		HashMap<String, String> ses = new HashMap<>();
		ses.put("seName", seName);
		ses.put("seMinSize", minSize);
		ses.put("seType", mss);
		ses.put("seQoS", qos);
		ses.put("seExclusiveWrite", seExclusiveWrite);
		ses.put("seExclusiveRead", seExclusiveRead);
		ses.put("seVersion", seVersion);
		ses.put("seStoragePath", path);
		ses.put("seioDaemons", seioDaemons);
		return ses;
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
			joined = null;
		}
		return joined;
	}

	private static void addEntityLog(HashMap<String, String> modifications, String element) {
		if (modifications.get(element) == null) {
			modifications.put(element, element + " :  ");
		}
	}
}
