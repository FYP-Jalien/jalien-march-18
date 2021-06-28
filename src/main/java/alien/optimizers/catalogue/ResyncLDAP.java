package alien.optimizers.catalogue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
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
import alien.user.UserFactory;
import alien.user.UsersHelper;
import lazyj.DBFunctions;

/**
 * @author Marta
 * @since May 3, 2021
 */
public class ResyncLDAP extends Optimizer {

	/**
	 * Optimizer synchronizations
	 */
	static final Object requestSync = new Object();
	static final Object backRequestSync = new Object();

	/**
	 * Logging facility
	 */
	static final Logger logger = ConfigUtils.getLogger(ResyncLDAP.class.getCanonicalName());

	/**
	 * When to update the lastUpdateTimestamp in the OPTIMIZERS db
	 */
	final static int updateDBCount = 10000;

	/**
	 * Periodic synchronization boolean
	 */
	private static AtomicBoolean periodic = new AtomicBoolean(true);

	private static String[] classnames = { "users", "roles", "SEs" };

	private static String logOutput = "";

	@Override
	public void run() {
		this.setSleepPeriod(3600 * 1000); // 1 hour
		logger.log(Level.INFO, "DB resyncLDAP starts");

		DBSyncUtils.checkLdapSyncTable();
		while (true) {
			try {
				resyncLDAP();
				synchronized (backRequestSync) {
					backRequestSync.notifyAll();
				}
				periodic.set(true);
			}
			catch (final Exception e) {
				e.printStackTrace();
			}
			try {
				synchronized (requestSync) {
					logger.log(Level.INFO, "Periodic sleeps " + this.getSleepPeriod());
					requestSync.wait(this.getSleepPeriod());
				}
			}
			catch (final InterruptedException e) {
				logger.log(Level.SEVERE, "The periodic optimiser has been forced to exit", e);
				break;
			}
		}
	}

	/**
	 * Manual instruction for the ResyncLDAP
	 *
	 * @return the log of the manually executed command
	 */
	public static String manualResyncLDAP() {
		synchronized (requestSync) {
			logger.log(Level.INFO, "Started manual ResyncLDAP");
			periodic.set(false);
			requestSync.notifyAll();
		}

		while (!periodic.get()) {
			try {
				synchronized (backRequestSync) {
					backRequestSync.wait(1000);
				}
			}
			catch (final InterruptedException e) {
				logger.log(Level.SEVERE, "The periodic optimiser has been forced to exit", e);
				break;
			}
		}

		return logOutput;
	}

	/**
	 * Performs the ResyncLDAP for the users, roles and SEs
	 *
	 * @param usersdb Database instance for SE, SE_VOLUMES and LDAP_SYNC tables
	 * @param admindb Database instance for USERS_LDAP and USERS_LDAP_ROLE tables
	 */
	private static void resyncLDAP() {
		final int frequency = 3600 * 1000; // 1 hour default
		logOutput = "";

		logger.log(Level.INFO, "Checking if an LDAP resynchronisation is needed");
		boolean updated = true;
		for (final String classname : classnames) {
			if (periodic.get())
				updated = DBSyncUtils.updatePeriodic(frequency, ResyncLDAP.class.getCanonicalName() + "." + classname);

			if (updated) {
				switch (classname) {
					case "users":
						updateUsers();
						break;
					case "roles":
						updateRoles();
						break;
					case "SEs":
						updateSEs();
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
	private static void updateUsers() {
		logger.log(Level.INFO, "Synchronising DB users with LDAP");
		final String ouHosts = "ou=People,";
		final HashMap<String, String> modifications = new HashMap<>();

		// Insertion of the users
		final Set<String> uids = LDAPHelper.checkLdapInformation("(objectClass=pkiUser)", ouHosts, "uid", false);
		final int length = uids.size();
		logger.log(Level.INFO, "Inserting " + length + " users");
		if (length == 0) {
			logger.log(Level.WARNING, "No users gotten from LDAP. This is likely due to an LDAP server problem, bailing out");
			return;
		}

		try (DBFunctions db = ConfigUtils.getDB("ADMIN");) {
			if (db == null) {
				logger.log(Level.INFO, "Could not get DBs!");
				return;
			}
			boolean querySuccess = db.query("SELECT user from `USERS_LDAP`", false);
			if (!querySuccess) {
				logger.log(Level.SEVERE, "Error getting users from DB");
				return;
			}
			while (db.moveNext()) {
				final String user = db.gets("user");
				if (!uids.contains(user)) {
					modifications.put(user, user + ": deleted account \n");
				}
			}

			int counter = 0;
			// TODO: To be done with replace into
			db.query("UPDATE USERS_LDAP SET up=0");
			for (final String user : uids) {
				final ArrayList<String> originalDns = new ArrayList<>();
				querySuccess = db.query("SELECT * from `USERS_LDAP` WHERE user = ?", false, user);
				if (!querySuccess) {
					logger.log(Level.SEVERE, "Error getting DB entry for user " + user);
					return;
				}
				while (db.moveNext())
					originalDns.add(db.gets("dn"));
				final Set<String> dns = LDAPHelper.checkLdapInformation("uid=" + user, ouHosts, "subject", false);
				final ArrayList<String> currentDns = new ArrayList<>();
				for (final String dn : dns) {
					final String trimmedDN = dn.replaceAll("(^[\\s\\r\\n]+)|([\\s\\r\\n]+$)", "");
					currentDns.add(trimmedDN);
					// db.query("REPLACE INTO USERS_LDAP (user, dn, up) VALUES (?, ?, 1)", false, user, dn);
					db.query("INSERT INTO USERS_LDAP (user, dn, up) VALUES (?, ?, 1)", false, user, trimmedDN);
				}

				printModifications(modifications, currentDns, originalDns, user, "added", "DNs");
				printModifications(modifications, originalDns, currentDns, user, "removed", "DNs");

				final String homeDir = UsersHelper.getHomeDir(user);
				LFN userHome = LFNUtils.getLFN(homeDir);
				if (userHome == null || !userHome.exists) {
					final AliEnPrincipal adminUser = UserFactory.getByUsername("admin");
					userHome = LFNUtils.mkdirs(adminUser, homeDir);
				}
				if (userHome != null) {
					final AliEnPrincipal newUser = UserFactory.getByUsername(user);

					if (newUser != null)
						userHome.chown(newUser);
				}

				counter = counter + 1;
				if (counter > updateDBCount)
					DBSyncUtils.setLastActive(ResyncLDAP.class.getCanonicalName() + ".users");
			}
			db.query("select a.user from USERS_LDAP a left join USERS_LDAP b on b.up=0 and a.user=b.user where a.up=1 and b.user is null");
			while (db.moveNext()) {
				final String userToDelete = db.gets("user");
				db.query("SELECT count(*) from `USERS_LDAP` WHERE user = ?", false, userToDelete);
				if (db.moveNext()) {
					final int count = db.geti(1);
					if (count == 0) {
						logger.log(Level.WARNING, "The user " + userToDelete + " is no longer listed in LDAP. It will be deleted from the database");
					}
				}
			}

			logger.log(Level.INFO, "Deleting inactive users");
			db.query("DELETE FROM USERS_LDAP WHERE up = 0");
			// TODO: Delete home dir of inactive users

			final String usersLog = "Users: " + length + " synchronized. " + modifications.size() + " changes. \n" + String.join("\n", modifications.values());

			logOutput = logOutput + "\n" + usersLog;
			if (periodic.get())
				DBSyncUtils.registerLog(ResyncLDAP.class.getCanonicalName() + ".users", usersLog);
			else if (modifications.size() > 0)
				DBSyncUtils.updateManual(ResyncLDAP.class.getCanonicalName() + ".users", usersLog);
		}
	}

	/**
	 * Updates the roles in the LDAP database
	 *
	 * @param Database instance
	 */
	private static void updateRoles() {
		logger.log(Level.INFO, "Synchronising DB roles with LDAP");
		final String ouRoles = "ou=Roles,";
		final HashMap<String, String> modifications = new HashMap<>();

		// Insertion of the roles
		final Set<String> roles = LDAPHelper.checkLdapInformation("(objectClass=AliEnRole)", ouRoles, "uid", false);
		final int length = roles.size();
		logger.log(Level.INFO, "Inserting " + length + " roles");
		if (length == 0) {
			logger.log(Level.WARNING, "No roles gotten from LDAP. This is likely an error, exiting now");
			return;
		}

		try (DBFunctions db = ConfigUtils.getDB("ADMIN");) {
			if (db == null) {
				logger.log(Level.INFO, "Could not get DBs!");
				return;
			}
			boolean querySuccess = db.query("SELECT role from `USERS_LDAP_ROLE`", false);
			if (!querySuccess) {
				logger.log(Level.SEVERE, "Error getting roles from DB");
				return;
			}
			while (db.moveNext()) {
				final String role = db.gets("role");
				if (!roles.contains(role)) {
					modifications.put(role, role + ": deleted role \n");
				}
			}

			int counter = 0;
			// TODO: To be done with replace into
			db.query("UPDATE USERS_LDAP_ROLE SET up=0");
			for (final String role : roles) {
				final ArrayList<String> originalUsers = new ArrayList<>();
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
				final ArrayList<String> currentUsers = new ArrayList<>();
				for (final String user : users) {
					querySuccess = db.query("SELECT count(*) from `USERS_LDAP` WHERE user = ?", false, user);
					if (!querySuccess) {
						logger.log(Level.SEVERE, "Error getting user count from DB");
						return;
					}
					if (db.moveNext()) {
						final int userInstances = db.geti(1);
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

				counter = counter + 1;
				if (counter > updateDBCount)
					DBSyncUtils.setLastActive(ResyncLDAP.class.getCanonicalName() + ".roles");
			}

			db.query("select a.role from USERS_LDAP_ROLE a left join USERS_LDAP_ROLE b on b.up=0 and a.role=b.role where a.up=1 and b.role is null");
			while (db.moveNext()) {
				final String roleToDelete = db.gets("role");
				logger.log(Level.WARNING, "The role " + roleToDelete + " is no longer listed in LDAP. It will be deleted from the database");
			}

			logger.log(Level.INFO, "Deleting inactive roles");
			db.query("DELETE FROM USERS_LDAP_ROLE WHERE up = 0");

			final String rolesLog = "Roles: " + length + " synchronized. " + modifications.size() + " changes. \n" + String.join("\n", modifications.values());

			logOutput = logOutput + "\n" + rolesLog;
			if (periodic.get())
				DBSyncUtils.registerLog(ResyncLDAP.class.getCanonicalName() + ".roles", rolesLog);
			else if (modifications.size() > 0)
				DBSyncUtils.updateManual(ResyncLDAP.class.getCanonicalName() + ".roles", rolesLog);
		}
	}

	/**
	 * Updates the SEs and SE_VOLUMES in the LDAP database
	 *
	 * @param Database instance
	 */
	private static void updateSEs() {
		logger.log(Level.INFO, "Synchronising DB SEs and volumes with LDAP");
		final String ouSites = "ou=Sites,";

		final Set<String> dns = LDAPHelper.checkLdapInformation("(objectClass=AliEnSE)", ouSites, "dn", true);
		final ArrayList<String> seNames = new ArrayList<>();
		final ArrayList<String> dnsEntries = new ArrayList<>();
		final ArrayList<String> sites = new ArrayList<>();
		final HashMap<String, String> modifications = new HashMap<>();

		try (DBFunctions db = ConfigUtils.getDB("alice_users");DBFunctions dbTransfers = ConfigUtils.getDB("transfers")) {
			if (db == null || dbTransfers == null) {
				logger.log(Level.INFO, "Could not get DBs!");
				return;
			}
			dbTransfers.query("UPDATE PROTOCOLS set updated=0");

			// From the dn we get the seName and site
			final Iterator<String> itr = dns.iterator();
			while (itr.hasNext()) {
				final String dn = itr.next();
				if (dn.contains("disabled")) {
					logger.log(Level.WARNING, "Skipping " + dn + " (it is disabled)");
				}
				else {
					dnsEntries.add(dn);
					final String[] entries = dn.split("[=,]");
					if (entries.length >= 8) {
						seNames.add(entries[1]);
						sites.add(entries[entries.length - 1]);
					}
				}
			}

			final int length = seNames.size();
			if (length == 0)
				logger.log(Level.WARNING, "No SEs gotten from LDAP");

			for (int ind = 0; ind < sites.size(); ind++) {
				final String site = sites.get(ind);
				final String se = seNames.get(ind);
				final String dnsEntry = dnsEntries.get(ind);
				int seNumber = -1;

				// This will be the base dn for the SE
				final String ouSE = dnsEntry + ",ou=Sites,";

				final String vo = "ALICE";
				final String seName = vo + "::" + site + "::" + se;

				final Set<String> ftdprotocols = LDAPHelper.checkLdapInformation("name=" + se, ouSE, "ftdprotocol");
				for (String ftdprotocol : ftdprotocols) {
					final String[] temp = ftdprotocol.split("\\s+");
					String protocol = temp[0];
					String transfers = null;
					if (temp.length > 1)
						transfers = temp[1];
					Integer numTransfers = null;
					if (transfers != null) {
						if (!transfers.matches("transfers=(\\d+)"))
							logger.log(Level.INFO, "Could not get the number of transfers for " + seName + " (ftdprotocol: " + protocol + ")");
						else
							numTransfers = Integer.valueOf(transfers.split("=")[1]);
					}

					boolean querySuccess = dbTransfers.query("SELECT * from `PROTOCOLS` WHERE sename=? and protocol=?", false, seName, protocol);
					if (!querySuccess) {
						logger.log(Level.SEVERE, "Error getting PROTOCOLS from DB");
						return;
					}
					if (dbTransfers.moveNext())
						dbTransfers.query("UPDATE PROTOCOLS SET max_transfers=?, updated=1 where sename=? and protocol=?", false, numTransfers, seName, protocol);
					else
						dbTransfers.query("INSERT INTO PROTOCOLS(sename,protocol,max_transfers) values (?,?,?)", false, seName, protocol, numTransfers);
				}

				HashMap<String, String> originalSEs = new HashMap<>();

				boolean querySuccess = db.query("SELECT * from `SE` WHERE seName = ?", false, seName);
				if (!querySuccess) {
					logger.log(Level.SEVERE, "Error getting SEs from DB");
					return;
				}

				while (db.moveNext()) {
					originalSEs = populateSERegistry(db.gets("seName"), db.gets("seioDaemons"), db.gets("seStoragePath"), db.gets("seMinSize"), db.gets("seType"), db.gets("seQoS"),
							db.gets("seExclusiveWrite"), db.gets("seExclusiveRead"), db.gets("seVersion"));
					seNumber = db.geti("seNumber");
				}
				if (originalSEs.isEmpty())
					modifications.put(seName, seName + " : new storage element, ");

				final String t = getLdapContentSE(ouSE, se, "mss", null);
				final String host = getLdapContentSE(ouSE, se, "host", null);

				final Set<String> savedir = LDAPHelper.checkLdapInformation("name=" + se, ouSE, "savedir");
				for (String path : savedir) {
					HashMap<String, String> originalSEVolumes = new HashMap<>();

					long size = -1;
					logger.log(Level.INFO, "Checking the path of " + path);
					if (path.matches(".*,\\d+")) {
						size = Long.parseLong(path.split(",")[1]);
						path = path.split(",")[0];
					}
					logger.log(Level.INFO, "Need to add the volume " + path);
					final String method = t.toLowerCase() + "://" + host;

					int volumeId = -1;
					querySuccess = db.query("SELECT * from `SE_VOLUMES` WHERE seName=? and mountpoint=?", false, seName, path);
					if (!querySuccess) {
						logger.log(Level.SEVERE, "Error getting SE volumes from DB");
						return;
					}
					while (db.moveNext()) {
						originalSEVolumes = populateSEVolumesRegistry(db.gets("sename"), db.gets("volume"), db.gets("method"), db.gets("mountpoint"), db.gets("size"));
						volumeId = db.geti("volumeId");
					}

					if (volumeId != -1)
						db.query("UPDATE SE_VOLUMES SET volume=?, method=?, size=? WHERE seName=? AND mountpoint=? and volumeId=?", false,
								path, method, Long.valueOf(size), seName, path, Integer.valueOf(volumeId));
					else
						db.query("INSERT INTO SE_VOLUMES(sename,volume,method,mountpoint,size) values (?,?,?,?,?)", false,
								seName, path, method, path, Long.valueOf(size));

					final HashMap<String, String> currentSEVolumes = populateSEVolumesRegistry(seName, path, method, path, String.valueOf(size));
					printModificationsSEs(modifications, originalSEVolumes, currentSEVolumes, seName, "SE Volumes");

				}

				final String iodaemons = getLdapContentSE(ouSE, se, "ioDaemons", null);
				final String[] temp = iodaemons.split(":");
				String seioDaemons = "";
				if (temp.length > 2) {
					String proto = temp[0];
					proto = proto.replace("xrootd", "root");

					String hostName = temp[1].matches("host=([^:]+)(:.*)?$") ? temp[1] : temp[2];
					String port = temp[2].matches("port=(\\d+)") ? temp[2] : temp[1];

					if (!hostName.matches("host=([^:]+)(:.*)?$")) {
						logger.log(Level.INFO, "Error getting the host name from " + seName);
						seioDaemons = null;
					}
					hostName = hostName.split("=")[1];

					if (!port.matches("port=(\\d+)")) {
						logger.log(Level.INFO, "Error getting the port for " + seName);
						seioDaemons = null;
					}
					port = port.split("=")[1];

					if (!"NULL".equals(seioDaemons)) {
						seioDaemons = proto + "://" + hostName + ":" + port;
						logger.log(Level.INFO, "Using proto = " + proto + " host = " + hostName + " and port = " + port + " for " + seName);
					}
				}

				String path = getLdapContentSE(ouSE, se, "savedir", null);
				if ("".equals(path)) {
					logger.log(Level.INFO, "Error getting the savedir for " + seName);
					return;
				}

				if (path.matches(".*,\\d+")) {
					path = path.split(",")[0];
				}

				int minSize = 0;

				final Set<String> options = LDAPHelper.checkLdapInformation("name=" + se, ouSE, "options");
				for (final String option : options) {
					if (option.matches("min_size\\s*=\\s*(\\d+)")) {
						minSize = Integer.parseInt(option.split("=")[1]);
					}
				}

				final String mss = getLdapContentSE(ouSE, se, "mss", null);
				final String qos = "," + getLdapContentSE(ouSE, se, "Qos", null) + ",";
				final String seExclusiveWrite = getLdapContentSE(ouSE, se, "seExclusiveWrite", "");
				final String seExclusiveRead = getLdapContentSE(ouSE, se, "seExclusiveRead", "");
				final String seVersion = getLdapContentSE(ouSE, se, "seVersion", "");

				final HashMap<String, String> currentSEs = populateSERegistry(seName, seioDaemons, path, String.valueOf(minSize), mss, qos, seExclusiveWrite, seExclusiveRead, seVersion);
				printModificationsSEs(modifications, originalSEs, currentSEs, seName, "SEs");

				if (seNumber != -1)
					db.query("UPDATE SE SET seMinSize=?, seType=?, seQoS=?, seExclusiveWrite=?, seExclusiveRead=?, seVersion=?, seStoragePath=?, seioDaemons=?"
							+ "WHERE seNumber=? and seName=?", false, Integer.valueOf(minSize), mss, qos, seExclusiveWrite, seExclusiveRead, seVersion, path,
							seioDaemons, Integer.valueOf(seNumber), seName);
				else
					db.query("INSERT INTO SE (seName,seMinSize,seType,seQoS,seExclusiveWrite,seExclusiveRead,seVersion,seStoragePath,seioDaemons) "
							+ "values (?,?,?,?,?,?,?,?,?)", false, seName, Integer.valueOf(minSize), mss, qos, seExclusiveWrite, seExclusiveRead, seVersion, path, seioDaemons);
				logger.log(Level.INFO, "Added or updated entry for SE " + seName);

				if (ind > updateDBCount)
					DBSyncUtils.setLastActive(ResyncLDAP.class.getCanonicalName() + ".SEs");
			}

			logger.log(Level.INFO, "Deleting inactive protocols");
			dbTransfers.query("delete from PROTOCOLS where updated = 0");
			dbTransfers.query("insert into PROTOCOLS(sename,max_transfers) values ('no_se',10)");

			db.query("update SE_VOLUMES set usedspace=0 where usedspace is null");
			db.query("update SE_VOLUMES set freespace=size-usedspace where size <> -1");
			db.query("update SE_VOLUMES set freespace=size-usedspace where size <> -1");

			// TODO: Delete inactive SEs

			final String sesLog = "SEs: " + length + " synchronized. " + modifications.size() + " changes. \n" + String.join("\n", modifications.values());

			logOutput = logOutput + "\n" + sesLog;
			if (periodic.get())
				DBSyncUtils.registerLog(ResyncLDAP.class.getCanonicalName() + ".SEs", sesLog);
			else if (modifications.size() > 0)
				DBSyncUtils.updateManual(ResyncLDAP.class.getCanonicalName() + ".SEs", sesLog);
		}
	}


	/**
	 * Method for building the output for the manual ResyncLDAP for Users and Roles
	 *
	 * @param modifications
	 * @param original
	 * @param current
	 * @param element
	 * @param action
	 * @param entity
	 */
	private static void printModifications(final HashMap<String, String> modifications, final ArrayList<String> original, final ArrayList<String> current, final String element, final String action,
			final String entity) {
		final ArrayList<String> entities = new ArrayList<>(original);
		entities.removeAll(current);
		if (entities.size() > 0) {
			addEntityLog(modifications, element);
			modifications.put(element, modifications.get(element) + " " + action + " " + entities.size() + " " + entity + " (" + entities.toString() + ")");
		}
	}

	/**
	 * Method for building the output for the manual ResyncLDAP for Storage Elements
	 *
	 * @param modifications
	 * @param original
	 * @param current
	 * @param se
	 */
	private static void printModificationsSEs(final HashMap<String, String> modifications, final HashMap<String, String> original, final HashMap<String, String> current, final String se,
			final String entity) {
		final ArrayList<String> updatedSEs = new ArrayList<>();
		final Set<String> keySet = new LinkedHashSet<>(original.keySet());
		keySet.addAll(current.keySet());
		for (final String param : keySet) {
			if (original.get(param) == null || original.get(param).isBlank())
				original.put(param, "null");
			if (current.get(param) == null || current.get(param).isBlank())
				current.put(param, "null");
			if (!original.get(param).equalsIgnoreCase(current.get(param))) {
				updatedSEs.add(param + " (new value = " + current.get(param) + ")");
			}
		}

		if (updatedSEs.size() > 0) {
			addEntityLog(modifications, se);
			modifications.put(se, modifications.get(se) + "\n \t " + entity + " updated " + updatedSEs.size() + " parameters " + updatedSEs.toString());
		}
	}

	private static HashMap<String, String> populateSEVolumesRegistry(final String seName, final String volume, final String method, final String mountpoint, final String size) {
		final HashMap<String, String> seVolumes = new HashMap<>();
		seVolumes.put("seName", seName);
		seVolumes.put("volume", volume);
		seVolumes.put("method", method);
		seVolumes.put("mountpoint", mountpoint);
		seVolumes.put("size", size);
		return seVolumes;
	}

	private static HashMap<String, String> populateSERegistry(final String seName, final String seioDaemons, final String path, final String minSize, final String mss, final String qos,
			final String seExclusiveWrite, final String seExclusiveRead, final String seVersion) {
		final HashMap<String, String> ses = new HashMap<>();
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

	private static String getLdapContentSE(final String ouSE, final String se, final String parameter, final String defaultString) {
		final Set<String> param = LDAPHelper.checkLdapInformation("name=" + se, ouSE, parameter);
		String joined = "";
		if (param.size() > 1) {
			joined = String.join(",", param);
		}
		else if (param.size() == 1) {
			joined = param.iterator().next();
		}
		else if (param.size() == 0) {
			joined = defaultString;
		}
		return joined;
	}

	private static void addEntityLog(final HashMap<String, String> modifications, final String element) {
		if (modifications.get(element) == null) {
			modifications.put(element, element + " :  ");
		}
	}
}
