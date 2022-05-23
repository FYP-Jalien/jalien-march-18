package alien.optimizers.catalogue;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.api.Request;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.config.ConfigUtils;
import alien.optimizers.DBSyncUtils;
import alien.optimizers.Optimizer;
import alien.user.AliEnPrincipal;
import alien.user.LDAPHelper;
import alien.user.UserFactory;
import alien.user.UsersHelper;
import utils.DBUtils;
import lazyj.DBFunctions;
import lazyj.Format;

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

	private static String[] classnames = { "users", "roles", "SEs", "CEs" };

	private static String logOutput = "";

	@Override
	public void run() {
		this.setSleepPeriod(3600 * 1000); // 1 hour
		logger.log(Level.INFO, "DB resyncLDAP starts");

		DBSyncUtils.checkLdapSyncTable();
		while (true) {
			try {
				resyncLDAP(Request.getVMID());
			}
			catch (final Exception e) {
				logger.log(Level.WARNING, "Exception running the LDAP resync", e);
			}
			finally {
				periodic.set(true);

				synchronized (backRequestSync) {
					backRequestSync.notifyAll();
				}
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
	 * @param uuid
	 *
	 * @param usersdb Database instance for SE, SE_VOLUMES and LDAP_SYNC tables
	 * @param admindb Database instance for USERS_LDAP and USERS_LDAP_ROLE tables
	 */
	private static void resyncLDAP(UUID uuid) {
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
					case "CEs":
						updateCEs();
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

		final HashMap<String, Set<String>> dbUsersOld = new HashMap<>();
		try (DBFunctions db = ConfigUtils.getDB("ADMIN");) {
			if (db == null) {
				logger.log(Level.INFO, "Could not get DBs!");
				return;
			}
			boolean querySuccess = db.query("SELECT user, dn from `USERS_LDAP`", false);
			if (!querySuccess) {
				logger.log(Level.SEVERE, "Error getting users from DB");
				return;
			}
			while (db.moveNext()) {
				final String user = db.gets("user");
				dbUsersOld.computeIfAbsent(user, (k) -> new LinkedHashSet<>()).add(db.gets("dn"));

				if (!uids.contains(user)) {
					modifications.put(user, user + ": deleted account \n");
				}
			}

			int counter = 0;
			// TODO: To be done with replace into
			final HashMap<String, Set<String>> dbUsersNew = new HashMap<>();
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
					try (DBUtils dbu = new DBUtils(db.getConnection())) {
						dbu.lockTables("USERS_LDAP WRITE");
						try{
							String selectQuery = "SELECT COUNT(*) FROM USERS_LDAP WHERE user='" + Format.escSQL(user) + "' AND dn='" + Format.escSQL(trimmedDN) + "'";
							if (!dbu.executeQuery(selectQuery)) {
								logger.log(Level.SEVERE, "Error getting users from DB");
								return;
							}

							if (dbu.getResultSet().next()) {
								final int valuecounts = dbu.getResultSet().getInt(1);
								if (valuecounts == 0) {
									String insertQuery = "INSERT INTO USERS_LDAP (user, dn, up) VALUES ('" + Format.escSQL(user) + "','" + Format.escSQL(trimmedDN) + "', 1)";
									if (!dbu.executeQuery(insertQuery)) {
										logger.log(Level.SEVERE, "Error inserting user " + user + " in DB");
										return;
									}
								}
							}
							dbUsersNew.computeIfAbsent(user, (k) -> new LinkedHashSet<>()).add(trimmedDN);
						}
						finally{
						         dbu.unlockTables();
						}
					}
					catch (SQLException e) {
						logger.log(Level.SEVERE, "SQL error executing the DB operations in resyncLDAP Users: " + e.getMessage());
					}
					catch (IOException e1) {
						logger.log(Level.SEVERE, "IO error executing the DB operations in resyncLDAP Users: " + e1.getMessage());
					}
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

			HashMap<String, Set<String>> toDelete = listToDelete(dbUsersOld, dbUsersNew, "user");
			logger.log(Level.INFO, "Deleting inactive users (" + toDelete + ")");

			try (DBUtils dbu = new DBUtils(db.getConnection())) {
				dbu.lockTables("USERS_LDAP WRITE");
				try {
					for (String user : toDelete.keySet()) {
						for (String dn : toDelete.get(user)) {
							String deleteQuery = "DELETE FROM USERS_LDAP WHERE user='" + Format.escSQL(user) + "' AND dn='" + Format.escSQL(dn) + "'";
							if (!dbu.executeQuery(deleteQuery)) {
								logger.log(Level.SEVERE, "Error deleting user " + user + " from DB");
								return;
							}
						}
					}
				}
				finally{
				         dbu.unlockTables();
				}
			}
			catch (IOException e) {
				logger.log(Level.SEVERE, "IO error executing the DB operations in resyncLDAP Users: " + e.getMessage());
			}

			// TODO: Delete home dir of inactive users

			final String usersLog = "Users: " + length + " synchronized. " + modifications.size() + " changes. \n" + String.join("\n", modifications.values());

			logOutput = logOutput + "\n" + usersLog;
			if (periodic.get())
				DBSyncUtils.registerLog(ResyncLDAP.class.getCanonicalName() + ".users", usersLog);
			else if (modifications.size() > 0)
				DBSyncUtils.updateManual(ResyncLDAP.class.getCanonicalName() + ".users", usersLog);
		}
	}

	private static HashMap<String, Set<String>> listToDelete(final HashMap<String, Set<String>> dbUsersOld, final HashMap<String, Set<String>> dbUsersNew, String entity) {
		HashMap<String, Set<String>> toDelete = new HashMap<>();

		for (String user : dbUsersOld.keySet()) {
			if (dbUsersNew.containsKey(user)) {
				if (!dbUsersNew.get(user).equals(dbUsersOld.get(user))) {
					Set<String> deleteDns = dbUsersOld.get(user);
					deleteDns.removeAll(dbUsersNew.get(user));
					toDelete.put(user, deleteDns);
					logger.log(Level.WARNING, "The " + entity + " " + user + " has DNs that are no longer listed in LDAP. Those DNs (" + deleteDns + ") will be deleted from the database");
				}
			}
			else {
				logger.log(Level.WARNING, "The " + entity + " " + user + " is no longer listed in LDAP. It will be deleted from the database");
				toDelete.put(user, dbUsersOld.get(user));
			}
		}
		return toDelete;
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
		final HashMap<String, Set<String>> dbRolesOld = new HashMap<>();
		final HashMap<String, Set<String>> dbRolesNew = new HashMap<>();
		try (DBFunctions db = ConfigUtils.getDB("ADMIN");) {
			if (db == null) {
				logger.log(Level.INFO, "Could not get DBs!");
				return;
			}

			try (DBUtils dbu = new DBUtils(db.getConnection())) {
				dbu.lockTables("USERS_LDAP_ROLE WRITE, USERS_LDAP READ");
				try {
					String selectQuery = "SELECT role from `USERS_LDAP_ROLE`";
					if (!dbu.executeQuery(selectQuery)) {
						logger.log(Level.SEVERE, "Error getting roles from DB");
						return;
					}

					while (dbu.getResultSet().next()) {
						final String role = dbu.getResultSet().getString("role");
						if (!roles.contains(role)) {
							modifications.put(role, role + ": deleted role \n");
						}
					}

					int counter = 0;
					// TODO: To be done with replace into

					for (final String role : roles) {
						final ArrayList<String> originalUsers = new ArrayList<>();
						selectQuery = "SELECT * from `USERS_LDAP_ROLE` WHERE role = '" + Format.escSQL(role) + "'";
						if (!dbu.executeQuery(selectQuery)) {

							logger.log(Level.SEVERE, "Error getting DB entry for role " + role);
							return;
						}

						while (dbu.getResultSet().next())
							originalUsers.add(dbu.getResultSet().getString("user"));
						dbRolesOld.put(role, new HashSet<>(originalUsers));

						if (originalUsers.isEmpty())
							modifications.put(role, role + ": new role, ");

						final Set<String> users = LDAPHelper.checkLdapInformation("uid=" + role, ouRoles, "users", false);
						final ArrayList<String> currentUsers = new ArrayList<>();
						for (final String user : users) {
							selectQuery = "SELECT count(*) from `USERS_LDAP` WHERE user = '" + Format.escSQL(user) + "'";
							if (!dbu.executeQuery(selectQuery)) {
								logger.log(Level.SEVERE, "Error getting user count from DB");
								return;
							}

							if (dbu.getResultSet().next()) {
								final int userInstances = dbu.getResultSet().getInt(1);
								if (userInstances == 0) {
									logger.log(Level.WARNING, "An already deleted user is still associated with role " + role + ". Consider cleaning ldap");
									if (originalUsers.contains(user))
										originalUsers.remove(user);
								}
								else {
									currentUsers.add(user);
									if (!originalUsers.contains(user)) {
										String insertQuery = "INSERT INTO USERS_LDAP_ROLE (user, role, up) VALUES ('" + Format.escSQL(user) + "','" + Format.escSQL(role) + "', 1)";
										if (!dbu.executeQuery(insertQuery)) {
											logger.log(Level.SEVERE, "Error inserting user " + user + " with role " + role + " in USERS_LDAP_ROLE table");
											return;
										}

									}
								}
							}
						}
						dbRolesNew.put(role, new HashSet<>(currentUsers));
						if (currentUsers.isEmpty())
							modifications.remove(role);
						printModifications(modifications, currentUsers, originalUsers, role, "added", "users");
						printModifications(modifications, originalUsers, currentUsers, role, "removed", "users");

						counter = counter + 1;
						if (counter > updateDBCount)
							DBSyncUtils.setLastActive(ResyncLDAP.class.getCanonicalName() + ".roles");
					}

					HashMap<String, Set<String>> toDelete = listToDelete(dbRolesOld, dbRolesNew, "role");
					logger.log(Level.INFO, "Deleting inactive roles (" + toDelete + ")");
					for (String role : toDelete.keySet()) {
						for (String user : toDelete.get(role)) {
							String deleteQuery = "DELETE FROM USERS_LDAP_ROLE WHERE role='" + Format.escSQL(role) + "' AND user='" + Format.escSQL(user) + "'";
							if (!dbu.executeQuery(deleteQuery)) {
								logger.log(Level.SEVERE, "Error deleting role " + role + " for user " + user + " from DB");
								return;
							}
						}
					}
				}
				finally{
				         dbu.unlockTables();
				}
			}
			catch (SQLException e) {
				logger.log(Level.SEVERE, "SQL error executing the DB operations in resyncLDAP Roles: " + e.getMessage());
			}
			catch (IOException e1) {
				logger.log(Level.SEVERE, "IO error executing the DB operations in resyncLDAP Roles: " + e1.getMessage());
			}

			final String rolesLog = "Roles: " + length + " synchronized. " + modifications.size() + " changes. \n" + String.join("\n", modifications.values());

			logOutput = logOutput + "\n" + rolesLog;
			if (periodic.get())
				DBSyncUtils.registerLog(ResyncLDAP.class.getCanonicalName() + ".roles", rolesLog);
			else if (modifications.size() > 0)
				DBSyncUtils.updateManual(ResyncLDAP.class.getCanonicalName() + ".roles", rolesLog);
		}
	}

	/**
	 * Updates the CEs from the LDAP database
	 *
	 * @param Database instance
	 */
	private static void updateCEs() {
		logger.log(Level.INFO, "Synchronising DB CEs with LDAP");
		final String ouSites = "ou=Sites,";

		final Set<String> dns = LDAPHelper.checkLdapInformation("(objectClass=AliEnCE)", ouSites, "dn", true);
		final ArrayList<String> ceNames = new ArrayList<>();
		final ArrayList<String> dnsEntries = new ArrayList<>();
		final ArrayList<String> sites = new ArrayList<>();
		final HashMap<String, String> modifications = new HashMap<>();
		final Set<String> updatedCEs = new HashSet<>();

		if (!dns.isEmpty()) {
			try (DBFunctions db = ConfigUtils.getDB("processes")) {
				if (db == null) {
					logger.log(Level.INFO, "Could not get DBs!");
					return;
				}

				// From the dn we get the ceName and site
				final Iterator<String> itr = dns.iterator();
				while (itr.hasNext()) {
					final String dn = itr.next();
					dnsEntries.add(dn);
					final String[] entries = dn.split("[=,]");
					if (entries.length >= 8) {
						ceNames.add(entries[1]);
						sites.add(entries[entries.length - 1]);
					}
				}

				final int length = ceNames.size();
				if (length == 0)
					logger.log(Level.WARNING, "No CEs gotten from LDAP");

				HashMap<String, String> originalCEs = new HashMap<>();

				for (int ind = 0; ind < sites.size(); ind++) {
					final String site = sites.get(ind);
					final String ce = ceNames.get(ind);
					final String dnsEntry = dnsEntries.get(ind);

					// This will be the base dn for the CE
					final String ouCE = dnsEntry + ",ou=Sites,";

					final String vo = "ALICE";
					final String ceName = vo + "::" + site + "::" + ce;
					updatedCEs.add(ceName);

					final String maxjobs = getLdapContentSE(ouCE, ce, "maxjobs", null);

					final String maxqueuedjobs = getLdapContentSE(ouCE, ce, "maxqueuedjobs", null);

					try (DBUtils dbu = new DBUtils(db.getConnection())) {
						dbu.lockTables("SITEQUEUES WRITE");
						try {
							int siteId = -1;
							String selectQuery = "SELECT * from `SITEQUEUES` WHERE site='" + Format.escSQL(ceName) + "'";
							if (!dbu.executeQuery(selectQuery)) {
								logger.log(Level.SEVERE, "Error getting CEs from DB");
								return;
							}

							if (dbu.getResultSet().next()) {
								siteId = dbu.getResultSet().getInt("siteId");
								originalCEs = populateCERegistry(dbu.getResultSet().getString("site"), dbu.getResultSet().getString("maxrunning"), dbu.getResultSet().getString("maxqueued"));
							}

							logger.log(Level.INFO, "Inserting or updating database entry for CE " + ceName);
							if (siteId != -1) {
								String updateQuery = "UPDATE SITEQUEUES SET maxrunning=" + Integer.valueOf(maxjobs) + ", maxqueued=" + Integer.valueOf(maxqueuedjobs) + " WHERE site='" + Format.escSQL(ceName) + "'";
								if (!dbu.executeQuery(updateQuery)) {
									logger.log(Level.SEVERE, "Error updating CEs to DB");
									return;
								}
							}
							else {
								String insertQuery = "INSERT INTO SITEQUEUES(site,maxrunning,maxqueued) values ('" + Format.escSQL(ceName) + "'," + Integer.valueOf(maxjobs) + ","
										+ Integer.valueOf(maxqueuedjobs) + ")";
								if (!dbu.executeQuery(insertQuery)) {
									logger.log(Level.SEVERE, "Error inserting CE " + ceName + " to DB");
									return;
								}
							}
						}
						finally{
						         dbu.unlockTables();
						}
					}
					catch (SQLException e) {
						logger.log(Level.SEVERE, "SQL error executing the DB operations in resyncLDAP CEs: " + e.getMessage());
					}
					catch (IOException e1) {
						logger.log(Level.SEVERE, "IO error executing the DB operations in resyncLDAP CEs: " + e1.getMessage());
					}

					final HashMap<String, String> currentCEs = populateCERegistry(ceName, maxjobs, maxqueuedjobs);
					printModifications(modifications, originalCEs, currentCEs, ceName, "CEs");

					if (ind > updateDBCount)
						DBSyncUtils.setLastActive(ResyncLDAP.class.getCanonicalName() + ".CEs");
				}

				try (DBUtils dbu = new DBUtils(db.getConnection())) {
					dbu.lockTables("SITEQUEUES WRITE");
					try {
						ArrayList<String> toDelete = new ArrayList<>();
						String selectQuery = "SELECT site from `SITEQUEUES`";
						if (!dbu.executeQuery(selectQuery)) {
							logger.log(Level.SEVERE, "Error getting CEs from DB");
							return;
						}
						while (dbu.getResultSet().next()) {
							String ce = dbu.getResultSet().getString("site");
							if (!updatedCEs.contains(ce) && !ce.equals("unassigned::site")) {
								toDelete.add(ce);
							}
						}
						for (String element : toDelete) {
							logger.log(Level.INFO, "Deleting CE " + element + " from CE database");
							String deleteQuery = "DELETE from `SITEQUEUES` where site='" + Format.escSQL(element) + "'";
							if (!dbu.executeQuery(deleteQuery)) {
								logger.log(Level.SEVERE, "Error deleting CE " + element + " from DB");
								return;
							}
						}
					}
					finally{
					         dbu.unlockTables();
					}
				}
				catch (SQLException e) {
					logger.log(Level.SEVERE, "SQL error executing the DB operations in resyncLDAP CEs: " + e.getMessage());
				}
				catch (IOException e1) {
					logger.log(Level.SEVERE, "IO error executing the DB operations in resyncLDAP CEs: " + e1.getMessage());
				}

				final String cesLog = "CEs: " + length + " synchronized. " + modifications.size() + " changes. \n" + String.join("\n", modifications.values());

				logOutput = logOutput + "\n" + cesLog;

				if (periodic.get())
					DBSyncUtils.registerLog(ResyncLDAP.class.getCanonicalName() + ".CEs", cesLog);
				else if (modifications.size() > 0)
					DBSyncUtils.updateManual(ResyncLDAP.class.getCanonicalName() + ".CEs", cesLog);
			}
		}
		else {
			logger.log(Level.WARNING, "Could not synchronize CEs with LDAP. LDAP is not responding.");
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
		final HashMap<String, String> modificationsProtocols = new HashMap<>();
		final Set<String> updatedProtocols = new HashSet<>();
		final Set<String> updatedProtocolsNTransfers = new HashSet<>();
		if (!dns.isEmpty()) {
			try (DBFunctions db = ConfigUtils.getDB("alice_users"); DBFunctions dbTransfers = ConfigUtils.getDB("transfers")) {
				if (db == null || dbTransfers == null) {
					logger.log(Level.INFO, "Could not get DBs!");
					return;
				}

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
						updatedProtocols.add(seName + "#" + protocol);
						updatedProtocolsNTransfers.add(seName + "#" + protocol + "#" + numTransfers);
					}

					HashMap<String, String> originalSEs = new HashMap<>();

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

						try (DBUtils dbu = new DBUtils(db.getConnection())) {
							dbu.lockTables("SE_VOLUMES WRITE");
							try {
								int volumeId = -1;
								String selectQuery = "SELECT * from `SE_VOLUMES` WHERE seName='" + Format.escSQL(seName) + "' and mountpoint='" + Format.escSQL(path) + "'";
								if (!dbu.executeQuery(selectQuery)) {
									logger.log(Level.SEVERE, "Error getting SE volumes from DB");
									return;
								}
								while (dbu.getResultSet().next()) {
									originalSEVolumes = populateSEVolumesRegistry(dbu.getResultSet().getString("sename"), dbu.getResultSet().getString("volume"), dbu.getResultSet().getString("method"), dbu.getResultSet().getString("mountpoint"), dbu.getResultSet().getString("size"));
									volumeId = dbu.getResultSet().getInt("volumeId");
								}
								if (volumeId != -1) {
									String updateQuery = "UPDATE SE_VOLUMES SET volume='" + Format.escSQL(path) + "',method='" + Format.escSQL(method) + "',size=" + Long.valueOf(size) + " WHERE seName='" + Format.escSQL(seName) + "' AND mountpoint='" + Format.escSQL(path) + "' and volumeId=" + Integer.valueOf(volumeId);
									if (!dbu.executeQuery(updateQuery)) {
										logger.log(Level.SEVERE, "Error updating SE_VOLUMES from DB");
										return;
									}
								}
								else {
									String insertQuery = "INSERT INTO SE_VOLUMES(sename,volume,method,mountpoint,size) values ('" + Format.escSQL(seName) + "','" + Format.escSQL(path) + "','" + Format.escSQL(method) + "','" + Format.escSQL(path) + "'," + Long.valueOf(size) + ")";
									if (!dbu.executeQuery(insertQuery)) {
										logger.log(Level.SEVERE, "Error inserting SE_VOLUMES to DB");
										return;
									}
								}
								final HashMap<String, String> currentSEVolumes = populateSEVolumesRegistry(seName, path, method, path, String.valueOf(size));
								printModifications(modifications, originalSEVolumes, currentSEVolumes, seName, "SE Volumes");

							}
							finally{
							         dbu.unlockTables();
							}
						}
						catch (SQLException e) {
							logger.log(Level.SEVERE, "SQL error executing the DB operations in resyncLDAP SEs: " + e.getMessage());
						}
						catch (IOException e1) {
							logger.log(Level.SEVERE, "IO error executing the DB operations in resyncLDAP SEs: " + e1.getMessage());
						}
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

					try (DBUtils dbu = new DBUtils(db.getConnection())) {
						dbu.lockTables("SE WRITE");
						try {
							String selectQuery = "SELECT * from `SE` WHERE seName = '" + Format.escSQL(seName) + "'";
							if (!dbu.executeQuery(selectQuery)) {
								logger.log(Level.SEVERE, "Error getting SEs from DB");
								return;
							}
							if (dbu.getResultSet().next()) {
								originalSEs = populateSERegistry(dbu.getResultSet().getString("seName"), dbu.getResultSet().getString("seioDaemons"), dbu.getResultSet().getString("seStoragePath"), dbu.getResultSet().getString("seMinSize"), dbu.getResultSet().getString("seType"), dbu.getResultSet().getString("seQoS"),
										dbu.getResultSet().getString("seExclusiveWrite"), dbu.getResultSet().getString("seExclusiveRead"), dbu.getResultSet().getString("seVersion"));
								seNumber = dbu.getResultSet().getInt("seNumber");
							}
							if (originalSEs.isEmpty())
								modifications.put(seName, seName + " : new storage element, \n");

							final HashMap<String, String> currentSEs = populateSERegistry(seName, seioDaemons, path, String.valueOf(minSize), mss, qos, seExclusiveWrite, seExclusiveRead, seVersion);
							printModifications(modifications, originalSEs, currentSEs, seName, "SEs");

							if (seNumber != -1) {
								String updateQuery = "UPDATE SE SET seMinSize=" + Integer.valueOf(minSize) + ", seType='" + Format.escSQL(mss) + "', seQoS='" + Format.escSQL(qos) + "', seExclusiveWrite='" + Format.escSQL(seExclusiveWrite) + "', seExclusiveRead='" + Format.escSQL(seExclusiveWrite) + "', seVersion='" + seVersion + "', seStoragePath='" + Format.escSQL(path) + "', seioDaemons='" + Format.escSQL(seioDaemons)
										+ "' WHERE seNumber=" + Integer.valueOf(seNumber) + " and seName='" + Format.escSQL(seName) + "'";
								if (!dbu.executeQuery(updateQuery)) {
									logger.log(Level.SEVERE, "Error updating SEs from DB");
									return;
								}
							}
							else {
								String insertQuery = "INSERT INTO SE (seName,seMinSize,seType,seQoS,seExclusiveWrite,seExclusiveRead,seVersion,seStoragePath,seioDaemons) "
										+ "values ('" + Format.escSQL(seName) + "'," + Integer.valueOf(minSize) + ",'" + Format.escSQL(mss) + "','" + Format.escSQL(qos) + "','" + Format.escSQL(seExclusiveWrite) + "','" + Format.escSQL(seExclusiveRead) + "','" + Format.escSQL(seVersion) + "','" + Format.escSQL(path) + "','" + Format.escSQL(seioDaemons) + "')";
								if (!dbu.executeQuery(insertQuery)) {
									logger.log(Level.SEVERE, "Error inserting SEs to DB");
									return;
								}
							}
							logger.log(Level.INFO, "Added or updated entry for SE " + seName);
						}
						finally{
						         dbu.unlockTables();
						}
					}
					catch (SQLException e) {
						logger.log(Level.SEVERE, "SQL error executing the DB operations in resyncLDAP SEs: " + e.getMessage());
					}
					catch (IOException e1) {
						logger.log(Level.SEVERE, "IO error executing the DB operations in resyncLDAP SEs: " + e1.getMessage());
					}
					if (ind > updateDBCount)
						DBSyncUtils.setLastActive(ResyncLDAP.class.getCanonicalName() + ".SEs");
				}

				if (updatedProtocolsNTransfers.size() > 1) {
					try (DBUtils dbu = new DBUtils(dbTransfers.getConnection())) {
						// lock tables in order to update the protocols table
						dbu.lockTables("PROTOCOLS WRITE");
						try {

							for (String combined : updatedProtocolsNTransfers) {
								String seName = combined.split("#")[0];
								String protocol = combined.split("#")[1];
								int numTransfers = ((combined.split("#").length == 3 &&  !combined.split("#")[2].equals("null")) ? Integer.parseInt(combined.split("#")[2]) : 0);
								String selectQuery = "SELECT seName, protocol from `PROTOCOLS` WHERE sename='" + Format.escSQL(seName) + "' and protocol='" + Format.escSQL(protocol) + "'";
								if (!dbu.executeQuery(selectQuery)) {
									logger.log(Level.SEVERE, "Error getting PROTOCOLS from DB");
									return;
								}

								if (dbu.getResultSet().next()) {
									logger.log(Level.INFO, "Updating protocol " + protocol + " on SE " + seName);
									String updateQuery = "UPDATE PROTOCOLS SET max_transfers=" + Integer.valueOf(numTransfers) + " where sename='"  + Format.escSQL(seName) + "' and protocol='" + Format.escSQL(protocol) + "'";
									if (!dbu.executeQuery(updateQuery)) {
										logger.log(Level.SEVERE, "Error updating protocol " + protocol + " in SE " + seName);
										return;
									}
								}
								else {
									modificationsProtocols.put("\t" + seName + " - " + protocol, protocol + " : new protocol in " + seName + "\n");
									logger.log(Level.INFO, "Inserting protocol " + protocol + " on SE " + seName);
									String insertQuery = "INSERT INTO PROTOCOLS(sename,protocol,max_transfers) values ('" + Format.escSQL(seName) + "','" + Format.escSQL(protocol) + "'," + Integer.valueOf(numTransfers) + ")";
									if (!dbu.executeQuery(insertQuery)) {
										logger.log(Level.SEVERE, "Error inserting protocol " + protocol + " in SE " + seName);
										return;
									}
								}
							}

							ArrayList<String> toDelete = new ArrayList<>();
							String selectQuery = "SELECT concat(seName, '#', protocol) from `PROTOCOLS` where protocol is not null;";
							if (!dbu.executeQuery(selectQuery)) {
								logger.log(Level.SEVERE, "Error getting PROTOCOLS from DB");
								return;
							}
							while (dbu.getResultSet().next()) {
								String composed = dbu.getResultSet().getString(1);
								if (!updatedProtocols.contains(composed))
									toDelete.add(composed);
							}

							for (String element : toDelete) {
								try {
									String protocol = element.split("#")[1];
									String se = element.split("#")[0];
									logger.log(Level.INFO, "Deleting protocol " + protocol + " from SE " + se);
									modificationsProtocols.put("\t" + se + " - " + protocol, protocol + " : deleted protocol from " + se + "\n");
									String deleteQuery = "DELETE from `PROTOCOLS` where sename='" + Format.escSQL(se) + "' and protocol='" + Format.escSQL(protocol) + "'";
									if (!dbu.executeQuery(deleteQuery)) {
										logger.log(Level.SEVERE, "Error deleting PROTOCOLS from DB");
										return;
									}
								}
								catch (Exception e) {
									logger.log(Level.WARNING, "Could not split SE and protocol string `" + element + "` " + e);
								}
							}
						}
						finally{
						         dbu.unlockTables();
						}
					}
					catch (SQLException e) {
						logger.log(Level.SEVERE, "SQL error executing the DB operations in resyncLDAP Protocols: " + e.getMessage());
					}
					catch (IOException e1) {
						logger.log(Level.SEVERE, "IO error executing the DB operations in resyncLDAP Protocols: " + e1.getMessage());
					}
				}
				db.query("update SE_VOLUMES set usedspace=0 where usedspace is null");
				db.query("update SE_VOLUMES set freespace=size-usedspace where size <> -1");
				db.query("update SE_VOLUMES set freespace=size-usedspace where size <> -1");

				// TODO: Delete inactive SEs

				String sesLog = "SEs: " + length + " synchronized. " + modifications.size() + " changes. \n" + String.join("\n", modifications.values());
				if (modifications.size() > 0)
					sesLog = sesLog + "\n";
				final String protocolsLog = "Protocols: " + updatedProtocols.size() + " synchronized. " + modificationsProtocols.size() + " changes. \n" + String.join("", modificationsProtocols.values());

				logOutput = logOutput + "\n" + sesLog + "\n" + protocolsLog;
				if (periodic.get())
					DBSyncUtils.registerLog(ResyncLDAP.class.getCanonicalName() + ".SEs", sesLog);
				else if (modifications.size() > 0 || modificationsProtocols.size() > 0)
					DBSyncUtils.updateManual(ResyncLDAP.class.getCanonicalName() + ".SEs", sesLog);

			}
		}
		else {
			logger.log(Level.WARNING, "Could not synchronize SEs with LDAP. LDAP is not responding.");
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
	private static void printModifications(final HashMap<String, String> modifications, final HashMap<String, String> original, final HashMap<String, String> current, final String se,
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

	private static HashMap<String, String> populateCERegistry(final String ceName, final String maxjobs, final String maxqueued) {
		final HashMap<String, String> ces = new HashMap<>();
		ces.put("ceName", ceName);
		ces.put("maxjobs", String.valueOf(maxjobs));
		ces.put("maxqueued", String.valueOf(maxqueued));
		return ces;
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
