package alien.taskQueue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.api.aaa.GetTokenCertificate;
import alien.api.aaa.TokenCertificateType;
import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.user.AliEnPrincipal;
import lazyj.DBFunctions;

/**
 *
 */
public class JobBroker {

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(JobBroker.class.getCanonicalName());

	/**
	 * Monitoring component
	 */
	static transient final Monitor monitor = MonitorFactory.getMonitor(JobBroker.class.getCanonicalName());

	/**
	 * @param matchRequest
	 * @return the information of a matching job for the jobAgent (queueId,
	 *         JDL...)
	 */
	public static HashMap<String, Object> getMatchJob(final HashMap<String, Object> matchRequest) {
		try (DBFunctions db = TaskQueueUtils.getQueueDB()) {
			if (db == null)
				return null;

			db.setQueryTimeout(300);

			if (monitor != null) {
				monitor.incrementCounter("TQ_db_lookup");
				monitor.incrementCounter("TQ_get_match_job");
			}

			HashMap<String, Object> matchAnswer = new HashMap<>();
			HashMap<String, Object> waiting = new HashMap<>();

			logger.log(Level.INFO, "We received parameters: " + matchRequest.toString());

			if (!matchRequest.containsKey("AliEnPrincipal")) {
				logger.log(Level.SEVERE, "getMatchJob: AliEnPrincipal field missing");
				matchAnswer.put("Error", "AliEnPrincipal field missing");
				matchAnswer.put("Code", Integer.valueOf(-1));
			}

			// Checking if the CE is open
			final int openQueue = checkQueueOpen((String) matchRequest.get("CE"));
			if (openQueue != 1) {
				logger.log(Level.INFO, "Queue is not open! Check queueinfo");
				matchAnswer.put("Error", "Queue is not open! Check queueinfo");
				return matchAnswer;
			}

			matchRequest.put("Remote", Integer.valueOf(0));
			matchRequest.put("Return", "entryId"); // skipping ,filebroker

			// TODO: to fully test, print output to JobBroker/host file
			if (!TaskQueueUtils.updateHostStatus((String) matchRequest.get("Host"), "ACTIVE")) {
				logger.log(Level.INFO, "Updating queue failed!");
				matchAnswer.put("Error", "Updating host failed");
				return matchAnswer;
			}

			waiting = getNumberWaitingForSite(matchRequest);

			// we got something back fitting all requirements :-)
			if (waiting.containsKey("entryId")) {
				logger.log(Level.INFO, "We have a job back");
				matchAnswer = getWaitingJobForAgentId(waiting);
			}
			else {
				String installedPackages = null;
				if (!matchRequest.containsKey("CVMFS")) {
					// try without InstalledPackages
					installedPackages = (String) matchRequest.get("InstalledPackages");
					matchRequest.remove("InstalledPackages");
					matchRequest.put("Return", "packages");

					waiting = getNumberWaitingForSite(matchRequest);
				}

				if (waiting.containsKey("packages")) {
					final String packages = (String) waiting.get("packages");
					logger.log(Level.INFO, "Telling the site to install packages '" + packages + "'");

					final ArrayList<String> list = new ArrayList<>(Arrays.asList(packages.split(",")));
					list.removeAll(Collections.singleton("%"));

					logger.log(Level.INFO, "After removing, we have to install @packs ");

					TaskQueueUtils.setSiteQueueStatus((String) matchRequest.get("CE"), "jobagent-install-pack");

					matchAnswer.put("Error", "Packages needed to install: " + list.toString());
					matchAnswer.put("Packages", list);
					matchAnswer.put("Code", Integer.valueOf(-3));
				}
				else {
					// try remote access (no site)
					logger.log(Level.INFO, "Going to try with remote execution agents");
					if (!matchRequest.containsKey("CVMFS"))
						matchRequest.put("InstalledPackages", installedPackages);
					matchRequest.put("Return", "entryId");
					matchRequest.put("Remote", Integer.valueOf(1));
					matchRequest.remove("Site");
					matchRequest.remove("Extrasites");

					waiting = getNumberWaitingForSite(matchRequest);

					if (waiting.containsKey("entryId")) {
						logger.log(Level.INFO, "We have a job back for remote");
						matchAnswer = getWaitingJobForAgentId(waiting);
					}
					else {
						if (!matchRequest.containsKey("CVMFS")) {
							// last case, no site && no packages...
							matchRequest.put("Return", "packages");
							matchRequest.remove("InstalledPackages");

							waiting = getNumberWaitingForSite(matchRequest);
						}

						if (waiting.containsKey("packages")) {
							final String packages = (String) waiting.get("packages");
							logger.log(Level.INFO, "Telling the site to install packages '" + packages + "'");

							final ArrayList<String> list = new ArrayList<>(Arrays.asList(packages.split(",")));
							list.removeAll(Collections.singleton("%"));

							logger.log(Level.INFO, "After removing, we have to install " + list.toString());

							TaskQueueUtils.setSiteQueueStatus((String) matchRequest.get("CE"), "jobagent-install-pack");

							matchAnswer.put("Error", "Packages needed to install (remote): " + list.toString());
							matchAnswer.put("Packages", list);
							matchAnswer.put("Code", Integer.valueOf(-3));
						}
						else {
							logger.log(Level.INFO, "Removing site and packages requirements hasn't been enough. Nothing to run!");
							matchAnswer.put("Error", "Nothing to run :-(");
							matchAnswer.put("Code", Integer.valueOf(-2));
							TaskQueueUtils.setSiteQueueStatus((String) matchRequest.get("CE"), "jobagent-no-match");
						}
					}
				}
			}

			// we get back the needed information
			if (matchAnswer.containsKey("queueId")) {
				// success!!
				matchAnswer.put("Code", Integer.valueOf(1));
				final Long queueId = (Long) matchAnswer.get("queueId");
				final String username = (String) matchAnswer.get("User");

				int resubmission = -1;
				final JobToken jobToken = TaskQueueUtils.insertJobToken(queueId.longValue(), (String) matchAnswer.get("User"));
				if (jobToken != null) {
					resubmission = jobToken.resubmission;
				}

				if (resubmission >= 0) {
					GetTokenCertificate gtc = new GetTokenCertificate((AliEnPrincipal) matchRequest.get("AliEnPrincipal"), username, TokenCertificateType.JOB_TOKEN,
							"queueid=" + queueId + "/resubmission=" + resubmission, 1);
					try {
						gtc.run();
						matchAnswer.put("TokenCertificate", gtc.getCertificateAsString());
						matchAnswer.put("TokenKey", gtc.getPrivateKeyAsString());
					} catch (Exception e) {
						logger.info("Getting TokenCertificate for job " + queueId + " failed: " + e);
					}
				}

				if (!matchAnswer.containsKey("TokenCertificate") || !matchAnswer.containsKey("TokenKey")) {
					logger.log(Level.INFO, "The job could not create token: " + queueId);

					db.setReadOnly(true);
					TaskQueueUtils.setJobStatus(queueId.longValue(), JobStatus.ERROR_A);
					matchAnswer.put("Code", Integer.valueOf(-1));
					matchAnswer.put("Error", "Error getting the TokenCertificate of the job " + queueId);
					if (jobToken != null)
						jobToken.destroy(db);
				}
				else {
					logger.log(Level.INFO, "Created a TokenCertificate for the job...");
					TaskQueueUtils.setSiteQueueStatus((String) matchRequest.get("CE"), "jobagent-match");
					TaskQueueUtils.putJobLog(queueId.longValue(), "state", "Job ASSIGNED to: " + (String) matchRequest.get("CE"), null);
				}
			} // nothing back, something went wrong while obtaining queueId
				// from the positive cases
			else
				if (!matchAnswer.containsKey("Code")) {
					matchAnswer.put("Error", "Nothing to run :-( (no waiting jobs?) ");
					matchAnswer.put("Code", Integer.valueOf(-2));
					TaskQueueUtils.setSiteQueueStatus((String) matchRequest.get("CE"), "jobagent-no-match");
				}

			return matchAnswer;
		}
	}

	private static HashMap<String, Object> getWaitingJobForAgentId(final HashMap<String, Object> waiting) {
		try (DBFunctions db = TaskQueueUtils.getQueueDB()) {
			if (db == null)
				return null;

			db.setQueryTimeout(60);

			final String agentId = (String) waiting.get("entryId");
			final String host = (String) waiting.get("Host");
			final String ceName = (String) waiting.get("CE");

			int hostId, siteId;

			hostId = TaskQueueUtils.getOrInsertFromLookupTable("host", host);
			siteId = TaskQueueUtils.getSiteId(ceName);

			if (hostId == 0 || siteId == 0)
				logger.log(Level.INFO, "The value for " + (hostId > 0 ? "site" : "host") + " is missing");

			logger.log(Level.INFO, "Getting a waiting job for " + agentId + " and " + host + " and " + hostId + " and " + siteId);

			final HashMap<String, Object> job = new HashMap<>();

			String extra = "";
			if (waiting.containsKey("Remote") && ((Integer) waiting.get("Remote")).intValue() == 1)
				extra = "and timestampdiff(SECOND,mtime,now())>=ifnull(remoteTimeout,43200)";

			// Lock QUEUE
			if (!db.query("LOCK TABLE QUEUE WRITE", false)) {
				job.put("Error", "Failed locking QUEUE");
				job.put("Code", Integer.valueOf(-5));
				return job;
			}

			// Get a job
			long queueId;
			db.query("SELECT min(queueId) FROM QUEUE where statusId=5 and agentid=? " + extra, false, agentId);
			if (db.moveNext()) {
				queueId = db.getl(1);
				logger.log(Level.INFO, "Got the queueId: " + queueId);
			}
			else {
				logger.log(Level.INFO, "Couldn't get the queueId for agentId: " + agentId);
				job.put("Error", "Couldn't get the queueId for the agentId: " + agentId);
				job.put("Code", Integer.valueOf(-6));
				return job;
			}

			// Update to ASSIGNED
			db.query("UPDATE QUEUE set statusId=6,siteid=?, exechostid=? where statusId=5 and queueId=? limit 1", false, Integer.valueOf(siteId), Integer.valueOf(hostId), Long.valueOf(queueId));

			final int count = db.getUpdateCount();

			// Unlock QUEUE
			if (!db.query("UNLOCK TABLES", false))
				logger.log(Level.INFO, "Failed unlocking QUEUE");

			if (count > 0) {
				// we got something to run
				String jdl, user;

				db.query("select queueId, origjdl jdl, user from QUEUEJDL join QUEUE using (queueid) " + "join QUEUE_USER using (userId) where queueId=?", false, Long.valueOf(queueId));

				if (db.moveNext()) {
					logger.log(Level.INFO, "Updated and getting fields queueId, jdl, user");
					queueId = db.getl(1);
					jdl = db.gets(2);
					user = db.gets(3);
				}
				else {
					logger.log(Level.INFO, "Couldn't get the queueId, jdl and user for the agentId: " + agentId);
					job.put("Error", "Couldn't get the queueId, jdl and user for the agentId: " + agentId);
					job.put("Code", Integer.valueOf(-7));
					return job;
				}

				db.query("update QUEUEPROC set lastupdate=CURRENT_TIMESTAMP where queueId=?", false, Long.valueOf(queueId));

				db.query("update SITEQUEUES set ASSIGNED=ASSIGNED+1 where siteid=?", false, Integer.valueOf(siteId));
				db.query("update SITEQUEUES set WAITING=WAITING-1 where siteid=?", false, Integer.valueOf(siteId));

				TaskQueueUtils.deleteJobAgent(Long.parseLong(agentId), queueId);

				job.put("queueId", Long.valueOf(queueId));
				job.put("JDL", jdl);
				job.put("User", user);

				logger.log(Level.INFO, "Going to return " + queueId + " and " + user + " and " + jdl);
				return job;
			}
			logger.log(Level.INFO, "No jobs to give back");
			job.put("Error", "No jobs to give back");
			job.put("Code", Integer.valueOf(-2));
			return job;
		}
	}

	private static int checkQueueOpen(final String ce) {
		try (DBFunctions db = TaskQueueUtils.getQueueDB()) {
			if (db == null)
				return -2;

			if (monitor != null) {
				monitor.incrementCounter("TQ_db_lookup");
				monitor.incrementCounter("TQ_queue_open");
			}

			db.setReadOnly(true);
			db.setQueryTimeout(30);

			if (db.query("select count(1) from SITEQUEUES where blocked='open' and site='" + ce + "'") && db.moveNext() && db.geti(1) > 0)
				return 1;
			// TODO: use TaskQueueUtils.setSiteQueueStatus(ce,
			// "closed-blocked");

			return -1;
		}
	}

	/**
	 * @param matchRequest
	 * @return number of jobs waiting for a site given its parameters, or an
	 *         entry to JOBAGENT if asked for
	 */
	public static HashMap<String, Object> getNumberWaitingForSite(final HashMap<String, Object> matchRequest) {
		try (DBFunctions db = TaskQueueUtils.getQueueDB()) {
			if (db == null)
				return null;

			db.setQueryTimeout(60);

			final HashMap<String, Object> matchAnswer = new HashMap<>();
			matchAnswer.put("Code", Integer.valueOf(0));

			String where = "";
			String ret = "sum(counter) as counter";
			if (matchRequest.containsKey("Return"))
				ret = (String) matchRequest.get("Return");

			final ArrayList<Object> bindValues = new ArrayList<>();

			if (matchRequest.containsKey("TTL")) {
				where += "and ttl < ? ";
				bindValues.add(matchRequest.get("TTL"));
			}

			if (matchRequest.containsKey("Disk")) {
				where += "and disk < ? ";
				bindValues.add(matchRequest.get("Disk"));
			}

			if (matchRequest.containsKey("Site")) {
				where += "and (site='' or site like concat('%,',?,',%')";
				bindValues.add(matchRequest.get("Site"));
			}

			if (matchRequest.containsKey("Extrasites")) {
				@SuppressWarnings("unchecked")
				final ArrayList<String> extrasites = (ArrayList<String>) matchRequest.get("Extrasites");
				for (final String site : extrasites) {
					where += " or site like concat('%,',?,',%') ";
					bindValues.add(site);
				}
				where += ") ";
			}
			else
				if (matchRequest.containsKey("Site"))
					where += ") ";

			// skipping extrasites: used ?

			if (!matchRequest.containsKey("CVMFS"))
				if (matchRequest.containsKey("InstalledPackages")) {
					where += "and ? like packages ";
					bindValues.add(matchRequest.get("InstalledPackages"));
				}
				else {
					where += "and ? like packages ";
					bindValues.add(matchRequest.get("Packages"));
				}

			if (matchRequest.containsKey("Partition")) {
				where += "and ? like concat('%,',`partition`, '%,') ";
				bindValues.add(matchRequest.get("Partition"));
			}

			if (matchRequest.containsKey("CE")) {
				where += " and (ce like '' or ce like concat('%,',?,',%')) and noce not like concat('%,',?,',%')";
				bindValues.add(matchRequest.get("CE"));
				bindValues.add(matchRequest.get("CE"));
			}

			if (matchRequest.containsKey("Users")) {
				@SuppressWarnings("unchecked")
				final ArrayList<String> users = (ArrayList<String>) matchRequest.get("Users");
				String orconcat = " and (";
				for (final String user : users) {
					final Integer userId = TaskQueueUtils.getUserId(user);

					if (userId != null) {
						where += orconcat + "userId = ?";
						orconcat = " or ";
						bindValues.add(userId);
					}
				}
				where += ")";
			}

			if (matchRequest.containsKey("NoUsers")) {
				@SuppressWarnings("unchecked")
				final ArrayList<String> users = (ArrayList<String>) matchRequest.get("NoUsers");
				for (final String user : users) {
					final Integer userId = TaskQueueUtils.getUserId(user);

					if (userId != null) {
						where += " and userId != ? ";
						bindValues.add(userId);
					}
				}
			}

			db.setReadOnly(true);

			if (((Integer) matchRequest.get("Remote")).intValue() == 1) {
				logger.log(Level.INFO, "Checking for remote agents");

				// TODO: ask cache for ns:jobbroker key:remoteagents
				// $self->{CONFIG}->{CACHE_SERVICE_ADDRESS}?ns=jobbroker&key=remoteagents

				db.query("select distinct agentId from QUEUE where agentId is not null and statusId=5 and timestampdiff(SECOND,mtime,now())>=ifnull(remoteTimeout,43200)");

				if (db.moveNext()) {
					where += "and entryId in (";

					do {
						final int agentid = db.geti("agentId");
						where += agentid + ",";
					} while (db.moveNext());

					where = where.substring(0, where.length() - 1);
					where += ")";

					// TODO: store in cache
					// $self->{CONFIG}->{CACHE_SERVICE_ADDRESS}?ns=jobbroker&key=remoteagents&timeout=300&value=".Dumper([@$agents])
				}
				else
					return matchAnswer;
			}

			db.setReadOnly(true);

			final String q = "select " + ret + " from JOBAGENT where 1=1 " + where + " order by priority desc, price desc, oldestQueueId asc limit 1";
			logger.log(Level.INFO, "Going to select agents (" + q + ")");
			logger.log(Level.INFO, "Bind values: " + bindValues.toString());

			db.query(q, false, bindValues.toArray(new Object[0]));

			if (db.moveNext()) {
				final String[] columns = db.getColumnNames();
				matchAnswer.put("Code", Integer.valueOf(1));

				for (final String col : columns) {
					logger.log(Level.INFO, "Putting " + col + "-" + db.gets(col));
					matchAnswer.put(col, db.gets(col));
				}
				matchAnswer.put("CE", matchRequest.get("CE"));
				matchAnswer.put("Host", matchRequest.get("Host"));
			}

			return matchAnswer;
		}
	}

	/**
	 * @param host
	 * @param port
	 * @param ceName
	 * @param version
	 * @return a two-element list with the status code ([0]) and the number of
	 *         slots ([1])
	 */
	public static List<Integer> getNumberFreeSlots(String host, int port, String ceName, String version) {
		try (DBFunctions db = TaskQueueUtils.getQueueDB()) {
			if (db == null)
				return null;

			ArrayList<Integer> code_and_slots = new ArrayList<>();
			code_and_slots.add(Integer.valueOf(0));

			int hostId = TaskQueueUtils.getHostOrInsert(host, port, version);
			if (hostId == 0) {
				logger.severe("Error: getNumberFreeSlots, failed getHostOrInsert: " + host);
				code_and_slots.set(0, Integer.valueOf(1));
				return code_and_slots;
			}

			if (!TaskQueueUtils.updateHost(host, "CONNECTED", Integer.valueOf(1), port, version, ceName)) {
				logger.severe("Error: getNumberFreeSlots, failed updateHost: " + host);
				code_and_slots.set(0, Integer.valueOf(2));
				return code_and_slots;
			}

			if (!ceName.equals("")) {
				String blocking = TaskQueueUtils.getSiteQueueBlocked(ceName);

				if (blocking == null || !blocking.equals("open")) {
					logger.info("The queue " + ceName + " is blocked in the master queue!");
					TaskQueueUtils.setSiteQueueStatus(ceName, "closed-blocked");
					code_and_slots.set(0, Integer.valueOf(-2));
					return code_and_slots;
				}
			}

			ArrayList<Integer> slots = TaskQueueUtils.getNumberMaxAndQueuedCE(host, ceName);
			if (slots == null || slots.size() != 2) {
				logger.severe("Error: getNumberFreeSlots, failed to get slots: " + host + " - " + ceName);
				code_and_slots.set(0, Integer.valueOf(3));
				return code_and_slots;
			}

			code_and_slots.addAll(slots);

			return code_and_slots;
		}
	}

}
