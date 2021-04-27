package alien.site;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
// EXPERIMENTAL
// ========== imports for ORNL Titan
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import alien.api.JBoxServer;
import alien.api.catalogue.CatalogueApiUtils;
import alien.api.taskQueue.GetMatchJob;
import alien.api.taskQueue.TaskQueueApiUtils;
import alien.catalogue.BookingTable.BOOKING_STATE;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.catalogue.XmlCollection;
import alien.config.ConfigUtils;
import alien.io.IOUtils;
import alien.io.Transfer;
import alien.io.protocols.Protocol;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.MonitoringObject;
import alien.shell.commands.JAliEnCOMMander;
import alien.site.packman.CVMFS;
import alien.site.packman.PackMan;
import alien.taskQueue.JDL;
import alien.taskQueue.Job;
import alien.taskQueue.JobStatus;
import apmon.ApMonException;
import apmon.ApMonMonitoringConstants;
import apmon.BkThread;
import lia.util.Utils;

/**
 * @author mmmartin, ron
 * @since Apr 1, 2015
 */
public class JobAgentProxy extends Thread implements MonitoringObject {

	// Folders and files
	private File tempDir = null;
	private static final String defaultOutputDirPrefix = "/jalien-job-";
	private String jobWorkdir = "";

	// Variables passed through VoBox environment
	private final Map<String, String> env = System.getenv();
	private final String site;
	private final String ce;
	private int origTtl;

	// Job variables
	private JDL jdl = null;
	private long queueId;
	private String jobToken;
	private String username;
	private String jobAgentId = "";
	private String workdir = null;
	private HashMap<String, Object> matchedJob = null;
	private String partition;
	private String ceRequirements = "";
	private List<String> packages;
	private List<String> installedPackages;
	private ArrayList<String> extrasites;
	private HashMap<String, Object> siteMap = new HashMap<>();
	private int workdirMaxSizeMB;
	private int jobMaxMemoryMB;
	private JobStatus jobStatus;

	private int totalJobs;
	private final long jobAgentStartTime = new java.util.Date().getTime();

	// Other
	private PackMan packMan = null;
	private String hostName = null;
	private String alienCm = null;
	private final int pid;
	// private final JAliEnCOMMander commander = JAliEnCOMMander.getInstance();
	// private final CatalogueApiUtils c_api = new CatalogueApiUtils(commander);
	// private static final HashMap<String, Integer> jaStatus = new HashMap<>();

	private JAliEnCOMMander commander; // = JAliEnCOMMander.getInstance();
	private CatalogueApiUtils c_api; // = new CatalogueApiUtils(commander);
	private static HashMap<String, Integer> jaStatus; // = new HashMap<>();

	static {
		jaStatus.put("REQUESTING_JOB", Integer.valueOf(1));
		jaStatus.put("INSTALLING_PKGS", Integer.valueOf(2));
		jaStatus.put("JOB_STARTED", Integer.valueOf(3));
		jaStatus.put("RUNNING_JOB", Integer.valueOf(4));
		jaStatus.put("DONE", Integer.valueOf(5));
		jaStatus.put("ERROR_HC", Integer.valueOf(-1)); // error in getting host classad
		jaStatus.put("ERROR_IP", Integer.valueOf(-2)); // error installing packages
		jaStatus.put("ERROR_GET_JDL", Integer.valueOf(-3)); // error getting jdl
		jaStatus.put("ERROR_JDL", Integer.valueOf(-4)); // incorrect jdl
		jaStatus.put("ERROR_DIRS", Integer.valueOf(-5)); // error creating directories, not enough free space in workdir
		jaStatus.put("ERROR_START", Integer.valueOf(-6)); // error forking to start job
	}

	private static final Logger logger = ConfigUtils.getLogger(JobAgentProxy.class.getCanonicalName());

	private static final Monitor monitor = MonitorFactory.getMonitor(JobAgentProxy.class.getCanonicalName());

	// Resource monitoring vars

	private Integer RES_NOCPUS = Integer.valueOf(1);
	private String RES_CPUMHZ = "";
	private String RES_CPUFAMILY = "";

	// EXPERIMENTAL
	// for ORNL Titan
	private String dbname;
	private String monitoring_dbname;
	private String dblink;
	private int numCores;
	private int current_rank;

	/**
	 */
	public JobAgentProxy() {

		commander = JAliEnCOMMander.getInstance();
		c_api = new CatalogueApiUtils(commander);

		site = env.get("site"); // or ConfigUtils.getConfig().gets("alice_close_site").trim();
		ce = env.get("CE");

		totalJobs = 0;

		partition = "";
		if (env.containsKey("partition"))
			partition = env.get("partition");

		if (env.containsKey("TTL"))
			origTtl = Integer.parseInt(env.get("TTL"));
		else
			origTtl = 12 * 3600;

		if (env.containsKey("cerequirements"))
			ceRequirements = env.get("cerequirements");

		if (env.containsKey("closeSE"))
			extrasites = new ArrayList<>(Arrays.asList(env.get("closeSE").split(",")));

		hostName = ConfigUtils.getLocalHostname();

		alienCm = hostName;
		if (env.containsKey("ALIEN_CM_AS_LDAP_PROXY"))
			alienCm = env.get("ALIEN_CM_AS_LDAP_PROXY");

		if (env.containsKey("ALIEN_JOBAGENT_ID"))
			jobAgentId = env.get("ALIEN_JOBAGENT_ID");
		pid = Integer.parseInt(ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);

		workdir = env.get("HOME");
		if (env.containsKey("WORKDIR"))
			workdir = env.get("WORKDIR");
		if (env.containsKey("TMPBATCH"))
			workdir = env.get("TMPBATCH");

		siteMap = getSiteParameters();

		Hashtable<Long, String> cpuinfo;
		try {
			cpuinfo = BkThread.getCpuInfo();
			RES_CPUFAMILY = cpuinfo.get(ApMonMonitoringConstants.LGEN_CPU_FAMILY);
			RES_CPUMHZ = cpuinfo.get(ApMonMonitoringConstants.LGEN_CPU_MHZ);
			RES_CPUMHZ = RES_CPUMHZ.substring(0, RES_CPUMHZ.indexOf("."));
			RES_NOCPUS = Integer.valueOf(BkThread.getNumCPUs());

			System.out.println("CPUFAMILY: " + RES_CPUFAMILY);
			System.out.println("CPUMHZ: " + RES_CPUMHZ);
			System.out.println("NOCPUS: " + RES_NOCPUS);
		}
		catch (final IOException e) {
			System.out.println("Problem with the monitoring objects IO Exception: " + e.toString());
		}
		catch (final ApMonException e) {
			System.out.println("Problem with the monitoring objects ApMon Exception: " + e.toString());
		}

		monitor.addMonitoring("jobAgent-TODO", this);

		// EXPERIMENTAL
		// ========= for ORNL Titan

		try {
			try (Connection connection = DriverManager.getConnection(dbname); Statement statement = connection.createStatement();) {
				String titan_cores_str = null;
				if (env.containsKey("TITAN_CORES_CLAIMED"))
					titan_cores_str = env.get("TITAN_CORES_CLAIMED");
				if (titan_cores_str == null)
					throw new NumberFormatException("Titan cores number not defined");
				numCores = Integer.parseInt(titan_cores_str);
				if (numCores <= 0)
					throw new NumberFormatException("Titan cores number is invalid");
				dbname = String.format("jdbc:sqlite:" + workdir + "/jobagent_titan_%d.db", Integer.valueOf(pid));

				statement.executeUpdate("DROP TABLE IF EXISTS alien_jobs");
				statement.executeUpdate(
						"CREATE TABLE alien_jobs (rank INTEGER NOT NULL, queue_id VARCHAR(20), job_folder VARCHAR(256) NOT NULL, status CHAR(1), executable VARCHAR(256), validation VARCHAR(256),"
								+ "environment TEXT," + "exec_code INTEGER DEFAULT -1, val_code INTEGER DEFAULT -1)");
				statement.executeUpdate("CREATE TEMPORARY TABLE numbers(n INTEGER)");
				statement.executeUpdate("INSERT INTO numbers " + "select 1 " + "from (" + "select 0 union select 1 union select 2 " + ") a, ("
						+ "select 0 union select 1 union select 2 union select 3 " + "union select 4 union select 5 union select 6 " + "union select 7 union select 8 union select 9" + ") b, ("
						+ "select 0 union select 1 union select 2 union select 3 " + "union select 4 union select 5 union select 6 " + "union select 7 union select 8 union select 9" + ") c, ("
						+ "select 0 union select 1 union select 2 union select 3 " + "union select 4 union select 5 union select 6 " + "union select 7 union select 8 union select 9" + ") d, ("
						+ "select 0 union select 1 union select 2 union select 3 " + "union select 4 union select 5 union select 6 " + "union select 7 union select 8 union select 9" + ") e, ("
						+ "select 0 union select 1 union select 2 union select 3 " + "union select 4 union select 5 union select 6 " + "union select 7 union select 8 union select 9" + ") f");
				statement.executeUpdate(String.format("INSERT INTO alien_jobs SELECT rowid-1, 0, '', 'I', '', '', '', 0, 0 FROM numbers LIMIT %d", Integer.valueOf(numCores)));
				statement.executeUpdate("DROP TABLE numbers");
			}

			monitoring_dbname = String.format("jdbc:sqlite:" + workdir + "/jobagent_titan_%d.db.monitoring", Integer.valueOf(pid));

			try (Connection connection = DriverManager.getConnection(monitoring_dbname); Statement statement = connection.createStatement();) {
				// creating monitoring db
				statement.executeUpdate("DROP TABLE IF EXISTS alien_jobs_monitoring");
				statement.executeUpdate("CREATE TABLE alien_jobs_monitoring (queue_id VARCHAR(20), resources VARCHAR(100))");
				connection.close();

				dblink = "/lustre/atlas/scratch/psvirin/csc108/workdir/database.lnk";
				try (PrintWriter out = new PrintWriter(dblink)) {
					out.println(dbname);
				}
			}
		}
		catch (

		SQLException e) {
			System.err.println("Unable to start JobAgentProxy for Titan because of SQLite exception: " + e.getMessage());
			System.exit(-1);
		}
		catch (NumberFormatException e) {
			System.err.println("Number of Titan cores (TITAN_CORES_CLAIMED environment variable) has incorrect value: " + e.getMessage());
		}
		catch (@SuppressWarnings("unused") Exception e) {
			System.err.println("Failed to open dblink file " + dblink);
		}

		// here create monitor thread
		class TitanMonitorThread extends Thread {
			private JobAgentProxy ja;

			public TitanMonitorThread(JobAgentProxy ja) {
				this.ja = ja;
			}

			@Override
			public void run() {
				while (true) {
					try {
						Thread.sleep(5 * 60 * 1000);
					}
					catch (@SuppressWarnings("unused") InterruptedException e) {
						// ignore
					}
					checkProcessResources();
					ja.sendProcessResources();
				}
			}
		}

		new TitanMonitorThread(this).start();

		// END EXPERIMENTAL
	}

	@Override
	public void run() {

		logger.log(Level.INFO, "Starting JobAgentProxy in " + hostName);

		// We start, if needed, the node JBox
		// Does it check a previous one is already running?
		try {
			System.out.println("Trying to start JBox");
			JBoxServer.startJBoxService();
		}
		catch (final Exception e) {
			System.err.println("Unable to start JBox.");
			e.printStackTrace();
		}

		class TitanJobStatus {
			public int rank;
			public Long aliEnId;
			public String jobFolder;
			public String status;
			public int executionCode;
			public int validationCode;

			public TitanJobStatus(int r, Long qid, String job_folder, String st, int exec_code, int val_code) {
				rank = r;
				aliEnId = qid;
				jobFolder = job_folder;
				status = st;
				executionCode = exec_code;
				validationCode = val_code;
			}
		}

		while (true) {
			if (!updateDynamicParameters()) {
				System.err.println("update for dynamic parameters failed. Stopping the agent.");
				break;
			}

			LinkedList<TitanJobStatus> idleRanks = new LinkedList<>();
			try (Connection connection = DriverManager.getConnection(dbname);
					Statement statement = connection.createStatement();
					ResultSet rs = statement.executeQuery("SELECT rank, queue_id, job_folder, status, exec_code, val_code FROM alien_jobs WHERE status='D' OR status='I'")) {
				while (rs.next()) {
					idleRanks.add(new TitanJobStatus(rs.getInt("rank"), Long.valueOf(rs.getLong("queue_id")), rs.getString("job_folder"), rs.getString("status"), rs.getInt("exec_code"),
							rs.getInt("val_code")));
				}
			}
			catch (SQLException e) {
				System.err.println("Getting free slots failed: " + e.getMessage());
				continue;
			}
			int count = idleRanks.size();
			System.out.println("We can start " + count + " jobs");

			if (count == 0) {
				try {
					Thread.sleep(30000);
				}
				catch (@SuppressWarnings("unused") InterruptedException e) {
					// ignore
				}
				finally {
					System.out.println("Going for the next round....");
				}
				continue;
			}

			// uploading data from finished jobs
			for (TitanJobStatus js : idleRanks) {
				if (js.status.equals("D")) {
					queueId = js.aliEnId.longValue();
					System.err.println(String.format("Uploading job: %d", js.aliEnId));
					jobWorkdir = js.jobFolder;
					tempDir = new File(js.jobFolder);
					// read JDL from file
					String jdl_content = null;
					try {
						byte[] encoded = Files.readAllBytes(Paths.get(js.jobFolder + "/jdl"));
						jdl_content = new String(encoded, Charset.defaultCharset());
					}
					catch (IOException e) {
						System.err.println("Unable to read JDL file: " + e.getMessage());
					}
					if (jdl_content != null) {
						jdl = null;
						try {
							jdl = new JDL(Job.sanitizeJDL(jdl_content));
						}
						catch (IOException e) {
							System.err.println("Unable to parse JDL: " + e.getMessage());
						}
						if (jdl != null) {
							if (js.executionCode != 0)
								changeStatus(JobStatus.ERROR_E);
							else if (js.validationCode != 0)
								changeStatus(JobStatus.ERROR_V);
							else
								changeStatus(JobStatus.SAVING);
							uploadOutputFiles(); // upload data
							cleanup();
							System.err.println(String.format("Upload job %d finished", js.aliEnId));

							try (Connection connection = DriverManager.getConnection(dbname); Statement statement = connection.createStatement();) {
								statement.executeUpdate(String.format("UPDATE alien_jobs SET status='I' WHERE rank=%d", Integer.valueOf(js.rank)));
							}
							catch (@SuppressWarnings("unused") SQLException e) {
								System.err.println("Update job state to I failed");
							}
						}
					}
				}
			}

			while (count > 0) {
				System.out.println(siteMap.toString());
				TitanJobStatus js = idleRanks.pop();

				try {
					logger.log(Level.INFO, "Trying to get a match...");

					monitor.sendParameter("ja_status", getJaStatusForML("REQUESTING_JOB"));
					monitor.sendParameter("TTL", siteMap.get("TTL"));

					final GetMatchJob jobMatch = commander.q_api.getMatchJob(siteMap);
					matchedJob = jobMatch.getMatchJob();

					// TODELETE
					if (matchedJob != null)
						System.out.println(matchedJob.toString());

					if (matchedJob != null && !matchedJob.containsKey("Error")) {
						jdl = new JDL(Job.sanitizeJDL((String) matchedJob.get("JDL")));
						queueId = ((Long) matchedJob.get("queueId")).intValue();
						username = (String) matchedJob.get("User");
						jobToken = (String) matchedJob.get("jobToken");

						// TODO: commander.setUser(username); commander.setSite(site);

						System.out.println(jdl.getExecutable());
						System.out.println(jdl.toString());
						System.out.println("====================");
						System.out.println(username);
						System.out.println(queueId);
						System.out.println(jobToken);

						// EXPERIMENTAL
						// for ORNL Titan
						current_rank = js.rank;

						// process payload
						handleJob();

						// cleanup();
					}
					else {
						if (matchedJob != null && matchedJob.containsKey("Error")) {
							logger.log(Level.INFO, (String) matchedJob.get("Error"));

							if (Integer.valueOf(3).equals(matchedJob.get("Code"))) {
								@SuppressWarnings("unchecked")
								final ArrayList<String> packToInstall = (ArrayList<String>) matchedJob.get("Packages");
								monitor.sendParameter("ja_status", getJaStatusForML("INSTALLING_PKGS"));
								installPackages(packToInstall);
							}
							else if (Integer.valueOf(-2).equals(matchedJob.get("Code"))) {
								logger.log(Level.INFO, "Nothing to run for now, idling for a while");
								count = 1; // breaking the loop
							}
						}
						else {
							// EXPERIMENTAL
							// for ORNL Titan
							logger.log(Level.INFO, "We didn't get anything back. Nothing to run right now. Idling 20secs zZz...");
							break;
						}

						/*
						 * try {
						 * // TODO?: monitor.sendBgMonitoring
						 * sleep(60000);
						 * break;
						 * } catch (final InterruptedException e) {
						 * e.printStackTrace();
						 * }
						 */
					}
				}
				catch (final Exception e) {
					logger.log(Level.INFO, "Error getting a matching job: " + e);
				}
				count--;
			}

			try {
				sleep(60000);
			}
			catch (InterruptedException e) {
				System.err.println("Sleep after full JA cycle failed: " + e.getMessage());
			}
		}

		logger.log(Level.INFO, "JobAgentProxy finished, id: " + jobAgentId + " totalJobs: " + totalJobs);
		// EXPERIMENTAL
		// For ORNL Titan
		// TO DELETE: use deleteOnExit instead
		File f = new File(dbname);
		f.delete();
		f = new File(dblink);
		f.delete();

		System.exit(0);
	}

	private void cleanup() {
		System.out.println("Cleaning up after execution...Removing sandbox: " + jobWorkdir);
		// Remove sandbox, TODO: use Java builtin
		Utils.getOutput("rm -rf " + jobWorkdir);
	}

	private Map<String, String> installPackages(final ArrayList<String> packToInstall) {
		Map<String, String> ok = null;

		for (final String pack : packToInstall) {
			ok = packMan.installPackage(username, pack, null);
			if (ok == null) {
				logger.log(Level.INFO, "Error installing the package " + pack);
				monitor.sendParameter("ja_status", "ERROR_IP");
				System.out.println("Error installing " + pack);
				System.exit(1);
			}
		}
		return ok;
	}

	private static Integer getJaStatusForML(final String status) {
		final Integer value = jaStatus.get(status);

		return value != null ? value : Integer.valueOf(0);
	}

	/**
	 * @return the site parameters to send to the job broker (packages, ttl, ce/site...)
	 */
	private HashMap<String, Object> getSiteParameters() {
		logger.log(Level.INFO, "Getting jobAgent map");

		// getting packages from PackMan
		packMan = getPackman();
		packages = packMan.getListPackages();
		installedPackages = packMan.getListInstalledPackages();

		// get users from cerequirements field
		final ArrayList<String> users = new ArrayList<>();
		if (!ceRequirements.equals("")) {
			final Pattern p = Pattern.compile("\\s*other.user\\s*==\\s*\"(\\w+)\"");
			final Matcher m = p.matcher(ceRequirements);
			while (m.find())
				users.add(m.group(1));
		}
		// setting entries for the map object
		siteMap.put("TTL", Integer.valueOf(origTtl));

		// We prepare the packages for direct matching
		String packs = ",";
		Collections.sort(packages);
		for (final String pack : packages)
			packs += pack + ",,";

		packs = packs.substring(0, packs.length() - 1);

		String instpacks = ",";
		Collections.sort(installedPackages);
		for (final String pack : installedPackages)
			instpacks += pack + ",,";

		instpacks = instpacks.substring(0, instpacks.length() - 1);

		siteMap.put("Platform", ConfigUtils.getPlatform());
		siteMap.put("Packages", packs);
		siteMap.put("InstalledPackages", instpacks);
		siteMap.put("CE", ce);
		siteMap.put("Site", site);
		if (users.size() > 0)
			siteMap.put("Users", users);
		if (extrasites != null && extrasites.size() > 0)
			siteMap.put("Extrasites", extrasites);
		siteMap.put("Host", alienCm);
		siteMap.put("Disk", Long.valueOf(JobAgent.getFreeSpace(workdir) / 1024));

		if (!partition.equals(""))
			siteMap.put("Partition", partition);

		return siteMap;
	}

	private PackMan getPackman() {
		switch (env.get("installationMethod")) {
			case "CVMFS":
				siteMap.put("CVMFS", Integer.valueOf(1));
				return new CVMFS("/lustre/atlas/proj-shared/csc108/psvirin/alice.cern.ch/alice.cern.ch/bin/");
			default:
				siteMap.put("CVMFS", Integer.valueOf(1));
				return new CVMFS("/lustre/atlas/proj-shared/csc108/psvirin/alice.cern.ch/alice.cern.ch/bin/");
		}
	}

	/**
	 * updates jobagent parameters that change between job requests
	 *
	 * @return false if we can't run because of current conditions, true if positive
	 */
	private boolean updateDynamicParameters() {
		logger.log(Level.INFO, "Updating dynamic parameters of jobAgent map");

		// free disk recalculation
		final long space = JobAgent.getFreeSpace(workdir) / 1024;

		// ttl recalculation
		final long jobAgentCurrentTime = new java.util.Date().getTime();
		// final int time_subs = (int) (jobAgentCurrentTime - jobAgentStartTime);
		final long time_subs = jobAgentCurrentTime - jobAgentStartTime;
		// int timeleft = origTtl - time_subs - 300;
		long timeleft = origTtl * 1000 - time_subs - 300 * 1000;

		logger.log(Level.INFO, "Still have " + timeleft + " seconds to live (" + jobAgentCurrentTime + "-" + jobAgentStartTime + "=" + time_subs + ")");

		// we check if the proxy timeleft is smaller than the ttl itself
		final int proxy = getRemainingProxyTime();
		logger.log(Level.INFO, "Proxy timeleft is " + proxy);
		// if (proxy > 0 && proxy < timeleft)
		if (proxy > 0 && proxy * 1000 < timeleft)
			timeleft = proxy;

		// safety time for saving, etc
		timeleft -= 300;

		// what is the minimum we want to run with? (100MB?)
		if (space <= 100 * 1024 * 1024) {
			logger.log(Level.INFO, "There is not enough space left: " + space);
			return false;
		}

		if (timeleft <= 0) {
			logger.log(Level.INFO, "There is not enough time left: " + timeleft);
			return false;
		}

		siteMap.put("Disk", Long.valueOf(space));
		// siteMap.put("TTL", Integer.valueOf(timeleft));
		siteMap.put("TTL", Long.valueOf(timeleft / 1000));

		return true;
	}

	/**
	 * @return the time in seconds that proxy is still valid for
	 */
	private int getRemainingProxyTime() {
		// TODO: to be modified!
		return origTtl;
	}

	/*
	 * private long ttlForJob() {
	 * final Integer iTTL = jdl.getInteger("TTL");
	 * 
	 * int ttl = (iTTL != null ? iTTL.intValue() : 0) + 300;
	 * commander.q_api.putJobLog(queueId, "trace", "Job asks to run for " + ttl + " seconds");
	 * 
	 * final String proxyttl = jdl.gets("ProxyTTL");
	 * if (proxyttl != null) {
	 * ttl = ((Integer) siteMap.get("TTL")).intValue() - 600;
	 * commander.q_api.putJobLog(queueId, "trace", "ProxyTTL enabled, running for " + ttl + " seconds");
	 * }
	 * 
	 * return ttl;
	 * }
	 */

	private void handleJob() {
		totalJobs++;
		try {
			logger.log(Level.INFO, "Started JA with: " + jdl);

			commander.q_api.putJobLog(queueId, "trace", "Job preparing to run in: " + hostName);

			changeStatus(JobStatus.STARTED);

			if (!createWorkDir() || !getInputFiles()) {
				changeStatus(JobStatus.ERROR_IB);
				return;
			}

			getMemoryRequirements();

			// EXPERIMENTAL
			// for ORNL Titan
			// save jdl into file
			try (PrintWriter out = new PrintWriter(tempDir + "/jdl")) {
				out.println(jdl);
			}

			// run payload
			changeStatus(JobStatus.RUNNING);
			if (execute() < 0)
				changeStatus(JobStatus.ERROR_E);

			// EXPERIMENTAL
			// for ORNL Titan
			/*
			 * if (!validate())
			 * changeStatus(JobStatus.ERROR_V);
			 * 
			 * if (jobStatus == JobStatus.RUNNING)
			 * changeStatus(JobStatus.SAVING);
			 * 
			 * uploadOutputFiles();
			 */

		}
		catch (final Exception e) {
			System.err.println("Unable to handle job");
			e.printStackTrace();
		}
	}

	// EXPERIMENTAL
	// for ORNL Titan
	private String getLocalCommand(final String command, final List<String> arguments) {
		final int idx = command.lastIndexOf('/');

		final String cmdStrip = idx < 0 ? command : command.substring(idx + 1);

		final File fExe = new File(tempDir, cmdStrip);

		if (!fExe.exists())
			return null;

		fExe.setExecutable(true);

		// cmd.add(fExe.getAbsolutePath());

		/*
		 * if (arguments != null && arguments.size() > 0){
		 * for (final String argument : arguments){
		 * if (argument.trim().length() > 0) {
		 * final StringTokenizer st = new StringTokenizer(argument);
		 * 
		 * while (st.hasMoreTokens())
		 * cmd.add(st.nextToken());
		 * }
		 * }
		 * }
		 */

		// JAVA 8
		String argString = "";
		if (arguments != null) {
			for (String s : arguments) {
				argString += " " + s;
			}
		}

		// System.err.println("Executing: " + cmd + ", arguments is " + arguments + " pid: " + pid);
		return fExe.getAbsolutePath() + argString;
	}
	// end EXPERIMENTAL

	/*
	 * @param command
	 * 
	 * @param arguments
	 * 
	 * @param timeout
	 * 
	 * @return <cod>0</code> if everything went fine, a positive number with the process exit code (which would mean a problem) and a negative error code in case of timeout or other supervised
	 * execution errors
	 * private int executeCommand(final String command, final List<String> arguments, final long timeout, final TimeUnit unit, final boolean monitorJob) {
	 * final List<String> cmd = new LinkedList<>();
	 * 
	 * final int idx = command.lastIndexOf('/');
	 * 
	 * final String cmdStrip = idx < 0 ? command : command.substring(idx + 1);
	 * 
	 * final File fExe = new File(tempDir, cmdStrip);
	 * 
	 * if (!fExe.exists())
	 * return -1;
	 * 
	 * fExe.setExecutable(true);
	 * 
	 * cmd.add(fExe.getAbsolutePath());
	 * 
	 * if (arguments != null && arguments.size() > 0)
	 * for (final String argument : arguments)
	 * if (argument.trim().length() > 0) {
	 * final StringTokenizer st = new StringTokenizer(argument);
	 * 
	 * while (st.hasMoreTokens())
	 * cmd.add(st.nextToken());
	 * }
	 * 
	 * System.err.println("Executing: " + cmd + ", arguments is " + arguments + " pid: " + pid);
	 * 
	 * // final ProcessBuilder pBuilder = new ProcessBuilder(cmd);
	 * // final List<String> cmd1 = new LinkedList<>();
	 * // cmd1.add("/lustre/atlas/scratch/psvirin/csc108/tmp/sq.sh");
	 * // cmd1.add(tempDir.getAbsolutePath());
	 * // cmd1.add(fExe.getAbsolutePath());
	 * // ProcessBuilder pBuilder1 = new ProcessBuilder(cmd1);
	 * // ProcessBuilder pBuilder = new ProcessBuilder(cmd);
	 * //////////*
	 * try{
	 * pBuilder1.start();
	 * //Process p;
	 * //p = Runtime.getRuntime().exec("sqlite3 /lustre/atlas/scratch/psvirin/csc108/alien.db \"INSERT INTO tasks_alien VALUES(0, '" + fExe.getAbsolutePath() + "', 'Q');\"");
	 * //p = Runtime.getRuntime().exec("/lustre/atlas/scratch/psvirin/csc108/add_to_db");
	 * //p.waitFor();
	 * //BufferedReader reader =
	 * //new BufferedReader(new InputStreamReader(p.getInputStream()));
	 * 
	 * //String line = "";
	 * //while ((line = reader.readLine())!= null) {
	 * //System.out.println(line);
	 * //}
	 * //System.out.println("SQLITE run");
	 * }
	 * catch(Exception e){
	 * System.out.println(e.getMessage());
	 * }
	 * /////////////
	 * 
	 * // EXPERIMENTAL
	 * // pBuilder = new ProcessBuilder(cmd);
	 * ProcessBuilder pBuilder = new ProcessBuilder("sleep", "200");
	 * 
	 * pBuilder.directory(tempDir);
	 * 
	 * final HashMap<String, String> environment_packages = getJobPackagesEnvironment();
	 * final Map<String, String> processEnv = pBuilder.environment();processEnv.putAll(environment_packages);processEnv.putAll(
	 * 
	 * loadJDLEnvironmentVariables());
	 * 
	 * pBuilder.redirectOutput(Redirect.appendTo(new File(tempDir, "stdout")));
	 * pBuilder.redirectError(Redirect.appendTo(new File(tempDir, "stderr")));
	 * // pBuilder.redirectErrorStream(true);
	 * 
	 * final Process p;
	 * 
	 * try {
	 * changeStatus(JobStatus.RUNNING);
	 * 
	 * p = pBuilder.start();
	 * } catch (final IOException ioe) {
	 * System.out.println("Exception running " + cmd + " : " + ioe.getMessage());
	 * return -2;
	 * }
	 * 
	 * final Timer t = new Timer();
	 * t.schedule(new TimerTask() {
	 * 
	 * @Override
	 * public void run() {
	 * p.destroy();
	 * }
	 * }, TimeUnit.MILLISECONDS.convert(timeout, unit));
	 * 
	 * mj = new MonitoredJob(pid, jobWorkdir, ce, hostName);
	 * final Vector<Integer> child = mj.getChildren();
	 * if (child == null || child.size() <= 1) {
	 * System.err.println("Can't get children. Failed to execute? " + cmd.toString() + " child: " + child);
	 * return -1;
	 * }
	 * System.out.println("Child: " + child.get(1).toString());
	 * 
	 * boolean processNotFinished = true;
	 * int code = 0;
	 * 
	 * if (monitorJob) {
	 * payloadPID = child.get(1).intValue();
	 * apmon.addJobToMonitor(payloadPID, jobWorkdir, ce, hostName); // TODO: test
	 * mj = new MonitoredJob(payloadPID, jobWorkdir, ce, hostName);
	 * checkProcessResources();
	 * sendProcessResources();
	 * }
	 * 
	 * int monitor_loops = 0;
	 * try {
	 * while (processNotFinished)
	 * try {
	 * Thread.sleep(60 * 1000);
	 * code = p.exitValue();
	 * processNotFinished = false;
	 * } catch (final IllegalThreadStateException e) {
	 * // TODO: check job-token exist (job not killed)
	 * 
	 * // process hasn't terminated
	 * if (monitorJob) {
	 * monitor_loops++;
	 * final String error = checkProcessResources();
	 * if (error != null) {
	 * p.destroy();
	 * System.out.println("Process overusing resources: " + error);
	 * return -2;
	 * }
	 * if (monitor_loops == 10) {
	 * monitor_loops = 0;
	 * sendProcessResources();
	 * }
	 * }
	 * }
	 * return code;
	 * } catch (final InterruptedException ie) {
	 * System.out.println("Interrupted while waiting for this command to finish: " + cmd.toString());
	 * return -2;
	 * } finally {
	 * t.cancel();
	 * }
	 * }
	 */

	private void sendProcessResources() {
		// EXPERIMENTAL
		// for ORNL Titan
		class ProcInfoPair {
			public final long queue_id;
			public final String procinfo;

			public ProcInfoPair(String queue_id, String procinfo) {
				this.queue_id = Long.parseLong(queue_id);
				this.procinfo = procinfo;
			}
		}
		LinkedList<ProcInfoPair> job_resources = new LinkedList<>();

		try (Connection connection = DriverManager.getConnection(monitoring_dbname);
				Statement statement = connection.createStatement();
				ResultSet rs = statement.executeQuery("SELECT * FROM alien_jobs_monitoring");) {
			// read all
			while (rs.next()) {
				job_resources.add(new ProcInfoPair(rs.getString("queue_id"), rs.getString("resources")));
				// idleRanks.add(new TitanJobStatus(rs.getInt("rank"), rs.getLong("queue_id"), rs.getString("job_folder"),
				// rs.getString("status"), rs.getInt("exec_code"), rs.getInt("val_code")));
			}
			// delete all
			statement.executeUpdate("DELETE FROM alien_jobs_monitoring");
		}
		catch (SQLException e) {
			System.err.println("Unable to get monitoring data: " + e.getMessage());
		}
		// foreach send

		// runtime(date formatted) start cpu(%) mem cputime rsz vsize ncpu cpufamily cpuspeed resourcecost maxrss maxvss ksi2k
		// final String procinfo = String.format("%s %d %.2f %.2f %.2f %.2f %.2f %d %s %s %s %.2f %.2f 1000", RES_FRUNTIME, RES_RUNTIME, RES_CPUUSAGE, RES_MEMUSAGE, RES_CPUTIME, RES_RMEM, RES_VMEM,
		// RES_NOCPUS, RES_CPUFAMILY, RES_CPUMHZ, RES_RESOURCEUSAGE, RES_RMEMMAX, RES_VMEMMAX);
		// System.out.println("+++++ Sending resources info +++++");
		// System.out.println(procinfo);
		for (ProcInfoPair pi : job_resources) {
			commander.q_api.putJobLog(pi.queue_id, "proc", pi.procinfo);
		}
	}

	private static String checkProcessResources() {
		String error = null;
		// EXPERIMENTAL
		// for ORNL Titan
		/*
		 * System.out.println("Checking resources usage");
		 * 
		 * try {
		 * final HashMap<Long, Double> jobinfo = mj.readJobInfo();
		 * final HashMap<Long, Double> diskinfo = mj.readJobDiskUsage();
		 * 
		 * // gettng cpu, memory and runtime info
		 * RES_WORKDIR_SIZE = diskinfo.get(ApMonMonitoringConstants.LJOB_WORKDIR_SIZE);
		 * RES_VMEM = Double.valueOf(jobinfo.get(ApMonMonitoringConstants.LJOB_VIRTUALMEM).doubleValue() / 1024);
		 * RES_RMEM = Double.valueOf(jobinfo.get(ApMonMonitoringConstants.LJOB_RSS).doubleValue() / 1024);
		 * RES_CPUTIME = Double.valueOf(jobinfo.get(ApMonMonitoringConstants.LJOB_CPU_TIME).doubleValue());
		 * RES_CPUUSAGE = Double.valueOf(jobinfo.get(ApMonMonitoringConstants.LJOB_CPU_USAGE).doubleValue());
		 * RES_RUNTIME = Long.valueOf(jobinfo.get(ApMonMonitoringConstants.LJOB_RUN_TIME).longValue());
		 * RES_MEMUSAGE = jobinfo.get(ApMonMonitoringConstants.LJOB_MEM_USAGE);
		 * RES_RESOURCEUSAGE = Format.showDottedDouble(RES_CPUTIME.doubleValue() * Double.parseDouble(RES_CPUMHZ) / 1000, 2);
		 * 
		 * //RES_WORKDIR_SIZE = 0;
		 * //RES_VMEM = 0;
		 * //RES_RMEM = 0;
		 * //RES_CPUTIME = 0;
		 * //RES_CPUUSAGE = 0;
		 * //RES_RUNTIME = 0;
		 * //RES_MEMUSAGE = 0;
		 * //RES_RESOURCEUSAGE = 0;
		 * 
		 * 
		 * // max memory consumption
		 * if (RES_RMEM.doubleValue() > RES_RMEMMAX.doubleValue())
		 * RES_RMEMMAX = RES_RMEM;
		 * 
		 * if (RES_VMEM.doubleValue() > RES_VMEMMAX.doubleValue())
		 * RES_VMEMMAX = RES_VMEM;
		 * 
		 * // formatted runtime
		 * if (RES_RUNTIME.doubleValue() < 60)
		 * RES_FRUNTIME = String.format("00:00:%02d", RES_RUNTIME);
		 * else if (RES_RUNTIME.doubleValue() < 3600){
		 * System.out.println(RES_RUNTIME.doubleValue()/60);
		 * System.out.println(Double.valueOf(RES_RUNTIME.doubleValue() % 60));
		 * //RES_FRUNTIME = String.format("00:%02d:%02d", Double.valueOf(RES_RUNTIME.doubleValue() / 60), Double.valueOf(RES_RUNTIME.doubleValue() % 60));
		 * RES_FRUNTIME = String.format("00:%02d:%02d", RES_RUNTIME / 60, RES_RUNTIME % 60);
		 * }
		 * else
		 * RES_FRUNTIME = String.format("%02d:%02d:%02d", Double.valueOf(RES_RUNTIME.doubleValue() / 3600),
		 * Double.valueOf((RES_RUNTIME.doubleValue() - (RES_RUNTIME.doubleValue() / 3600) * 3600) / 60),
		 * Double.valueOf((RES_RUNTIME.doubleValue() - (RES_RUNTIME.doubleValue() / 3600) * 3600) % 60));
		 * 
		 * // check disk usage
		 * if (workdirMaxSizeMB != 0 && RES_WORKDIR_SIZE.doubleValue() > workdirMaxSizeMB)
		 * error = "Disk space limit is " + workdirMaxSizeMB + ", using " + RES_WORKDIR_SIZE;
		 * 
		 * // check disk usage
		 * if (jobMaxMemoryMB != 0 && RES_VMEM.doubleValue() > jobMaxMemoryMB)
		 * error = "Memory usage limit is " + jobMaxMemoryMB + ", using " + RES_VMEM;
		 * 
		 * // cpu
		 * final long time = System.currentTimeMillis();
		 * 
		 * if (prevTime != 0 && prevTime + (20 * 60 * 1000) < time && RES_CPUTIME == prevCpuTime)
		 * error = "The job hasn't used the CPU for 20 minutes";
		 * else {
		 * prevCpuTime = RES_CPUTIME;
		 * prevTime = time;
		 * }
		 * 
		 * } catch (final IOException e) {
		 * System.out.println("Problem with the monitoring objects: " + e.toString());
		 * }
		 */

		return error;
	}

	private void getMemoryRequirements() {
		// Sandbox size
		final String workdirMaxSize = jdl.gets("Workdirectorysize");

		if (workdirMaxSize != null) {
			final Pattern p = Pattern.compile("\\p{L}");
			final Matcher m = p.matcher(workdirMaxSize);
			if (m.find()) {
				final String number = workdirMaxSize.substring(0, m.start());
				final String unit = workdirMaxSize.substring(m.start());

				switch (unit) {
					case "KB":
						workdirMaxSizeMB = Integer.parseInt(number) / 1024;
						break;
					case "GB":
						workdirMaxSizeMB = Integer.parseInt(number) * 1024;
						break;
					default: // MB
						workdirMaxSizeMB = Integer.parseInt(number);
				}
			}
			else
				workdirMaxSizeMB = Integer.parseInt(workdirMaxSize);
			commander.q_api.putJobLog(queueId, "trace", "Disk requested: " + workdirMaxSizeMB);
		}
		else
			workdirMaxSizeMB = 0;

		// Memory use
		final String maxmemory = jdl.gets("Memorysize");

		if (maxmemory != null) {
			final Pattern p = Pattern.compile("\\p{L}");
			final Matcher m = p.matcher(maxmemory);
			if (m.find()) {
				final String number = maxmemory.substring(0, m.start());
				final String unit = maxmemory.substring(m.start());

				switch (unit) {
					case "KB":
						jobMaxMemoryMB = Integer.parseInt(number) / 1024;
						break;
					case "GB":
						jobMaxMemoryMB = Integer.parseInt(number) * 1024;
						break;
					default: // MB
						jobMaxMemoryMB = Integer.parseInt(number);
				}
			}
			else
				jobMaxMemoryMB = Integer.parseInt(maxmemory);
			commander.q_api.putJobLog(queueId, "trace", "Memory requested: " + jobMaxMemoryMB);
		}
		else
			jobMaxMemoryMB = 0;

	}

	private int execute() {
		commander.q_api.putJobLog(queueId, "trace", "Starting execution");

		// final int code = executeCommand(jdl.gets("Executable"), jdl.getArguments(), ttlForJob(), TimeUnit.SECONDS, true);
		// final int code = executeCommand(jdl.gets("Executable"), jdl.getArguments(), ttlForJob(), TimeUnit.SECONDS, false);

		// EXPERIMENTAL
		// for ORNL Titan
		try (Connection connection = DriverManager.getConnection(dbname); Statement statement = connection.createStatement();) {

			// statement.executeUpdate(String.format("INSERT INTO alien_jobs(rank, queue_id, job_folder , status , executable, validation, environment ) " +
			// "VALUES(%d, %d, '%s', '%s', '%s', '%s', '%s')",
			// current_rank, queueId, tempDir, "Q",
			// jdl.gets("Executable"),
			// jdl.gets("ValidationCommand"),
			// "" ));
			// setting variables
			final HashMap<String, String> alice_environment_packages = loadJDLEnvironmentVariables();

			// setting variables for packages
			final HashMap<String, String> environment_packages = getJobPackagesEnvironment();

			try (PrintWriter out = new PrintWriter(tempDir + "/environment")) {
				for (Entry<String, String> e : alice_environment_packages.entrySet()) {
					out.println(String.format("export %s=%s", e.getKey(), e.getValue()));
				}

				for (Entry<String, String> e : environment_packages.entrySet()) {
					out.println(String.format(" export %s=%s", e.getKey(), e.getValue()));
				}
			}

			String validationCommand = jdl.gets("ValidationCommand");
			statement.executeUpdate(String.format("UPDATE alien_jobs SET queue_id=%d, job_folder='%s', status='%s', executable='%s', validation='%s', environment='%s' " + "WHERE rank=%d",
					Long.valueOf(queueId), tempDir, "Q", getLocalCommand(jdl.gets("Executable"), jdl.getArguments()), validationCommand != null ? getLocalCommand(validationCommand, null) : "", "",
					Integer.valueOf(current_rank)));
		}
		catch (SQLException e) {
			System.err.println("Failed to insert job: " + e.getMessage());
		}
		catch (@SuppressWarnings("unused") FileNotFoundException e) {
			System.err.println("Failed to write variables file");
		}

		// System.err.println("Execution code: " + code);

		// return code;
		return 0;
	}

	/*
	 * private boolean validate() {
	 * int code = 0;
	 * 
	 * final String validation = jdl.gets("ValidationCommand");
	 * 
	 * if (validation != null) {
	 * commander.q_api.putJobLog(queueId, "trace", "Starting validation");
	 * code = executeCommand(validation, null, 5, TimeUnit.MINUTES, false);
	 * }
	 * System.err.println("Validation code: " + code);
	 * 
	 * return code == 0;
	 * }
	 */

	private boolean getInputFiles() {
		final Set<String> filesToDownload = new HashSet<>();

		List<String> list = jdl.getInputFiles(false);

		if (list != null)
			filesToDownload.addAll(list);

		list = jdl.getInputData(false);

		if (list != null)
			filesToDownload.addAll(list);

		String s = jdl.getExecutable();

		if (s != null)
			filesToDownload.add(s);

		s = jdl.gets("ValidationCommand");

		if (s != null)
			filesToDownload.add(s);

		final List<LFN> iFiles = c_api.getLFNs(filesToDownload, true, false);

		if (iFiles == null || iFiles.size() != filesToDownload.size()) {
			System.out.println("Not all requested files could be located");
			return false;
		}

		final Map<LFN, File> localFiles = new HashMap<>();

		for (final LFN l : iFiles) {
			File localFile = new File(tempDir, l.getFileName());

			final int i = 0;

			while (localFile.exists() && i < 100000)
				localFile = new File(tempDir, l.getFileName() + "." + i);

			if (localFile.exists()) {
				System.out.println("Too many occurences of " + l.getFileName() + " in " + tempDir.getAbsolutePath());
				return false;
			}

			localFiles.put(l, localFile);
		}

		for (final Map.Entry<LFN, File> entry : localFiles.entrySet()) {
			final List<PFN> pfns = c_api.getPFNsToRead(entry.getKey(), null, null);

			if (pfns == null || pfns.size() == 0) {
				System.out.println("No replicas of " + entry.getKey().getCanonicalName() + " to read from");
				return false;
			}

			final GUID g = pfns.iterator().next().getGuid();

			commander.q_api.putJobLog(queueId, "trace", "Getting InputFile: " + entry.getKey().getCanonicalName());

			final StringBuilder errorMessage = new StringBuilder();

			final File f = IOUtils.get(g, entry.getValue(), errorMessage);

			if (f == null) {
				System.out.println("Could not download " + entry.getKey().getCanonicalName() + " to " + entry.getValue().getAbsolutePath() + " due to:\n" + errorMessage);
				return false;
			}
		}

		dumpInputDataList();

		System.out.println("Sandbox prepared : " + tempDir.getAbsolutePath());

		return true;
	}

	private void dumpInputDataList() {
		// creates xml file with the InputData
		try {
			final String list = jdl.gets("InputDataList");

			if (list == null)
				return;

			System.out.println("Going to create XML: " + list);

			final String format = jdl.gets("InputDataListFormat");
			if (format == null || !format.equals("xml-single")) {
				System.out.println("XML format not understood");
				return;
			}

			final XmlCollection c = new XmlCollection();
			c.setName("jobinputdata");
			final List<String> datalist = jdl.getInputData(true);

			for (final String s : datalist) {
				final LFN l = c_api.getLFN(s);
				if (l == null)
					continue;
				c.add(l);
			}

			final String content = c.toString();

			Files.write(Paths.get(jobWorkdir + "/" + list), content.getBytes());

		}
		catch (final Exception e) {
			System.out.println("Problem dumping XML: " + e.toString());
		}

	}

	private HashMap<String, String> getJobPackagesEnvironment() {
		final String voalice = "VO_ALICE@";
		String packagestring = "";
		final HashMap<String, String> packs = (HashMap<String, String>) jdl.getPackages();
		HashMap<String, String> envmap = new HashMap<>();

		if (packs != null) {
			for (final Map.Entry<String, String> entry : packs.entrySet())
				packagestring += voalice + entry.getKey() + "::" + packs.get(entry.getValue()) + ",";

			if (!packs.containsKey("APISCONFIG"))
				packagestring += voalice + "APISCONFIG,";

			packagestring = packagestring.substring(0, packagestring.length() - 1);

			final ArrayList<String> packagesList = new ArrayList<>();
			packagesList.add(packagestring);

			logger.log(Level.INFO, packagestring);

			envmap = (HashMap<String, String>) installPackages(packagesList);
		}

		logger.log(Level.INFO, envmap.toString());
		return envmap;
	}

	private boolean uploadOutputFiles() {
		boolean uploadedAllOutFiles = true;
		boolean uploadedNotAllCopies = false;

		commander.q_api.putJobLog(queueId, "trace", "Going to uploadOutputFiles");

		// EXPERIMENTAL
		final String outputDir = getJobOutputDir();
		// final String outputDir = getJobOutputDir() + "/" + queueId;

		System.out.println("queueId: " + queueId);
		System.out.println("outputDir: " + outputDir);

		if (c_api.getLFN(outputDir) != null) {
			System.err.println("OutputDir [" + outputDir + "] already exists.");
			changeStatus(JobStatus.ERROR_SV);
			return false;
		}

		final LFN outDir = c_api.createCatalogueDirectory(outputDir, true);

		if (outDir == null) {
			uploadedAllOutFiles = false;
			logger.log(Level.SEVERE, "Error creating the OutputDir [" + outputDir + "].");
			commander.q_api.putJobLog(queueId, "trace", "Can't create the output directory " + outputDir);
			changeStatus(JobStatus.ERROR_SV);
			return false;
		}

		String tag = "Output";
		if (jobStatus == JobStatus.ERROR_E)
			tag = "OutputErrorE";

		final ParsedOutput filesTable = new ParsedOutput(queueId, jdl, jobWorkdir, tag);

		for (final OutputEntry entry : filesTable.getEntries()) {
			File localFile;
			try {
				if (entry.isArchive())
					entry.createZip(jobWorkdir);

				localFile = new File(jobWorkdir + "/" + entry.getName());
				System.out.println("Processing output file: " + localFile);

				// EXPERIMENTAL
				System.err.println("===================");
				System.err.println("Filename: " + localFile.getName());
				System.err.println("File exists: " + localFile.exists());
				System.err.println("File is file: " + localFile.isFile());
				System.err.println("File readable: " + localFile.canRead());
				System.err.println("File length: " + localFile.length());

				if (localFile.exists() && localFile.isFile() && localFile.canRead() && localFile.length() > 0) {

					final long size = localFile.length();
					if (size <= 0)
						System.err.println("Local file has size zero: " + localFile.getAbsolutePath());
					String md5 = null;
					try {
						md5 = IOUtils.getMD5(localFile);
					}
					catch (@SuppressWarnings("unused") final Exception e1) {
						// ignore
					}
					if (md5 == null)
						System.err.println("Could not calculate md5 checksum of the local file: " + localFile.getAbsolutePath());

					final LFN lfn = c_api.getLFN(outDir.getCanonicalName() + "/" + entry.getName(), true);
					lfn.size = size;
					lfn.md5 = md5;
					lfn.jobid = queueId;
					lfn.type = 'f';
					final GUID guid = GUIDUtils.createGuid(localFile, commander.getUser());
					lfn.guid = guid.guid;
					final ArrayList<String> exses = entry.getSEsDeprioritized();

					final List<PFN> pfns = c_api.getPFNsToWrite(lfn, guid, entry.getSEsPrioritized(), exses, entry.getQoS());

					System.out.println("LFN :" + lfn + "\npfns: " + pfns);

					commander.q_api.putJobLog(queueId, "trace", "Uploading: " + lfn.getName());

					if (pfns != null && !pfns.isEmpty()) {
						final ArrayList<String> envelopes = new ArrayList<>(pfns.size());
						for (final PFN pfn : pfns) {
							final List<Protocol> protocols = Transfer.getAccessProtocols(pfn);
							for (final Protocol protocol : protocols) {
								envelopes.add(protocol.put(pfn, localFile));
								break;
							}
						}

						// drop the following three lines once put replies
						// correctly
						// with the signed envelope
						envelopes.clear();
						for (final PFN pfn : pfns)
							envelopes.add(pfn.ticket.envelope.getSignedEnvelope());

						final List<PFN> pfnsok = c_api.registerEnvelopes(envelopes, BOOKING_STATE.COMMITED);
						if (!pfns.equals(pfnsok))
							if (pfnsok != null && pfnsok.size() > 0) {
								System.out.println("Only " + pfnsok.size() + " could be uploaded");
								uploadedNotAllCopies = true;
							}
							else {
								System.err.println("Upload failed, sorry!");
								uploadedAllOutFiles = false;
								break;
							}
					}
					else
						System.out.println("Couldn't get write envelopes for output file");
				}
				else
					System.out.println("Can't upload output file " + localFile.getName() + ", does not exist or has zero size.");

			}
			catch (final IOException e) {
				e.printStackTrace();
				uploadedAllOutFiles = false;
			}
		}

		if (jobStatus != JobStatus.ERROR_E && jobStatus != JobStatus.ERROR_V)
			if (uploadedNotAllCopies)
				changeStatus(JobStatus.DONE_WARN);
			else if (uploadedAllOutFiles)
				changeStatus(JobStatus.DONE);
			else
				changeStatus(JobStatus.ERROR_SV);

		return uploadedAllOutFiles;
	}

	private boolean createWorkDir() {
		logger.log(Level.INFO, "Creating sandbox and chdir");

		jobWorkdir = String.format("%s%s%d", workdir, defaultOutputDirPrefix, Long.valueOf(queueId));

		tempDir = new File(jobWorkdir);
		if (!tempDir.exists()) {
			final boolean created = tempDir.mkdirs();
			if (!created) {
				logger.log(Level.INFO, "Workdir does not exist and can't be created: " + jobWorkdir);
				return false;
			}
		}

		// chdir
		System.setProperty("user.dir", jobWorkdir);

		commander.q_api.putJobLog(queueId, "trace", "Created workdir: " + jobWorkdir);
		// TODO: create the extra directories

		return true;
	}

	private HashMap<String, String> loadJDLEnvironmentVariables() {
		final HashMap<String, String> hashret = new HashMap<>();

		try {
			final HashMap<String, Object> vars = (HashMap<String, Object>) jdl.getJDLVariables();

			if (vars != null)
				for (final String s : vars.keySet()) {
					String value = "";
					final Object val = jdl.get(s);

					if (val instanceof Collection<?>) {
						@SuppressWarnings("unchecked")
						final Iterator<String> it = ((Collection<String>) val).iterator();
						String sbuff = "";
						boolean isFirst = true;

						while (it.hasNext()) {
							if (!isFirst)
								sbuff += "##";
							final String v = it.next().toString();
							sbuff += v;
							isFirst = false;
						}
						value = sbuff;
					}
					else
						value = val.toString();

					hashret.put("ALIEN_JDL_" + s.toUpperCase(), value);
				}
		}
		catch (final Exception e) {
			System.out.println("There was a problem getting JDLVariables: " + e);
		}

		return hashret;
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	@SuppressWarnings("unused")
	public static void main(final String[] args) throws IOException {
		final JobAgentProxy ja = new JobAgentProxy();
		// ja.run();

	}

	/**
	 * @param newStatus
	 */
	public void changeStatus(final JobStatus newStatus) {
		// if final status with saved files, we set the path
		if (newStatus == JobStatus.DONE || newStatus == JobStatus.DONE_WARN || newStatus == JobStatus.ERROR_E || newStatus == JobStatus.ERROR_V) {
			final HashMap<String, Object> extrafields = new HashMap<>();
			extrafields.put("path", getJobOutputDir());

			TaskQueueApiUtils.setJobStatus(queueId, newStatus, extrafields);
		}
		else if (newStatus == JobStatus.RUNNING) {
			final HashMap<String, Object> extrafields = new HashMap<>();
			extrafields.put("spyurl", hostName + ":" + JBoxServer.getPort());
			extrafields.put("node", hostName);

			TaskQueueApiUtils.setJobStatus(queueId, newStatus, extrafields);
		}
		else
			TaskQueueApiUtils.setJobStatus(queueId, newStatus);

		jobStatus = newStatus;

		return;
	}

	/**
	 * @return job output dir (as indicated in the JDL if OK, or the recycle path if not)
	 */
	public String getJobOutputDir() {
		String outputDir = jdl.getOutputDir();

		if (jobStatus == JobStatus.ERROR_V || jobStatus == JobStatus.ERROR_E)
			outputDir = FileSystemUtils.getAbsolutePath(username, null, "~" + "recycle/" + defaultOutputDirPrefix + queueId);
		else if (outputDir == null)
			outputDir = FileSystemUtils.getAbsolutePath(username, null, "~" + defaultOutputDirPrefix + queueId);

		return outputDir;
	}

	@Override
	public void fillValues(final Vector<String> paramNames, final Vector<Object> paramValues) {
		if (queueId > 0) {
			paramNames.add("jobID");
			paramValues.add(Double.valueOf(queueId));

			paramNames.add("statusID");
			paramValues.add(Double.valueOf(jobStatus.getAliEnLevel()));
		}
	}

}
