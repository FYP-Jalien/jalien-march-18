package alien.site;

import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import alien.api.JBoxServer;
import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.MonitoringObject;
import alien.shell.commands.JAliEnCOMMander;
import alien.site.packman.CVMFS;
import alien.site.packman.PackMan;
import alien.site.supercomputing.titan.FileDownloadController;
import alien.site.supercomputing.titan.JobDownloader;
import alien.site.supercomputing.titan.JobUploader;
import alien.site.supercomputing.titan.ProcInfoPair;
import alien.site.supercomputing.titan.TitanBatchController;
import alien.site.supercomputing.titan.TitanJobStatus;
import apmon.ApMon;
import apmon.ApMonException;
import apmon.ApMonMonitoringConstants;
import apmon.BkThread;

/**
 * @author mmmartin, ron, pavlo
 * @since Apr 1, 2015
 */

public class TitanJobService implements MonitoringObject, Runnable {

	// Folders and files
	private static final String defaultOutputDirPrefix = "/jalien-job-";

	// Variables passed through VoBox environment
	private final Map<String, String> env = System.getenv();
	private final String site;
	private final String ce;
	private int origTtl;

	// Job variables
	// private JDL jdl = null;
	// private long queueId;
	private String jobAgentId = "";
	private String globalWorkdir = null;
	// private HashMap<String, Object> matchedJob = null;
	private String partition;
	private String ceRequirements = "";
	private List<String> packages;
	private List<String> installedPackages;
	private ArrayList<String> extrasites;
	private HashMap<String, Object> siteMap = new HashMap<>();
	// private int workdirMaxSizeMB;
	// private int jobMaxMemoryMB;
	// private JobStatus jobStatus;

	private final int totalJobs;
	private final long jobAgentStartTime = new java.util.Date().getTime();

	// Other
	private PackMan packMan = null;
	private String hostName = null;
	private String alienCm = null;

	private final JAliEnCOMMander commander = JAliEnCOMMander.getInstance();
	private static final HashMap<String, Integer> jaStatus = new HashMap<>();

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

	private static final Logger logger = ConfigUtils.getLogger(TitanJobService.class.getCanonicalName());

	private static final Monitor monitor = MonitorFactory.getMonitor(TitanJobService.class.getCanonicalName());
	private static final ApMon apmon = MonitorFactory.getApMonSender();

	private Integer RES_NOCPUS = Integer.valueOf(1);
	private String RES_CPUMHZ = "";
	private String RES_CPUFAMILY = "";

	// EXPERIMENTAL
	// for ORNL Titan

	private TitanBatchController batchController;

	/**
	 */
	public TitanJobService() {
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

		globalWorkdir = env.get("HOME");
		if (env.containsKey("WORKDIR"))
			globalWorkdir = env.get("WORKDIR");
		if (env.containsKey("TMPBATCH"))
			globalWorkdir = env.get("TMPBATCH");

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

		TitanBatchController.siteMap = siteMap;
		batchController = new TitanBatchController(globalWorkdir);
		FileDownloadController.setCacheFolder("/lustre/atlas/proj-shared/csc108/psvirin/catalog_cache");

		JobUploader.ce = ce;
		JobUploader.hostName = hostName;
		JobUploader.defaultOutputDirPrefix = defaultOutputDirPrefix;

		JobDownloader.ce = ce;
		JobDownloader.packMan = getPackman();
		JobDownloader.hostName = hostName;
		JobDownloader.defaultOutputDirPrefix = defaultOutputDirPrefix;

		// here create monitor thread
		class TitanMonitorThread extends Thread {
			private final TitanBatchController tbc;

			public TitanMonitorThread(final TitanBatchController tbc) {
				this.tbc = tbc;
			}

			private void sendProcessResources() {
				// List<ProcInfoPair> job_resources = new LinkedList<ProcInfoPair>();
				final List<ProcInfoPair> job_resources = tbc.getBatchesMonitoringData();

				/*
				 * try{
				 * // open db
				 * Connection connection = DriverManager.getConnection(monitoring_dbname);
				 * Statement statement = connection.createStatement();
				 * ResultSet rs = statement.executeQuery("SELECT * FROM alien_jobs_monitoring");
				 * // read all
				 * while(rs.next()){
				 * job_resources.add(new ProcInfoPair( rs.getString("queue_id"), rs.getString("resources")));
				 * //idleRanks.add(new TitanJobStatus(rs.getInt("rank"), rs.getLong("queue_id"), rs.getString("job_folder"),
				 * // rs.getString("status"), rs.getInt("exec_code"), rs.getInt("val_code")));
				 * }
				 * // delete all
				 * statement.executeUpdate("DELETE FROM alien_jobs_monitoring");
				 * // close database
				 * connection.close();
				 * }
				 * catch(SQLException e){
				 * System.err.println("Unable to get monitoring data: " + e.getMessage());
				 * }
				 */
				// foreach send

				// runtime(date formatted) start cpu(%) mem cputime rsz vsize ncpu cpufamily cpuspeed resourcecost maxrss maxvss ksi2k
				// final String procinfo = String.format("%s %d %.2f %.2f %.2f %.2f %.2f %d %s %s %s %.2f %.2f 1000", RES_FRUNTIME, RES_RUNTIME, RES_CPUUSAGE, RES_MEMUSAGE, RES_CPUTIME, RES_RMEM,
				// RES_VMEM,
				// RES_NOCPUS, RES_CPUFAMILY, RES_CPUMHZ, RES_RESOURCEUSAGE, RES_RMEMMAX, RES_VMEMMAX);
				// System.out.println("+++++ Sending resources info +++++");
				// System.out.println(procinfo);

				// create pool of 16 thread
				for (final ProcInfoPair pi : job_resources)
					// notify to all processes waiting
					commander.q_api.putJobLog(pi.queue_id, pi.resubmission, "proc", pi.procinfo);

				// ApMon calls
				System.out.println("Running periodic Apmon update on running jobs");
				final List<TitanJobStatus> runningJobs = tbc.queryRunningDatabases();
				for (final TitanJobStatus pi : runningJobs) {
					/*
					 * final HashMap<String, Object> extrafields = new HashMap<>();
					 * extrafields.put("spyurl", hostName + ":" + JBoxServer.getPort());
					 * extrafields.put("node", hostName);
					 * TaskQueueApiUtils.setJobStatus(pi.queueId, JobStatus.RUNNING, extrafields);
					 */

					System.out.println("Running ApMon update for PID: " + pi.queueId);
					final Vector<String> varnames = new Vector<>();
					varnames.add("host");
					varnames.add("statusID");
					varnames.add("jobID");
					varnames.add("job_user");
					varnames.add("masterjob_id");
					varnames.add("host_pid");
					varnames.add("exechost");
					final Vector<Object> varvalues = new Vector<>();
					varvalues.add(hostName);
					varvalues.add(Double.valueOf(10));
					varvalues.add(Double.valueOf(pi.queueId.longValue()));
					varvalues.add("psvirin");
					varvalues.add(Double.valueOf(0));
					varvalues.add(Double.valueOf(10000));
					// varvalues.add(ce);
					varvalues.add(hostName);
					try {
						// apmon.sendParameters(ce+"_Jobs", String.format("%d",pi.queueId), 6, varnames, varvalues);
						apmon.sendParameters("TaskQueue_Jobs_ALICE", String.format("%d", pi.queueId), 6, varnames, varvalues);
					}
					catch (final ApMonException e) {
						System.out.println("Apmon exception: " + e.getMessage());
					}
					catch (final UnknownHostException e) {
						System.out.println("Unknown host exception: " + e.getMessage());
					}
					catch (final SocketException e) {
						System.out.println("Socket exception: " + e.getMessage());
					}

					catch (final IOException e) {
						System.out.println("IO exception: " + e.getMessage());
					}

					// notify to all processes waiting
					// commander.q_api.putJobLog(pi.queue_id, "proc", pi.procinfo);
				}
			}

			@Override
			public void run() {
				// here create a pool of 16 sending processes

				while (true) {
					try {
						Thread.sleep(1 * 60 * 1000);
					}
					catch (@SuppressWarnings("unused") final InterruptedException e) {
						// ignore
					}
					sendProcessResources();
				}
			}
		}

		new TitanMonitorThread(batchController).start();
		// END EXPERIMENTAL
	}

	@Override
	public void run() {

		logger.log(Level.INFO, "Starting JobAgent in " + hostName);

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

		while (true) {
			System.out.println("========================");
			System.out.println("Entering round");
			System.out.println("Updating bunches information");
			if (!batchController.updateDatabaseList()) {
				System.out.println("No batches, sleeping.");
				roundSleep();
				continue;
			}

			if (batchController.queryDatabases()) {
				System.out.println("Now running jobs exchange");
				monitor.sendParameter("ja_status", getJaStatusForML("REQUESTING_JOB"));
				monitor.sendParameter("TTL", siteMap.get("TTL"));
				batchController.runDataExchange();
			}

			if (!updateDynamicParameters()) {
				System.err.println("update for dynamic parameters failed. Stopping the agent.");
				break;
			}

			System.out.println("=========== Round finished =========");
			roundSleep();

		}

		logger.log(Level.INFO, "JobAgent finished, id: " + jobAgentId + " totalJobs: " + totalJobs);
		System.exit(0);
	}

	// =========================================================================================================
	// ================ run finished

	private static void roundSleep() {
		try {
			Thread.sleep(60000);
		}
		catch (final InterruptedException e) {
			System.err.println("Sleep after full JA cycle failed: " + e.getMessage());
		}
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
		siteMap.put("Disk", Long.valueOf(JobAgent.getFreeSpace(globalWorkdir) / 1024));

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
		final long space = JobAgent.getFreeSpace(globalWorkdir) / 1024;

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

		// EXPERIMENTAL
		// for ORNL Titan
		/*
		 * if (timeleft <= 0) {
		 * logger.log(Level.INFO, "There is not enough time left: " + timeleft);
		 * return false;
		 * }
		 */

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

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(final String[] args) throws IOException {
		ApMon.setLogLevel("DEBUG");
		final TitanJobService ja = new TitanJobService();
		ja.run();
	}

	@Override
	public void fillValues(final Vector<String> paramNames, final Vector<Object> paramValues) {
		final Long queueId = Long.valueOf(0L);
		if (queueId.longValue() > 0) {
			paramNames.add("jobID");
			paramValues.add(Double.valueOf(queueId.longValue()));

			// EXPERIMENTAL
			// temporarily commented out
			// paramNames.add("statusID");
			// paramValues.add(Double.valueOf(jobStatus.getAliEnLevel()));
		}
	}
}
