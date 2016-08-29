package alien.site;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
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
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import alien.api.JBoxServer;
import alien.api.catalogue.CatalogueApiUtils;
import alien.api.taskQueue.GetMatchJob;
import alien.api.taskQueue.TaskQueueApiUtils;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.GUID;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.catalogue.Register;
import alien.catalogue.XmlCollection;
import alien.config.ConfigUtils;
import alien.io.IOUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.MonitoringObject;
import alien.shell.commands.JAliEnCOMMander;
import alien.site.packman.CVMFS;
import alien.site.packman.PackMan;
import alien.taskQueue.JDL;
import alien.taskQueue.Job;
import alien.taskQueue.JobStatus;
import alien.user.UserFactory;
import apmon.ApMon;
import apmon.ApMonException;
import apmon.ApMonMonitoringConstants;
import apmon.BkThread;
import apmon.MonitoredJob;
import lia.util.Utils;

/**
 * @author mmmartin, ron
 * @since Apr 1, 2015
 */
public class JobAgent extends Thread implements MonitoringObject {

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
	private int payloadPID;
	private MonitoredJob mj;
	private Double prevCpuTime;
	private long prevTime = 0;
	private JobStatus jobStatus;

	private int totalJobs;
	private final long jobAgentStartTime = new java.util.Date().getTime();

	// Other
	private PackMan packMan = null;
	private String hostName = null;
	private String alienCm = null;
	private final int pid;
	private final JAliEnCOMMander commander = JAliEnCOMMander.getInstance();
	private final CatalogueApiUtils c_api = new CatalogueApiUtils(commander);
	private static final HashMap<String, Integer> jaStatus = new HashMap<>();

	static {
		jaStatus.put("REQUESTING_JOB", Integer.valueOf(1));
		jaStatus.put("INSTALLING_PKGS", Integer.valueOf(2));
		jaStatus.put("JOB_STARTED", Integer.valueOf(3));
		jaStatus.put("RUNNING_JOB", Integer.valueOf(4));
		jaStatus.put("DONE", Integer.valueOf(5));
		jaStatus.put("ERROR_HC", Integer.valueOf(-1)); // error in getting host
														// classad
		jaStatus.put("ERROR_IP", Integer.valueOf(-2)); // error installing
														// packages
		jaStatus.put("ERROR_GET_JDL", Integer.valueOf(-3)); // error getting jdl
		jaStatus.put("ERROR_JDL", Integer.valueOf(-4)); // incorrect jdl
		jaStatus.put("ERROR_DIRS", Integer.valueOf(-5)); // error creating
															// directories, not
															// enough free space
															// in workdir
		jaStatus.put("ERROR_START", Integer.valueOf(-6)); // error forking to
															// start job
	}

	private final int jobagent_requests = 1; // TODO: restore to 5

	/**
	 * logger object
	 */
	static transient final Logger logger = ConfigUtils.getLogger(JobAgent.class.getCanonicalName());

	/**
	 * ML monitor object
	 */
	static transient final Monitor monitor = MonitorFactory.getMonitor(JobAgent.class.getCanonicalName());
	/**
	 * ApMon sender
	 */
	static transient final ApMon apmon = MonitorFactory.getApMonSender();

	// Resource monitoring vars

	private static final Double ZERO = Double.valueOf(0);

	private Double RES_WORKDIR_SIZE = ZERO;
	private Double RES_VMEM = ZERO;
	private Double RES_RMEM = ZERO;
	private Double RES_VMEMMAX = ZERO;
	private Double RES_RMEMMAX = ZERO;
	private Double RES_MEMUSAGE = ZERO;
	private Double RES_CPUTIME = ZERO;
	private Double RES_CPUUSAGE = ZERO;
	private String RES_RESOURCEUSAGE = "";
	private Long RES_RUNTIME = Long.valueOf(0);
	private String RES_FRUNTIME = "";
	private Integer RES_NOCPUS = Integer.valueOf(1);
	private String RES_CPUMHZ = "";
	private String RES_CPUFAMILY = "";

	/**
	 */
	public JobAgent() {
		site = env.get("site"); // or
								// ConfigUtils.getConfig().gets("alice_close_site").trim();
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

		try {
			hostName = InetAddress.getLocalHost().getCanonicalHostName();
		} catch (final UnknownHostException e) {
			System.err.println("Couldn't get hostname");
			e.printStackTrace();
		}

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
		} catch (final IOException e) {
			System.out.println("Problem with the monitoring objects IO Exception: " + e.toString());
		} catch (final ApMonException e) {
			System.out.println("Problem with the monitoring objects ApMon Exception: " + e.toString());
		}

		monitor.addMonitoring("jobAgent-TODO", this);
	}

	@Override
	public void run() {

		logger.log(Level.INFO, "Starting JobAgent in " + hostName);

		// We start, if needed, the node JBox
		// Does it check a previous one is already running?
		try {
			System.out.println("Trying to start JBox");
			JBoxServer.startJBoxService(0);
		} catch (final Exception e) {
			System.err.println("Unable to start JBox.");
			e.printStackTrace();
		}

		int count = jobagent_requests;
		while (count > 0) {
			if (!updateDynamicParameters())
				break;

			System.out.println(siteMap.toString());

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
					queueId = ((Long) matchedJob.get("queueId")).longValue();
					username = (String) matchedJob.get("User");
					jobToken = (String) matchedJob.get("jobToken");

					// TODO: commander.setUser(username);
					// commander.setSite(site);

					System.out.println(jdl.getExecutable());
					System.out.println(username);
					System.out.println(queueId);
					System.out.println(jobToken);

					// process payload
					handleJob();

					cleanup();
				}
				else {
					if (matchedJob != null && matchedJob.containsKey("Error")) {
						logger.log(Level.INFO, (String) matchedJob.get("Error"));

						if (Integer.valueOf(3).equals(matchedJob.get("Code"))) {
							final ArrayList<String> packToInstall = (ArrayList<String>) matchedJob.get("Packages");
							monitor.sendParameter("ja_status", getJaStatusForML("INSTALLING_PKGS"));
							installPackages(packToInstall);
						}
					}
					else
						logger.log(Level.INFO, "We didn't get anything back. Nothing to run right now. Idling 20secs zZz...");

					try {
						// TODO?: monitor.sendBgMonitoring
						sleep(20000);
					} catch (final InterruptedException e) {
						e.printStackTrace();
					}
				}
			} catch (final Exception e) {
				logger.log(Level.INFO, "Error getting a matching job: " + e);
			}
			count--;
		}

		logger.log(Level.INFO, "JobAgent finished, id: " + jobAgentId + " totalJobs: " + totalJobs);
		System.exit(0);
	}

	private void cleanup() {
		System.out.println("Cleaning up after execution...Removing sandbox: " + jobWorkdir);
		// Remove sandbox, TODO: use Java builtin
		Utils.getOutput("rm -rf " + jobWorkdir);
		RES_WORKDIR_SIZE = ZERO;
		RES_VMEM = ZERO;
		RES_RMEM = ZERO;
		RES_VMEMMAX = ZERO;
		RES_RMEMMAX = ZERO;
		RES_MEMUSAGE = ZERO;
		RES_CPUTIME = ZERO;
		RES_CPUUSAGE = ZERO;
		RES_RESOURCEUSAGE = "";
		RES_RUNTIME = Long.valueOf(0);
		RES_FRUNTIME = "";
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
		
		// get nousers from cerequirements field
		final ArrayList<String> nousers = new ArrayList<>();
		if (!ceRequirements.equals("")) {
			final Pattern p = Pattern.compile("\\s*other.user\\s*!=\\s*\"(\\w+)\"");
			final Matcher m = p.matcher(ceRequirements);
			while (m.find())
				nousers.add(m.group(1));
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
		if (nousers.size() > 0)
			siteMap.put("NoUsers", nousers);
		if (extrasites != null && extrasites.size() > 0)
			siteMap.put("Extrasites", extrasites);
		siteMap.put("Host", alienCm);
		siteMap.put("Disk", Long.valueOf(new File(workdir).getFreeSpace() / 1024));

		if (!partition.equals(""))
			siteMap.put("Partition", partition);

		return siteMap;
	}

	private PackMan getPackman() {
		switch (env.get("installationMethod")) {
		case "CVMFS":
			siteMap.put("CVMFS", Integer.valueOf(1));
			return new CVMFS();
		default:
			siteMap.put("CVMFS", Integer.valueOf(1));
			return new CVMFS();
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
		final long space = new File(workdir).getFreeSpace() / 1024;

		// ttl recalculation
		final long jobAgentCurrentTime = new java.util.Date().getTime();
		final int time_subs = (int) (jobAgentCurrentTime - jobAgentStartTime);
		int timeleft = origTtl - time_subs - 300;

		logger.log(Level.INFO, "Still have " + timeleft + " seconds to live (" + jobAgentCurrentTime + "-" + jobAgentStartTime + "=" + time_subs + ")");

		// we check if the proxy timeleft is smaller than the ttl itself
		final int proxy = getRemainingProxyTime();
		logger.log(Level.INFO, "Proxy timeleft is " + proxy);
		if (proxy > 0 && proxy < timeleft)
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
		siteMap.put("TTL", Integer.valueOf(timeleft));

		return true;
	}

	/**
	 * @return the time in seconds that proxy is still valid for
	 */
	private int getRemainingProxyTime() {
		// TODO: to be modified!
		return origTtl;
	}

	private long ttlForJob() {
		final Integer iTTL = jdl.getInteger("TTL");

		int ttl = (iTTL != null ? iTTL.intValue() : 3600);
		commander.q_api.putJobLog(queueId, "trace", "Job asks to run for " + ttl + " seconds");
		ttl += 300; // extra time (saving)

		final String proxyttl = jdl.gets("ProxyTTL");
		if (proxyttl != null) {
			ttl = ((Integer) siteMap.get("TTL")).intValue() - 600;
			commander.q_api.putJobLog(queueId, "trace", "ProxyTTL enabled, running for " + ttl + " seconds");
		}

		return ttl;
	}

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

			// run payload
			if (execute() < 0)
				changeStatus(JobStatus.ERROR_E);

			if (!validate())
				changeStatus(JobStatus.ERROR_V);

			if (jobStatus == JobStatus.RUNNING)
				changeStatus(JobStatus.SAVING);

			uploadOutputFiles();

		} catch (final Exception e) {
			System.err.println("Unable to handle job");
			e.printStackTrace();
		}
	}

	/**
	 * @param command
	 * @param arguments
	 * @param timeout
	 * @return <code>0</code> if everything went fine, a positive number with the process exit code (which would mean a problem) and a negative error code in case of timeout or other supervised
	 *         execution errors
	 */
	private int executeCommand(final String command, final List<String> arguments, final long timeout, final TimeUnit unit, final boolean monitorJob) {
		final List<String> cmd = new LinkedList<>();

		final int idx = command.lastIndexOf('/');

		final String cmdStrip = idx < 0 ? command : command.substring(idx + 1);

		final File fExe = new File(tempDir, cmdStrip);

		if (!fExe.exists())
			return -1;

		fExe.setExecutable(true);

		cmd.add(fExe.getAbsolutePath());

		if (arguments != null && arguments.size() > 0)
			for (final String argument : arguments)
				if (argument.trim().length() > 0) {
					final StringTokenizer st = new StringTokenizer(argument);

					while (st.hasMoreTokens())
						cmd.add(st.nextToken());
				}

		System.err.println("Executing: " + cmd + ", arguments is " + arguments + " pid: " + pid);

		final ProcessBuilder pBuilder = new ProcessBuilder(cmd);

		pBuilder.directory(tempDir);

		final HashMap<String, String> environment_packages = getJobPackagesEnvironment();
		final Map<String, String> processEnv = pBuilder.environment();
		processEnv.putAll(environment_packages);
		processEnv.putAll(loadJDLEnvironmentVariables());

		pBuilder.redirectOutput(Redirect.appendTo(new File(tempDir, "stdout")));
		pBuilder.redirectError(Redirect.appendTo(new File(tempDir, "stderr")));
		// pBuilder.redirectErrorStream(true);

		final Process p;

		try {
			changeStatus(JobStatus.RUNNING);

			p = pBuilder.start();
		} catch (final IOException ioe) {
			System.out.println("Exception running " + cmd + " : " + ioe.getMessage());
			return -2;
		}

		final Timer t = new Timer();
		t.schedule(new TimerTask() {
			@Override
			public void run() {
				p.destroy();
			}
		}, TimeUnit.MILLISECONDS.convert(timeout, unit));

		mj = new MonitoredJob(pid, jobWorkdir, ce, hostName);
		final Vector<Integer> child = mj.getChildren();
		if (child == null || child.size() <= 1) {
			System.err.println("Can't get children. Failed to execute? " + cmd.toString() + " child: " + child);
			return -1;
		}
		System.out.println("Child: " + child.get(1).toString());

		boolean processNotFinished = true;
		int code = 0;

		if (monitorJob) {
			payloadPID = child.get(1).intValue();
			apmon.addJobToMonitor(payloadPID, jobWorkdir, ce, hostName); // TODO:
																			// test
			mj = new MonitoredJob(payloadPID, jobWorkdir, ce, hostName);
			final String fs = checkProcessResources();
			if (fs == null)
				sendProcessResources();
		}

		int monitor_loops = 0;
		try {
			while (processNotFinished)
				try {
					Thread.sleep(5 * 1000); // TODO: Change to 60
					code = p.exitValue();
					processNotFinished = false;
				} catch (final IllegalThreadStateException e) {
					logger.log(Level.WARNING, "Exception waiting for the process to finish", e);
					// TODO: check job-token exist (job not killed)

					// process hasn't terminated
					if (monitorJob) {
						monitor_loops++;
						final String error = checkProcessResources();
						if (error != null) {
							p.destroy();
							System.out.println("Process overusing resources: " + error);
							return -2;
						}
						if (monitor_loops == 10) {
							monitor_loops = 0;
							sendProcessResources();
						}
					}
				}
			return code;
		} catch (final InterruptedException ie) {
			System.out.println("Interrupted while waiting for this command to finish: " + cmd.toString() + "\n" + ie.getMessage());
			return -2;
		} finally {
			t.cancel();
		}
	}

	private void sendProcessResources() {
		// runtime(date formatted) start cpu(%) mem cputime rsz vsize ncpu
		// cpufamily cpuspeed resourcecost maxrss maxvss ksi2k
		final String procinfo = String.format("%s %d %.2f %.2f %.2f %.2f %.2f %d %s %s %s %.2f %.2f 1000", RES_FRUNTIME, RES_RUNTIME, RES_CPUUSAGE, RES_MEMUSAGE, RES_CPUTIME, RES_RMEM, RES_VMEM,
				RES_NOCPUS, RES_CPUFAMILY, RES_CPUMHZ, RES_RESOURCEUSAGE, RES_RMEMMAX, RES_VMEMMAX);
		System.out.println("+++++ Sending resources info +++++");
		System.out.println(procinfo);

		commander.q_api.putJobLog(queueId, "proc", procinfo);
	}

	private String checkProcessResources() {
		String error = null;
		System.out.println("Checking resources usage");

		try {
			final HashMap<Long, Double> jobinfo = mj.readJobInfo();
			final HashMap<Long, Double> diskinfo = mj.readJobDiskUsage();

			if (jobinfo == null || diskinfo == null) {
				System.err.println("JobInfo or DiskInfo monitor null");
				return "Not available";
			}

			// getting cpu, memory and runtime info
			RES_WORKDIR_SIZE = diskinfo.get(ApMonMonitoringConstants.LJOB_WORKDIR_SIZE);
			RES_VMEM = Double.valueOf(jobinfo.get(ApMonMonitoringConstants.LJOB_VIRTUALMEM).doubleValue() / 1024);
			RES_RMEM = Double.valueOf(jobinfo.get(ApMonMonitoringConstants.LJOB_RSS).doubleValue() / 1024);
			RES_CPUTIME = jobinfo.get(ApMonMonitoringConstants.LJOB_CPU_TIME);
			RES_CPUUSAGE = Double.valueOf(jobinfo.get(ApMonMonitoringConstants.LJOB_CPU_USAGE).doubleValue());
			RES_RUNTIME = Long.valueOf(jobinfo.get(ApMonMonitoringConstants.LJOB_RUN_TIME).longValue());
			RES_MEMUSAGE = jobinfo.get(ApMonMonitoringConstants.LJOB_MEM_USAGE);
			// RES_RESOURCEUSAGE =
			// Format.showDottedDouble(RES_CPUTIME.doubleValue() *
			// Double.parseDouble(RES_CPUMHZ) / 1000, 2);
			RES_RESOURCEUSAGE = String.format("%.02f", Double.valueOf(RES_CPUTIME.doubleValue() * Double.parseDouble(RES_CPUMHZ) / 1000));

			// max memory consumption
			if (RES_RMEM.doubleValue() > RES_RMEMMAX.doubleValue())
				RES_RMEMMAX = RES_RMEM;

			if (RES_VMEM.doubleValue() > RES_VMEMMAX.doubleValue())
				RES_VMEMMAX = RES_VMEM;

			// formatted runtime
			if (RES_RUNTIME.longValue() < 60)
				RES_FRUNTIME = String.format("00:00:%02d", Long.valueOf(RES_RUNTIME.longValue()));
			else
				if (RES_RUNTIME.longValue() < 3600)
					RES_FRUNTIME = String.format("00:%02d:%02d", Long.valueOf(RES_RUNTIME.longValue() / 60), Long.valueOf(RES_RUNTIME.longValue() % 60));
				else
					RES_FRUNTIME = String.format("%02d:%02d:%02d", Long.valueOf(RES_RUNTIME.longValue() / 3600), Long.valueOf((RES_RUNTIME.longValue() - (RES_RUNTIME.longValue() / 3600) * 3600) / 60),
							Long.valueOf((RES_RUNTIME.longValue() - (RES_RUNTIME.longValue() / 3600) * 3600) % 60));

			// check disk usage
			if (workdirMaxSizeMB != 0 && RES_WORKDIR_SIZE.doubleValue() > workdirMaxSizeMB)
				error = "Disk space limit is " + workdirMaxSizeMB + ", using " + RES_WORKDIR_SIZE;

			// check disk usage
			if (jobMaxMemoryMB != 0 && RES_VMEM.doubleValue() > jobMaxMemoryMB)
				error = "Memory usage limit is " + jobMaxMemoryMB + ", using " + RES_VMEM;

			// cpu
			final long time = System.currentTimeMillis();

			if (prevTime != 0 && prevTime + (20 * 60 * 1000) < time && RES_CPUTIME.equals(prevCpuTime))
				error = "The job hasn't used the CPU for 20 minutes";
			else {
				prevCpuTime = RES_CPUTIME;
				prevTime = time;
			}

		} catch (final IOException e) {
			System.out.println("Problem with the monitoring objects: " + e.toString());
		}

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

		final int code = executeCommand(jdl.gets("Executable"), jdl.getArguments(), ttlForJob(), TimeUnit.SECONDS, true);

		System.err.println("Execution code: " + code);

		return code;
	}

	private boolean validate() {
		int code = 0;

		final String validation = jdl.gets("ValidationCommand");

		if (validation != null) {
			commander.q_api.putJobLog(queueId, "trace", "Starting validation");
			code = executeCommand(validation, null, 5, TimeUnit.MINUTES, false);
		}
		System.err.println("Validation code: " + code);

		return code == 0;
	}

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

			final File f = IOUtils.get(g, entry.getValue());

			if (f == null) {
				System.out.println("Could not download " + entry.getKey().getCanonicalName() + " to " + entry.getValue().getAbsolutePath());
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

		} catch (final Exception e) {
			System.out.println("Problem dumping XML: " + e.toString());
		}

	}

	private HashMap<String, String> getJobPackagesEnvironment() {
		final String voalice = "VO_ALICE@";
		String packagestring = "";
		final HashMap<String, String> packs = (HashMap<String, String>) jdl.getPackages();
		HashMap<String, String> envmap = new HashMap<>();

		if (packs != null) {
			for (final String pack : packs.keySet())
				packagestring += voalice + pack + "::" + packs.get(pack) + ",";

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

		final String outputDir = getJobOutputDir();

		System.out.println("queueId: " + queueId);
		System.out.println("outputDir: " + outputDir);

		if (c_api.getLFN(outputDir) == null) {
			final LFN outDir = c_api.createCatalogueDirectory(outputDir);
			if (outDir == null) {
				System.err.println("Error creating the OutputDir [" + outputDir + "].");
				changeStatus(JobStatus.ERROR_SV);
				return false;
			}
		}

		String tag = "Output";
		if (jobStatus == JobStatus.ERROR_E)
			tag = "OutputErrorE";

		final ParsedOutput filesTable = new ParsedOutput(queueId, jdl, jobWorkdir, tag);

		for (final OutputEntry entry : filesTable.getEntries()) {
			File localFile;
			ArrayList<String> filesIncluded = null;
			try {
				if (entry.isArchive())
					filesIncluded = entry.createZip(jobWorkdir);

				localFile = new File(jobWorkdir + "/" + entry.getName());
				System.out.println("Processing output file: " + localFile);

				if (localFile.exists() && localFile.isFile() && localFile.canRead() && localFile.length() > 0) {
					// Use upload instead
					commander.q_api.putJobLog(queueId, "trace", "Uploading: " + entry.getName());

					final ByteArrayOutputStream out = new ByteArrayOutputStream();
					IOUtils.upload(localFile, outputDir + "/" + entry.getName(), UserFactory.getByUsername(username), out, "-w", "-S",
							(entry.getOptions() != null && entry.getOptions().length() > 0 ? entry.getOptions().replace('=', ':') : "disk:2"), "-j", String.valueOf(queueId));
					final String output_upload = out.toString("UTF-8");
					final String lower_output = output_upload.toLowerCase();

					System.out.println("Output upload: " + output_upload);

					if (lower_output.contains("only")) {
						uploadedNotAllCopies = true;
						commander.q_api.putJobLog(queueId, "trace", output_upload);
						break;
					}
					else
						if (lower_output.contains("failed")) {
							uploadedAllOutFiles = false;
							commander.q_api.putJobLog(queueId, "trace", output_upload);
							break;
						}

					if (filesIncluded != null) {
						// Register lfn links to archive
						Register.register(entry, outputDir + "/", UserFactory.getByUsername(username));
					}

				}
				else {
					System.out.println("Can't upload output file " + localFile.getName() + ", does not exist or has zero size.");
					commander.q_api.putJobLog(queueId, "trace", "Can't upload output file " + localFile.getName() + ", does not exist or has zero size.");
				}

			} catch (final IOException e) {
				e.printStackTrace();
				uploadedAllOutFiles = false;
			}
		}
		// }

		if (jobStatus != JobStatus.ERROR_E && jobStatus != JobStatus.ERROR_V) {
			if (!uploadedAllOutFiles)
				changeStatus(JobStatus.ERROR_SV);
			else
				if (uploadedNotAllCopies)
					changeStatus(JobStatus.DONE_WARN);
				else
					changeStatus(JobStatus.DONE);
		}

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
		} catch (final Exception e) {
			System.out.println("There was a problem getting JDLVariables: " + e);
		}

		return hashret;
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(final String[] args) throws IOException {
		final JobAgent ja = new JobAgent();
		ja.run();
	}

	/**
	 * @param newStatus
	 */
	public void changeStatus(final JobStatus newStatus) {
		final HashMap<String, Object> extrafields = new HashMap<>();
		extrafields.put("exechost", this.ce);
		// if final status with saved files, we set the path
		if (newStatus == JobStatus.DONE || newStatus == JobStatus.DONE_WARN || newStatus == JobStatus.ERROR_E || newStatus == JobStatus.ERROR_V) {
			extrafields.put("path", getJobOutputDir());

			TaskQueueApiUtils.setJobStatus(queueId, newStatus, extrafields);
		}
		else
			if (newStatus == JobStatus.RUNNING) {	
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
		else
			if (outputDir == null)
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
