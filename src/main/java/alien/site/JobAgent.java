package alien.site;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.lang.ProcessBuilder.Redirect;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import alien.api.DispatchSSLClient;
import alien.api.Request;
import alien.api.taskQueue.GetMatchJob;
import alien.api.taskQueue.TaskQueueApiUtils;
import alien.config.ConfigUtils;
import alien.config.Version;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.shell.commands.JAliEnCOMMander;
import alien.site.containers.Containerizer;
import alien.site.containers.ContainerizerFactory;
import alien.site.packman.CVMFS;
import alien.taskQueue.JDL;
import alien.taskQueue.Job;
import alien.taskQueue.JobStatus;
import alien.taskQueue.TaskQueueUtils;
import apmon.ApMon;
import apmon.ApMonException;
import apmon.ApMonMonitoringConstants;
import apmon.BkThread;
import apmon.MonitoredJob;
import lazyj.ExtProperties;
import lazyj.Format;
import lazyj.commands.CommandOutput;
import lazyj.commands.SystemCommand;
import lia.util.process.ExternalProcesses;
import utils.ProcessWithTimeout;
import utils.Signals;

/**
 * Gets matched jobs, and launches JobWrapper for executing them
 */
public class JobAgent implements Runnable {

	private static final long CHECK_RESOURCES_INTERVAL = 5 * 1000L;
	private static final long SEND_RESOURCES_INTERVAL = 10 * 60 * 1000L;
	private static final long SEND_JOBINFO_INTERVAL = 60 * 1000L;

	// Variables passed through VoBox environment
	private final static Map<String, String> env = System.getenv();
	private final String ce;

	// Folders and files
	private File tempDir;
	private File jobTmpDir;
	private static final String defaultOutputDirPrefix = "/alien-job-";
	private String jobWorkdir;
	private final String jobstatusFile = ".jalienJobstatus";
	private final String siteSonarUrl = "http://alimonitor.cern.ch/sitesonar/";
	private final Charset charSet = StandardCharsets.UTF_8;

	// Job variables
	private JDL jdl;
	private long queueId;
	private int resubmission;
	private String username;
	private String tokenCert;
	private String tokenKey;
	private String jobAgentId;
	private final String workdir;
	private String legacyToken;
	private String platforms;
	private HashMap<String, Object> matchedJob;
	protected static HashMap<String, Object> siteMap = null;
	private long workdirMaxSizeMB;
	protected long jobMaxMemoryMB;
	private MonitoredJob mj;
	private Double prevCpuTime = ZERO;
	protected int cpuCores = 1;
	private long ttl;
	private String endState = "";
	private Float jobPrice = Float.valueOf(1);

	private static AtomicInteger totalJobs = new AtomicInteger(0);
	private final int jobNumber;

	// Other
	protected final String hostName;
	private final JAliEnCOMMander commander = JAliEnCOMMander.getInstance();
	private String jarPath;
	private String jarName;
	private int childPID;
	private long lastHeartbeat = 0;
	private int lowCpuUsageCounter = 0;
	private long jobStartupTime;
	protected final Containerizer containerizer = ContainerizerFactory.getContainerizer();
	private boolean checkCoreUsingJava = true;
	private int killreason = 0;

	private enum jaStatus {
		/**
		 * Initial state of the JA
		 */
		STARTING_JA(0, "Starting running Job Agent"),
		/**
		 * Asking for a job
		 */
		REQUESTING_JOB(1, "Asking for a job"),
		/**
		 * Preparing environment
		 */
		INSTALLING_PKGS(2, "Found a matching job"),
		/**
		 * Starting the payload
		 */
		JOB_STARTED(3, "Starting processing job's payload"),
		/**
		 * Payload running, waiting for it to complete
		 */
		RUNNING_JOB(4, "Running job's payload"),
		/**
		 * Finished running the payload
		 */
		DONE(5, "Finished running job"),
		/**
		 * Cleaning up before exiting
		 */
		FINISHING_JA(6, "Finished running Job Agent"),
		/**
		 * Error getting AliEn jar path
		 */
		ERROR_IP(-1, "Error getting AliEn jar path"),
		/**
		 * Error getting jdl
		 */
		ERROR_GET_JDL(-2, "Error getting jdl"),
		/**
		 * Error creating working directories
		 */
		ERROR_DIRS(-3, "Error creating working directories"),
		/**
		 * Error launching Job Wrapper to start job
		 */
		ERROR_START(-4, "Error launching Job Wrapper to start job");

		private final int value;
		private final String value_string;

		jaStatus(final int value, final String value_string) {
			this.value = value;
			this.value_string = value_string;
		}

		public int getValue() {
			return value;
		}

		public String getStringValue() {
			return value_string;
		}
	}

	/**
	 * logger object
	 */
	private final Logger logger;

	/**
	 * ML monitor object
	 */
	private Monitor monitor;

	/**
	 * ApMon sender
	 */
	static final ApMon apmon = MonitorFactory.getApMonSender();

	/**
	 * JSON Parser
	 */
	static final JSONParser jsonParser = new JSONParser();

	// _ource monitoring vars

	private static final Double ZERO = Double.valueOf(0);

	private Double RES_WORKDIR_SIZE = ZERO;
	protected Double RES_VMEM = ZERO;
	protected Double RES_RMEM = ZERO;
	private Double RES_VMEMMAX = ZERO;
	private Double RES_RMEMMAX = ZERO;
	private Double RES_MEMUSAGE = ZERO;
	private Double RES_CPUTIME = ZERO;
	private Double RES_CPUUSAGE = ZERO;
	private String RES_RESOURCEUSAGE = "";
	private Long RES_RUNTIME = Long.valueOf(0);
	private String RES_FRUNTIME = "";
	protected static Integer RES_NOCPUS = Integer.valueOf(1);
	private String RES_CPUMHZ = "";
	private String RES_CPUFAMILY = "";
	private String RES_BATCH_INFO = "";

	// Resource management vars

	/**
	 * TTL for the slot
	 */
	static int origTtl;
	private static final long jobAgentStartTime = System.currentTimeMillis();

	/**
	 * Number of remaining CPU cores to advertise
	 */
	static Long RUNNING_CPU;

	/**
	 * Amount of free disk space in the scratch area to advertise (in MB)
	 */
	static Long RUNNING_DISK;

	/**
	 * Number of CPU cores assigned to this slot
	 */
	static Long MAX_CPU;

	/**
	 * Number of currently active JobAgent instances
	 */
	static long RUNNING_JOBAGENTS;

	private Long reqCPU = Long.valueOf(0);
	private Long reqDisk = Long.valueOf(0);

	/**
	 * Boolean for CPU isolation
	 */
	static boolean cpuIsolation;

	private jaStatus status;

	/**
	 * Allow only one agent to request a job at a time
	 */
	protected static final Object requestSync = new Object();

	/**
	 * Procect access to shared resources
	 */
	protected static final Object cpuSync = new Object();

	/**
	 * How many consecutive answers of "no job for you" we got from the broker
	 */
	protected static final AtomicInteger retries = new AtomicInteger(0);

	/**
	 * Number of attempts since last successful job
	 */
	protected static final AtomicInteger attempts = new AtomicInteger(1);

	static NUMAExplorer numaExplorer;
	// static final NUMAExplorer numaExplorer = new NUMAExplorer(Runtime.getRuntime().availableProcessors());
	static boolean wholeNode;
	static byte[] initialMask;

	// Memory management
	private static MemoryController memoryController;
	protected boolean alreadyPreempted = false;
	private long jobAgentThreadStartTime = System.currentTimeMillis();

	protected static final Object workDirSizeSync = new Object();
	/**
	 * Map of <job ID, max workdir size in MB>
	 */
	private static HashMap<Long, Long> slotWorkdirsMaxSize;

	protected String agentCgroupV2;

	/**
	 */
	public JobAgent() {
		// site = env.get("site"); // or
		// ConfigUtils.getConfig().gets("alice_close_site").trim();

		ce = env.get("CE");

		jobNumber = totalJobs.incrementAndGet();

		monitor = MonitorFactory.getMonitor(JobAgent.class.getCanonicalName(), jobNumber);

		monitor.addMonitoring("resource_status", (names, values) -> {
			names.add("TTL_left");
			values.add(Integer.valueOf(computeTimeLeft(Level.OFF)));

			if (status != null) {
				names.add("ja_status_string");
				values.add(status.getStringValue());

				names.add("ja_status");
				values.add(Integer.valueOf(status.getValue()));

				names.add("ja_status_" + status.getValue());
				values.add(Integer.valueOf(1));
			}

			if (reqCPU.longValue() > 0) {
				names.add(reqCPU + "_cores_jobs");
				values.add(Long.valueOf(1));
			}

			names.add("num_cores");
			values.add(reqCPU);
		});

		setStatus(jaStatus.STARTING_JA);

		logger = ConfigUtils.getLogger(JobAgent.class.getCanonicalName() + " " + jobNumber);

		FileHandler handler = null;
		try {
			handler = new FileHandler("job-agent-" + jobNumber + ".log");
			handler.setFormatter(new SimpleFormatter() {
				private final String format = "%1$tb %1$td, %1$tY %1$tl:%1$tM:%1$tS %1$Tp %2$s JobNumber: " + jobNumber + "%n%4$s: %5$s%n";

				@Override
				public synchronized String format(final LogRecord record) {
					String source;
					if (record.getSourceClassName() != null) {
						source = record.getSourceClassName();
						if (record.getSourceMethodName() != null) {
							source += " " + record.getSourceMethodName();
						}
					}
					else {
						source = record.getLoggerName();
					}
					final String message = formatMessage(record);
					String throwable = "";
					if (record.getThrown() != null) {
						final StringWriter sw = new StringWriter();
						try (PrintWriter pw = new PrintWriter(sw)) {
							pw.println();
							record.getThrown().printStackTrace(pw);
						}
						throwable = sw.toString();
					}
					return String.format(format,
							new Date(record.getMillis()),
							source,
							record.getLoggerName(),
							record.getLevel().getLocalizedName(),
							message,
							throwable);
				}
			});

			logger.addHandler(handler);

		}
		catch (final IOException ie) {
			logger.log(Level.WARNING, "Problem with getting logger: " + ie.toString());
			ie.printStackTrace();
		}

		final String DN = commander.getUser().getUserCert()[0].getSubjectDN().toString();

		logger.log(Level.INFO, "We have the following DN :" + DN);

		synchronized (env) {
			if (siteMap == null) {
				siteMap = (new SiteMap()).getSiteParameters(env);

				MAX_CPU = Long.valueOf(((Number) siteMap.getOrDefault("CPUCores", Integer.valueOf(1))).longValue());
				RUNNING_CPU = MAX_CPU;
				// siteMap.Disk is expected to be in KB here, RUNNING_DISK is in MB
				RUNNING_DISK = Long.valueOf(((Long) siteMap.getOrDefault("Disk", Long.valueOf(10 * 1024 * 1024 * RUNNING_CPU.longValue()))).longValue() / 1024);
				origTtl = ((Integer) siteMap.get("TTL")).intValue();
				RUNNING_JOBAGENTS = 0;
			}
		}

		hostName = (String) siteMap.get("Localhost");

		collectSystemInformation();
		// alienCm = (String) siteMap.get("alienCm");

		if (env.containsKey("ALIEN_JOBAGENT_ID"))
			jobAgentId = env.get("ALIEN_JOBAGENT_ID");
		else
			jobAgentId = Request.getVMID().toString();

		if (env.containsKey("cpuIsolation"))
			cpuIsolation = Boolean.parseBoolean(env.get("cpuIsolation"));

		logger.log(Level.INFO, "cpuIsolation = " + cpuIsolation);

		workdir = Functions.resolvePathWithEnv((String) siteMap.get("workdir"));

		Hashtable<Long, String> cpuinfo;

		try {
			cpuinfo = BkThread.getCpuInfo();
			RES_CPUFAMILY = cpuinfo.get(ApMonMonitoringConstants.LGEN_CPU_FAMILY);
			RES_CPUMHZ = cpuinfo.get(ApMonMonitoringConstants.LGEN_CPU_MHZ);
			RES_CPUMHZ = RES_CPUMHZ.substring(0, RES_CPUMHZ.indexOf("."));
			RES_NOCPUS = Integer.valueOf(BkThread.getNumCPUs());

			logger.log(Level.INFO, "CPUFAMILY: " + RES_CPUFAMILY);
			logger.log(Level.INFO, "CPUMHZ: " + RES_CPUMHZ);
			logger.log(Level.INFO, "NOCPUS: " + RES_NOCPUS);
		}
		catch (final IOException e) {
			logger.log(Level.WARNING, "Problem with the monitoring objects IO Exception: " + e.toString());
		}
		catch (final ApMonException e) {
			logger.log(Level.WARNING, "Problem with the monitoring objects ApMon Exception: " + e.toString());
		}
		catch (final Exception e) {
			logger.log(Level.WARNING, "Unknown exception: " + e.toString());
		}

		wholeNode = ((Boolean) siteMap.getOrDefault("WholeNode", Boolean.valueOf(false))).booleanValue();
		initialMask = getInitialMask();
		synchronized (cpuSync) {
			if (numaExplorer == null && cpuIsolation == true)
				numaExplorer = new NUMAExplorer(RES_NOCPUS.intValue());
		}
		logger.log(Level.INFO, "Going to register memory limits ... ");
		synchronized (env) {
			if (memoryController == null) {
				memoryController = new MemoryController(MAX_CPU.longValue());
				Thread tMemController = new Thread(memoryController, "MemoryController");
				tMemController.setDaemon(true);
				tMemController.start();
			}
			synchronized (workDirSizeSync) {
				if (slotWorkdirsMaxSize == null)
					slotWorkdirsMaxSize = new HashMap<>();
			}
		}

		try {
			final File filepath = new java.io.File(JobAgent.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath());
			jarName = filepath.getName();
			jarPath = filepath.toString().replace(jarName, "");
		}
		catch (final URISyntaxException e) {
			logger.log(Level.SEVERE, "Could not obtain AliEn jar path: " + e.toString());

			setStatus(jaStatus.ERROR_IP);

			setUsedCores(0);
		}

	}

	// #############################################################
	// ################## MAIN EXECUTION FLOW ######################
	// #############################################################
	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(final String[] args) throws IOException {
		ConfigUtils.setApplicationName("JobAgent");
		DispatchSSLClient.setIdleTimeout(30000);
		ConfigUtils.switchToForkProcessLaunching();
		final JobAgent jao = new JobAgent();
		jao.run();
	}

	@SuppressWarnings({ "boxing", "unchecked" })
	@Override
	public void run() {
		logger.log(Level.INFO, "Starting JobAgent " + jobNumber + " in " + hostName);

		// Wait before matching if previous jobs have been failing
		if (attempts.getAcquire() > 1) {
			final int timeToWait = attempts.getAcquire();
			try {
				if ((int) siteMap.get("TTL") - timeToWait > 0) {
					logger.log(Level.INFO, "A previous job failed. Will wait " + timeToWait + "s before continuing...");
					Thread.sleep(timeToWait * 1000);
				}
				else {
					logger.log(Level.INFO, "Too many jobs have failed, and the timeout delay now exceeds TTL. Aborting...");
					System.exit(0);
				}
			}
			catch (@SuppressWarnings("unused") InterruptedException e1) {
				// ignore
			}
		}

		logger.log(Level.INFO, siteMap.toString());
		try {
			logger.log(Level.INFO, "Resources available: " + RUNNING_CPU + " CPU cores and " + RUNNING_DISK + " MB of disk space");
			synchronized (requestSync) {
				RUNNING_JOBAGENTS += 1;
				if (!updateDynamicParameters()) {
					// requestSync.notify();
					return;
				}

				monitor.sendParameter("TTL", siteMap.get("TTL"));

				// TODO: Hack to exclude alihyperloop jobs from nodes without avx support. Remove me soon!
				try {
					if (!Files.readString(Paths.get("/proc/cpuinfo")).contains("avx") && !System.getProperty("os.arch").contains("aarch64")) {
						final ArrayList<String> noUsers = (ArrayList<String>) siteMap.computeIfAbsent("NoUsers", (k) -> new ArrayList<>());
						if (!noUsers.contains("alihyperloop"))
							((ArrayList<String>) siteMap.get("NoUsers")).add("alihyperloop");
						if (!noUsers.contains("alitrain"))
							((ArrayList<String>) siteMap.get("NoUsers")).add("alitrain");
						if (!noUsers.contains("aliprod"))
							((ArrayList<String>) siteMap.get("NoUsers")).add("aliprod");
						logger.log(Level.WARNING, "This host appears to be missing AVX support, and will be blocked from running the following jobs: " + noUsers.toString());
					}
				}
				catch (IOException | NullPointerException ex) {
					logger.log(Level.WARNING, "Unable to check for AVX support", ex);
				}

				// Verify environment if there are no containers, before matching
				if (containerizer == null && !env.getOrDefault("DISABLE_CONTAINER_ENFORCE", "").toLowerCase().contains("true")) {
					logger.log(Level.SEVERE, "This host does not appear to support containers. Please verify that user namespaces are enabled, or disable this check in container.properties");
					throw new EOFException("Job matching aborted due to missing containers");
				}

				setStatus(jaStatus.REQUESTING_JOB);

				// Brokering expects disk space to be expressed in KB (JOBAGENT table content)
				final GetMatchJob jobMatch = commander.q_api.getMatchJob(new HashMap<>(siteMap));

				matchedJob = jobMatch.getMatchJob();

				// TODELETE
				if (matchedJob == null || matchedJob.containsKey("Error")) {
					final String msg = "We didn't get anything back. Nothing to run right now.";
					logger.log(Level.INFO, msg);

					RUNNING_JOBAGENTS -= 1;

					setStatus(jaStatus.ERROR_GET_JDL);

					throw new EOFException(msg);
				}

				retries.set(0);

				jdl = new JDL(Job.sanitizeJDL((String) matchedJob.get("JDL")));

				queueId = ((Long) matchedJob.get("queueId")).longValue();
				resubmission = ((Integer) matchedJob.get("Resubmission")).intValue();
				username = (String) matchedJob.get("User");
				tokenCert = (String) matchedJob.get("TokenCertificate");
				tokenKey = (String) matchedJob.get("TokenKey");
				legacyToken = (String) matchedJob.get("LegacyToken");
				platforms = (String) matchedJob.get("Platforms");

				monitor.sendParameter("job_id", Long.valueOf(queueId));

				setStatus(jaStatus.INSTALLING_PKGS);

				matchedJob.entrySet().forEach(entry -> {
					logger.log(Level.INFO, entry.getKey() + " " + entry.getValue());
				});

				if (platforms != null) {
					putJobTrace("This job has requested packages available on the following platforms: " + platforms + ".");
					if (containerizer != null && !env.containsKey("JOB_CONTAINER_PATH")) {
						if (platforms.contains("el6-x86_64"))
							putJobTrace("Warning: This job has requested a deprecated package, and support may be dropped soon!");
						containerizer.setContainerPath(CVMFS.getContainerPath(platforms));
					}
				}

				if (!MemoryController.limitParser.isEmpty())
					putJobTrace(MemoryController.limitParser);

				logger.log(Level.INFO, jdl.getExecutable());
				logger.log(Level.INFO, username);
				logger.log(Level.INFO, Long.toString(queueId));

				sendBatchInfo();
				if (jobKilled) {
					putJobTrace("Error: this job is not supposed to be running anymore. Aborting.");
					throw new EOFException("Job is not supposed to be running");
				}

				reqCPU = Long.valueOf(TaskQueueUtils.getCPUCores(jdl));
				setUsedCores(1);

				reqDisk = Long.valueOf(TaskQueueUtils.getWorkDirSizeMB(jdl, reqCPU.intValue()));

				logger.log(Level.INFO, "Job requested " + reqCPU + " CPU cores and " + reqDisk + " MB of disk space");

				RUNNING_CPU -= reqCPU;
				synchronized (env) {
					RUNNING_DISK = recomputeDiskSpace();
				}
				logger.log(Level.INFO, "The recomputed disk space is " + RUNNING_DISK + " MB");
				RUNNING_DISK -= reqDisk;
				logger.log(Level.INFO, "Currently available CPUCores: " + RUNNING_CPU);
				logger.log(Level.INFO, "Task isolation is set to " + cpuIsolation);

				jobPrice = jdl.getFloat("Price");
				if (jobPrice == null)
					jobPrice = Float.valueOf(1);
				logger.log(Level.INFO, "Job Price is set to " + jobPrice);

				requestSync.notifyAll();
			}

			logger.log(Level.INFO, jdl.getExecutable());
			logger.log(Level.INFO, username);
			logger.log(Level.INFO, Long.toString(queueId));

			setStatus(jaStatus.JOB_STARTED);

			// process payload
			int exitCode = handleJob();

			// Resubmit if the job was never able to start, but not if inputfile(s) could not be fetched
			// or if has already been resubmitted several times (>=10)
			if (("ERROR_IB".equals(endState) || endState.isBlank()) && exitCode != 3 && resubmission < 10) {
				logger.log(Level.INFO, "Putting job " + queueId + " back to waiting");
				putJobTrace("Putting job back to waiting " + queueId);
				changeJobStatus(JobStatus.WAITING, -1);
			}

			cleanup();

			synchronized (requestSync) {
				RUNNING_CPU += reqCPU;
				RUNNING_DISK += reqDisk;
				RUNNING_JOBAGENTS -= 1;
				setUsedCores(0);

				requestSync.notifyAll();
			}
		}
		catch (final Exception e) {
			if (!(e instanceof EOFException))
				logger.log(Level.WARNING, "Another exception matching the job", e);

			setStatus(jaStatus.ERROR_GET_JDL);

			if (RUNNING_CPU.equals(MAX_CPU))
				retries.getAndIncrement();

		}
		finally {
			if (cpuIsolation == true)
				numaExplorer.refillAvailable(jobNumber);

			if ("ERROR_IB".equals(endState) || "ERROR_E".equals(endState))
				attempts.getAndIncrement();
			else
				attempts.set(1);

			synchronized (env) {
				logger.log(Level.INFO, "Removing job from Memory Controller Registries");
				MemoryController.removeJobFromRegistries(queueId);
				synchronized (workDirSizeSync) {
					slotWorkdirsMaxSize.remove(queueId);
				}
			}
		}

		setStatus(jaStatus.FINISHING_JA);

		monitor.setMonitoring("resource_status", null);

		MonitorFactory.stopMonitor(monitor);

		logger.log(Level.INFO, "JobAgent finished, id: " + jobAgentId + " totalJobs: " + totalJobs.get());
		Thread.currentThread().interrupt();
		return;
	}

	private int handleJob() {
		int returnValue = 0;
		Process p = null;
		try {
			if (!createWorkDir()) {
				changeJobStatus(JobStatus.ERROR_IB, -1);
				logger.log(Level.INFO, "Error. Workdir for job could not be created");
				putJobTrace("Error. Workdir for job could not be created");
				return -1;
			}

			logger.log(Level.INFO, "Started JA with: " + jdl);

			final String version = !Version.getTag().isEmpty() ? Version.getTag() : "Git: " + Version.getGitHash();
			putJobTrace("Running JAliEn JobAgent " + version + " on " + hostName + ". Builddate: " + Version.getCompilationTimestamp());

			if (env.getOrDefault("UNAME_M", "").contains("aarch64"))
				putJobTrace("Warning: this job will be executed on an aarch64 worker");

			// Set up constraints
			getMemoryRequirements();

			setupJobWrapperLogging();

			putJobTrace("Starting JobWrapper");

			ttl = ttlForJob();

			synchronized (cpuSync) {
				p = launchJobWrapper(generateLaunchCommand());
				if (p != null && p.isAlive())
					childPID = (int) p.pid();
			}

			// Start and monitor execution
			returnValue = monitorExecution(p, true);
		}
		catch (final Exception e) {
			logger.log(Level.SEVERE, "Unable to handle job", e);
			putJobTrace("ERROR: Unable to handle job: " + e.toString() + " " + Arrays.toString(e.getStackTrace()));
			returnValue = -1;

		}
		finally {
			boolean oom = checkOOMDump();
			endState = getWrapperJobStatus();
			if (oom && !"DONE".equals(endState) && !endState.startsWith("ERROR")) {
				killreason = 10;
				changeJobStatus(JobStatus.ERROR_E, -1);
				if (p != null)
					killGracefully(p);
			}
		}
		return returnValue;
	}

	/**
	 * @return Command w/arguments for starting the JobWrapper, based on the command used for the JobAgent
	 * @throws InterruptedException
	 */
	public List<String> generateLaunchCommand() throws InterruptedException {
		try {
			// Main cmd for starting the JobWrapper
			List<String> launchCmd = new ArrayList<>();

			final String[] cmdCheck = env.getOrDefault("JALIEN_JOBAGENT_CMD",
					SystemCommand.bash("ps -p " + String.valueOf(MonitorFactory.getSelfProcessID()) + " -o command=").stdout).split("\\s+");

			for (int i = 0; i < cmdCheck.length; i++) {
				logger.log(Level.INFO, cmdCheck[i]);
				if (cmdCheck[i].contains("-cp"))
					i++;
				else if (cmdCheck[i].contains("-Xms"))
					launchCmd.add("-Xms60M");
				else if (cmdCheck[i].contains("-Xmx"))
					launchCmd.add("-Xmx60M");
				else if (cmdCheck[i].contains("alien.site.JobRunner") || cmdCheck[i].contains("alien.site.JobAgent")) {
					launchCmd.add("-XX:OnOutOfMemoryError=\"echo 'Process %p has run out of memory' > ./" + queueId + ".oom\"");
					launchCmd.add("-Djobagent.vmid=" + queueId);
					launchCmd.add("-DAliEnConfig=.");
					launchCmd.add("-cp");
					launchCmd.add(jarPath + jarName);
					launchCmd.add("alien.site.JobWrapper");
				}
				else if (!cmdCheck[i].contains("JobRunner") && !cmdCheck[i].contains("JobAgent")) // Just to be completely sure...
					launchCmd.add(cmdCheck[i]);
			}

			// If there is container support present on site, add to launchCmd
			if (containerizer != null) {
				putJobTrace("Support for containers detected. Using " + containerizer.getContainerizerName());
				containerizer.setWorkdir(jobWorkdir); // Will be bind-mounted to "/workdir" in the container (workaround for unprivileged bind-mounts)

				if (jdl.gets("DebugTag") != null)
					containerizer.enableDebug(jdl.gets("DebugTag"));

				launchCmd = containerizer.containerize(String.join(" ", launchCmd));
			}

			final String currentCgroup = CgroupUtils.getCurrentCgroup(Math.toIntExact(MonitorFactory.getSelfProcessID()));
			// Run jobs in isolated environment
			if (cpuIsolation == true && CgroupUtils.haveCgroupsv2() && !currentCgroup.contains("runner")) {
				putJobTrace("The runner cgroupv2 could not be created. Running in cgroup " + currentCgroup);
			}
			if (cpuIsolation == true && (!CgroupUtils.haveCgroupsv2() || (CgroupUtils.haveCgroupsv2() && !currentCgroup.contains("runner")))) {
				String isolCmd = addIsolation();
				logger.log(Level.SEVERE, "IsolCmd command" + isolCmd);
				if (isolCmd != null && isolCmd.compareTo("") != 0)
					launchCmd.addAll(0, Arrays.asList("taskset", "-c", isolCmd));
			}

			return launchCmd;
		}
		catch (final Exception e) {
			logger.log(Level.SEVERE, ": ", e);
			putJobTrace("Could not generate JobWrapper launch command " + e.toString());

			try {
				File[] listOfFiles = new File("/proc/" + MonitorFactory.getSelfProcessID() + "/fd").listFiles();
				putJobTrace("Length of /proc/" + MonitorFactory.getSelfProcessID() + "/fd is: " + listOfFiles.length);
				logger.log(Level.INFO, "Length of /proc/" + MonitorFactory.getSelfProcessID() + "/fd is: " + listOfFiles.length);
			}
			catch (final Exception e2) {
				putJobTrace("Could not run debug for launchCommand: " + e2.toString());
				logger.log(Level.SEVERE, "Could not run debug for launchCommand: ", e2);
			}
			return null;
		}
	}

	private Process launchJobWrapper(final List<String> launchCommand) {
		logger.log(Level.INFO, "Launching jobwrapper using the command: " + launchCommand.toString());

		final ProcessBuilder pBuilder = new ProcessBuilder(launchCommand);
		pBuilder.environment().remove("JALIEN_TOKEN_CERT");
		pBuilder.environment().remove("JALIEN_TOKEN_KEY");
		pBuilder.environment().put("TMPDIR", "tmp"); // Only for JW start --> set to jobworkdir/tmp by JW for payload
		pBuilder.redirectError(Redirect.INHERIT);
		pBuilder.directory(tempDir);

		jobStartupTime = System.currentTimeMillis();
		setStatus(jaStatus.RUNNING_JOB);

		final Process p;

		// stdin from the viewpoint of the wrapper
		final OutputStream stdin;

		try {
			p = pBuilder.start();

			stdin = p.getOutputStream();
			try (ObjectOutputStream stdinObj = new ObjectOutputStream(stdin)) {
				stdinObj.writeObject(jdl);
				stdinObj.writeObject(username);
				stdinObj.writeObject(Long.valueOf(queueId));
				stdinObj.writeObject(Integer.valueOf(resubmission));
				stdinObj.writeObject(tokenCert);
				stdinObj.writeObject(tokenKey);
				stdinObj.writeObject(ce);
				stdinObj.writeObject(siteMap);
				stdinObj.writeObject(defaultOutputDirPrefix);
				stdinObj.writeObject(legacyToken);
				stdinObj.writeObject(Long.valueOf(ttl));
				stdinObj.writeObject(env.getOrDefault("PARENT_HOSTNAME", ""));
				stdinObj.writeObject(getMetaVariables());

				stdinObj.flush();
			}

			logger.log(Level.INFO, "JDL info sent to JobWrapper");
			putJobTrace("JobWrapper started");

			// Wait for JobWrapper to start
			try (InputStream stdout = p.getInputStream()) {
				stdout.read();
			}

			// Setup cgroup for wrapper if supported ("runner" cgroup exists)
			final String currentCgroup = CgroupUtils.getCurrentCgroup(Math.toIntExact(p.pid()));
			if (currentCgroup.contains("runner")) {
				final String agentsCgroup = currentCgroup.replace("runner", "agents");
				CgroupUtils.createCgroup(agentsCgroup, Thread.currentThread().getName());
				agentCgroupV2 = agentsCgroup + "/" + Thread.currentThread().getName();
				CgroupUtils.moveProcessToCgroup(agentCgroupV2, getWrapperPid());
				if (numaExplorer == null) {
					synchronized (cpuSync) {
						numaExplorer = new NUMAExplorer(RES_NOCPUS.intValue());
					}
				}
				logger.log(Level.INFO, "cgroup controllers set --> cpu: " + CgroupUtils.hasController(agentCgroupV2, "cpu") + " cpuset: " + CgroupUtils.hasController(agentCgroupV2, "cpuset")
						+ " memory: " + CgroupUtils.hasController(agentCgroupV2, "memory"));
				// if (CgroupUtils.hasController(agentCgroupV2, "memory"))
				// CgroupUtils.setLowMemoryLimit(agentCgroupV2, CgroupUtils.LOW_MEMORY_JA * cpuCores);
				if (CgroupUtils.haveCgroupsv2() && CgroupUtils.hasController(agentCgroupV2, "cpu") && cpuIsolation == true) {
					if (wholeNode && CgroupUtils.hasController(agentCgroupV2, "cpuset")) {
						numaExplorer.setFullNUMAMask();
						String isolCmd = addIsolation();
						logger.log(Level.INFO, "Going to assign cgroup " + agentCgroupV2 + " to CPU cores " + isolCmd);
						CgroupUtils.assignCPUCores(agentCgroupV2, isolCmd);
					}
					putJobTrace("Limiting CPU bandwidth of cgroup " + agentCgroupV2);
					CgroupUtils.setCPUUsageQuota(agentCgroupV2, cpuCores);
				}
			}
		}
		catch (final Exception ioe) {
			logger.log(Level.SEVERE, "Exception running " + launchCommand + " : " + ioe.getMessage());
			setStatus(jaStatus.ERROR_START);

			putJobTrace("Error starting JobWrapper: exception running " + launchCommand + " : " + ioe.getMessage());
			changeJobStatus(JobStatus.ERROR_A, -1);

			setUsedCores(0);

			return null;
		}

		return p;
	}

	private int monitorExecution(Process p, boolean monitorJob) {
		boolean payloadMonitoring = false;

		if (p == null || !p.isAlive())
			return -1;

		if (monitorJob) {
			final String process_res_format = "FRUNTIME | RUNTIME | CPUUSAGE | MEMUSAGE | CPUTIME | RMEM | VMEM | NOCPUS | CPUFAMILY | CPUMHZ | RESOURCEUSAGE | RMEMMAX | VMEMMAX";
			logger.log(Level.INFO, process_res_format);
			putJobLog("procfmt", process_res_format);

			// apmon.setNumCPUs(cpuCores);
			// apmon.addJobToMonitor(wrapperPID, jobWorkdir, ce + "_Jobs", matchedJob.get("queueId").toString());
			mj = new MonitoredJob(childPID, jobWorkdir, ce + "_Jobs", matchedJob.get("queueId").toString(), cpuCores);
			mj.setJobStartupTime(jobStartupTime);
			MemoryController.activeJAInstances.put(Long.valueOf(queueId), this);

			String monitoring = jdl.gets("Monitoring");
			if (monitoring != null && monitoring.toUpperCase().contains("PAYLOAD")) {
				payloadMonitoring = true;
				mj.setWrapperPid(getWrapperPid());
				// mjPayload = apmon.addJobToMonitor(getWrapperPid(), jobWorkdir, ce + "_JobWrapper", matchedJob.get("queueId").toString());
				mj.setPayloadMonitoring();
			}

			final String fs = checkProcessResources();
			if (fs == null)
				sendProcessResources(false);
		}

		Signals.addHandler("INT", () -> {
			logger.log(Level.SEVERE, "JobAgent: INT received. Killing payload!");
			if (p.isAlive()) {
				putJobTrace("JobAgent: INT received. Killing payload!");
				killForcibly(p);
			}
			else {
				JobWrapper.cleanupProcesses(queueId, mj.getPid());
				System.exit(130);
			}
		});

		Signals.addHandler("TERM", () -> {
			logger.log(Level.SEVERE, "JobAgent: TERM received. Killing payload!");
			if (p.isAlive()) {
				putJobTrace("JobAgent: TERM received. Killing payload!");
				killForcibly(p);
			}
			else {
				JobWrapper.cleanupProcesses(queueId, mj.getPid());
				System.exit(130);
			}
		});

		final TimerTask killPayload = new TimerTask() {
			@Override
			public void run() {
				logger.log(Level.SEVERE, "Timeout has occurred. Killing job!");
				putJobTrace("Killing the job (it was running for longer than its TTL)");
				killGracefully(p);
				killreason = 20;
			}
		};

		final Timer t = new Timer();
		t.schedule(killPayload, TimeUnit.MILLISECONDS.convert(ttl, TimeUnit.SECONDS)); // TODO: ttlForJob

		long lastStatusChange = getWrapperJobStatusTimestamp();

		int code = -1;

		final Thread heartMon = new Thread(heartbeatMonitor(p), "Heartbeat Monitor");
		heartMon.start();

		long initTimeSendResourcess = System.currentTimeMillis();
		long initTimeJobInfo = System.currentTimeMillis();

		boolean discoveredPid = false;
		try {
			while (p.isAlive()) {
				logger.log(Level.FINEST, "Waiting for the JobWrapper process to finish");
				if (monitorJob) {
					final String error = checkProcessResources();
					if (error != null) {
						t.cancel();
						heartMon.interrupt();
						if (!jobKilled && !jobOOMPreempted) {
							logger.log(Level.SEVERE, "Monitor has detected an error: " + error);
							putJobTrace("[FATAL]: Monitor has detected an error: " + error);
							killGracefully(p);
						}
						else {
							if (jobOOMPreempted) {
								changeJobStatus(JobStatus.ERROR_E, -1);
								logger.log(Level.SEVERE, "ERROR[FATAL]: Job PREEMPTED due to memory overconsumption. Terminating...");
								putJobTrace("ERROR[FATAL]: Job PREEMPTED due to memory overconsumption. Terminating...");
								killGracefully(p);
								/*
								 * synchronized(MemoryController.lockMemoryController) {
								 * logger.log(Level.INFO, "Preemption of job " + queueId + " is going to be done NOW");
								 * MemoryController.preemptingJob = false;
								 * }
								 */
							}
							else {
								logger.log(Level.SEVERE, "ERROR[FATAL]: Job KILLED by user! Terminating...");
								putJobTrace("ERROR[FATAL]: Job KILLED by user! Terminating...");
								killForcibly(p);
							}

						}
						return 1;
					}
					// Send report once every 10 min, or when the job changes state
					if ((System.currentTimeMillis() - initTimeSendResourcess) > SEND_RESOURCES_INTERVAL) {
						initTimeSendResourcess = initTimeSendResourcess + SEND_RESOURCES_INTERVAL;
						sendProcessResources(false);
					}

					// set to 24
					if ((System.currentTimeMillis() - initTimeJobInfo) > SEND_JOBINFO_INTERVAL) {
						initTimeJobInfo = initTimeJobInfo + SEND_JOBINFO_INTERVAL;
						try {
							apmon.sendOneJobInfo(mj, true);
							monitor.sendParameter("job_id", Long.valueOf(queueId));
						}
						catch (NullPointerException npe) {
							putJobTrace("Fatal: ApMon is null on " + hostName + ". " + npe.getMessage());
							killForcibly(p); // Abort
						}
					}

					else if (getWrapperJobStatusTimestamp() != lastStatusChange) {
						final String wrapperStatus = getWrapperJobStatus();

						if (!"STARTED".equals(wrapperStatus) && !"RUNNING".equals(wrapperStatus)) {
							lastStatusChange = getWrapperJobStatusTimestamp();
							sendProcessResources(false);
						}
						if ("RUNNING".equals(wrapperStatus) && payloadMonitoring == true && mj != null && discoveredPid == false) {
							mj.discoverPayloadPid("payload-" + queueId);
							discoveredPid = true;
						}

						// Check if the wrapper has finished without us knowing
						if ("DONE".equals(wrapperStatus) || wrapperStatus.contains("ERROR")) {

							// In case the wrapper was just about to exit normally, wait a few seconds
							if (!p.waitFor(15, TimeUnit.SECONDS)) {
								putJobTrace("Warning: The JobWrapper appears to be finished, but not fully exited. Killing remaining processes...");
								p.destroyForcibly();
							}
						}
					}
				}
				try {
					Thread.sleep(CHECK_RESOURCES_INTERVAL);
				}
				catch (final InterruptedException ie) {
					logger.log(Level.WARNING, "Interrupted while waiting for the JobWrapper to finish execution: " + ie.getMessage());
					return 1;
				}
			}
			code = p.exitValue();

			// Send a final report once the payload completes
			sendProcessResources(true);

			logger.log(Level.INFO, "JobWrapper has finished execution. Exit code: " + code);
			logger.log(Level.INFO, "All done for job " + queueId + ". Final status: " + getWrapperJobStatus());
			putJobTrace("JobWrapper exit code: " + code);
			if (code != 0)
				logger.log(Level.WARNING, "Error encountered in the JobWrapper process");

			return code;
		}
		catch (Exception ex) {
			logger.log(Level.WARNING, "Error encountered while running job: ", ex);
			putJobTrace("Error encountered while running job: " + ex);
			return -1;
		}
		finally {
			try {
				t.cancel();
				heartMon.interrupt();
				p.getOutputStream().close();
			}
			catch (final Exception e) {
				logger.log(Level.WARNING, "Not all resources from the current job could be cleared: " + e);
			}

			if (mj != null)
				mj.close();

			endState = getWrapperJobStatus();
			if (code != 0) {
				if ("STARTED".equals(endState) || "RUNNING".equals(endState)) {
					putJobTrace("ERROR: The JobWrapper was killed before job could complete");
					changeJobStatus(JobStatus.ERROR_E, code); // JobWrapper was killed before the job could be completed
				}
				else if ("SAVING".equals(endState)) {
					putJobTrace("ERROR: The JobWrapper was killed during saving");
					changeJobStatus(JobStatus.ERROR_SV, code); // JobWrapper was killed during saving
				}
				else if (endState.isBlank()) {
					putJobTrace("ERROR: The JobWrapper was killed before job start");
					changeJobStatus(JobStatus.ERROR_A, code); // JobWrapper was killed before payload start
				}
			}
		}
	}

	private void cleanup() {
		logger.log(Level.INFO, "Sending monitoring values...");

		setStatus(jaStatus.DONE);

		monitor.sendParameter("job_id", Integer.valueOf(0));

		logger.log(Level.INFO, "Cleaning up after execution...");
		putJobTrace("Cleaning up after execution...");

		try {
			Files.walk(tempDir.toPath())
					.map(Path::toFile)
					.sorted(Comparator.reverseOrder()) // or else dir will appear before its contents
					.forEach(File::delete);
		}
		catch (final IOException | UncheckedIOException e) {
			logger.log(Level.WARNING, "Error deleting the job workdir, using system commands instead", e);
			logger.log(Level.INFO, "Proceeding to attempt deleting the following dir using system commands: " + tempDir.getAbsolutePath());

			final CommandOutput rmOutput = SystemCommand.executeCommand(Arrays.asList("rm", "-rf", tempDir.getAbsolutePath()), true);

			if (rmOutput == null)
				logger.log(Level.SEVERE, "Cannot clean up the job dir even using system commands");
			else
				logger.log(Level.INFO, "System command cleaning of job work dir returned " + rmOutput.exitCode + ", full output:\n" + rmOutput.stdout);
		}

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

		if (mj != null)
			mj.close();

		logger.log(Level.INFO, "Done!");
	}

	// ####################################################################
	// ########################### CPU Isolation ##########################
	// ####################################################################

	/**
	 * @return mask with already pinned cores by other running workloads
	 */
	byte[] getFreeCPUs() {
		BigInteger mask = BigInteger.ZERO;

		try {
			String cmd = "pgrep -v -U root -u root | xargs -L1 taskset -a -p 2>/dev/null | cut -d' ' -f6 | sort -u";

			CommandOutput output = SystemCommand.bash(cmd, true);

			try (BufferedReader br = output.reader()) {
				String readArg;
				while ((readArg = br.readLine()) != null) {
					try {
						BigInteger newVal = new BigInteger(readArg.trim(), 16);
						if (BigInteger.ONE.shiftLeft(RES_NOCPUS.intValue()).subtract(BigInteger.ONE).equals(newVal))
							continue;

						mask = mask.or(newVal);
					}
					catch (NumberFormatException nfe) {
						logger.log(Level.WARNING, "Exception parsing a line of output from pgrep: " + readArg, nfe);
						return new byte[RES_NOCPUS.intValue()];
					}
				}
				return valueToArray(mask, RES_NOCPUS.intValue());
			}
		}
		catch (IOException | IllegalArgumentException e) {
			logger.log(Level.WARNING, "Exception when getting free CPUs", e);
		}

		return new byte[RES_NOCPUS.intValue()];
	}

	static byte[] valueToArray(BigInteger v, int size) {
		byte[] maskArray = new byte[size];
		int count = 0;
		BigInteger vAux = v;

		while (vAux.compareTo(BigInteger.ZERO) > 0 && count < size) {
			maskArray[count] = vAux.testBit(0) ? (byte) 1 : (byte) 0;
			vAux = vAux.shiftRight(1);
			count++;
		}
		return maskArray;
	}

	/**
	 * @return mask our workload has already been pinned to
	 */
	byte[] getHostMask() {

		String cmd = "taskset -p $$ | cut -d' ' -f6";

		CommandOutput output = SystemCommand.bash(cmd, true);

		try (BufferedReader br = output.reader()) {
			String readArg;
			if ((readArg = br.readLine()) != null) {
				try {
					return valueToArray((new BigInteger(readArg.trim(), 16)).not().and(BigInteger.ONE.shiftLeft(RES_NOCPUS.intValue()).subtract(BigInteger.ONE)), RES_NOCPUS.intValue());
				}
				catch (NumberFormatException nfe) {
					logger.log(Level.WARNING, "Exception parsing a line of output from taskset: " + readArg, nfe);
				}
			}
		}
		catch (IOException | IllegalArgumentException e) {
			logger.log(Level.WARNING, "Exception when getting hostMask", e);
		}
		return new byte[RES_NOCPUS.intValue()];
	}

	/**
	 * @return mask from which we start CPU assignment
	 */
	public byte[] getInitialMask() {
		byte[] mask;
		byte[] hostMask;
		mask = getFreeCPUs();
		hostMask = getHostMask();
		// In case we could not parse the mask of other processes we get it empty
		if (mask == null || hostMask == null)
			return (new byte[RES_NOCPUS.intValue()]);
		boolean check = true;
		for (int i = 0; i < RES_NOCPUS.intValue(); i++) {
			if (hostMask[i] != 0) {
				check = false;
				break;
			}
		}
		if (check == true)
			return mask;
		return hostMask;
	}

	/**
	 * @return CPU cores to which the job has to be pinned to
	 */
	private String addIsolation() {
		String isolatedCPUs = "";
		synchronized (cpuSync) {
			numaExplorer.activeJAInstances.put(Integer.valueOf(jobNumber), this);
			isolatedCPUs = numaExplorer.pickCPUs(reqCPU, jobNumber);
		}

		return isolatedCPUs;
	}

	// ####################################################################
	// ################## MONITORING HELPER FUNCTIONS #####################
	// ####################################################################

	/**
	 * updates jobagent parameters that change between job requests
	 *
	 * @return false if we can't run because of current conditions, true if positive
	 */
	public boolean checkParameters() {
		final int timeleft = computeTimeLeft(Level.INFO);
		if (timeleft <= 0)
			return false;

		RUNNING_DISK = recomputeDiskSpace();
		if (RUNNING_DISK.longValue() <= 10 * 1024) {
			if (!System.getenv().containsKey("JALIEN_IGNORE_STORAGE")) {
				logger.log(Level.WARNING, "There is not enough space left: " + RUNNING_DISK + " MB");
				return false;
			}

			logger.log(Level.INFO, "Ignoring the reported local disk space of " + RUNNING_DISK + " MB");
		}

		if (RUNNING_CPU.longValue() <= 0)
			return false;

		return true;
	}

	/**
	 * Re-computes the disk space available
	 *
	 * @return the amount of usable disk space left, in MB
	 */
	public Long recomputeDiskSpace() {
		long recomputedDisk = getFreeSpace((String) siteMap.get("workdir"));
		logger.log(Level.INFO, "Recomputing disk space of " + (String) siteMap.get("workdir") + ". Starting with a free space of " + recomputedDisk);
		synchronized (workDirSizeSync) {
			for (Long runningJob : slotWorkdirsMaxSize.keySet()) {
				long maxSize = slotWorkdirsMaxSize.get(runningJob).longValue() * 1024 * 1024;
				String runningJobWorkdir = (String) siteMap.get("workdir") + "/" + defaultOutputDirPrefix + runningJob;

				Path workdirPath = Paths.get(runningJobWorkdir);
				long workdirSize = 0;
				try {
					workdirSize = Files.walk(workdirPath)
							.filter(p -> p.toFile().isFile())
							.mapToLong(p -> p.toFile().length())
							.sum();
				}
				catch (IOException e) {
					logger.log(Level.INFO, "Could not compute current size of job workdir " + runningJobWorkdir, e);
				}

				recomputedDisk = recomputedDisk + workdirSize - maxSize;
				logger.log(Level.INFO, "WorkdirSize=" + workdirSize + ", maxSize=" + maxSize + ", recomputedDisk=" + recomputedDisk);
			}
		}
		return Long.valueOf(recomputedDisk > 0 ? recomputedDisk / 1024 / 1024 : 0);
	}

	/**
	 * Checks if the workload is already constrained to run in certain cores. If not in whole-node scenario, CPU cores are selected and workload is pinned.
	 *
	 * @param alreadyIsol Is the JR already isolated
	 * @param jobRunnerPid Process ID of the running JobRunner
	 */
	public boolean checkAndApplyIsolation(int jobRunnerPid, boolean alreadyIsol) {
		boolean tmpIsol = alreadyIsol;
		synchronized (cpuSync) {
			byte[] hostMask = getHostMask();
			for (int i = 0; i < RES_NOCPUS.intValue(); i++) {
				if (hostMask[i] != 0) {
					tmpIsol = true;
					break;
				}
			}

			if (wholeNode == false && tmpIsol == false && (!CgroupUtils.haveCgroupsv2() ||  (CgroupUtils.haveCgroupsv2() && !CgroupUtils.hasController(CgroupUtils.getCurrentCgroup(jobRunnerPid),"cpu")))) {
				logger.log(Level.INFO, "Applying isolation to the whole JobRunner CPU allocation - Allocation of " + RUNNING_CPU + " cores");
				String initMask = numaExplorer.computeInitialMask(RUNNING_CPU, wholeNode);
				NUMAExplorer.applyTaskset(initMask, jobRunnerPid);
				logger.log(Level.INFO, "JobRunner pinned to mask " + initMask);
				tmpIsol = true;
			}
		}
		return tmpIsol;
	}

	private boolean checkOOMDump() {
		File f = new File(jobWorkdir + "/" + queueId + ".oom");
		if (f.exists() && !f.isDirectory()) {
			logger.log(Level.SEVERE, "Detected an OOM on the JobWrapper JVM. Aborting.");
			putJobTrace("Detected an OOM on the JobWrapper JVM. Aborting.");
			return true;
		}
		return false;
	}

	private int computeTimeLeft(final Level loggingLevel) {
		final long jobAgentCurrentTime = System.currentTimeMillis();
		final int time_subs = (int) (jobAgentCurrentTime - jobAgentStartTime) / 1000; // convert to seconds
		int timeleft = origTtl - time_subs;

		logger.log(loggingLevel, "Still have " + timeleft + " seconds to live (" + jobAgentCurrentTime + "-" + jobAgentStartTime + "=" + time_subs + ")");

		// we check if the cert timeleft is smaller than the ttl itself
		final int certTime = getCertTime();
		logger.log(loggingLevel, "Certificate timeleft is " + certTime);
		timeleft = Math.min(timeleft, certTime - 900); // (-15min)

		// safety time for saving, etc
		timeleft -= 600;

		Long shutdownTime = MachineJobFeatures.getFeatureNumber("shutdowntime", MachineJobFeatures.FeatureType.MACHINEFEATURE);

		if (shutdownTime != null) {
			shutdownTime = Long.valueOf(shutdownTime.longValue() - System.currentTimeMillis() / 1000);
			logger.log(loggingLevel, "Shutdown is" + shutdownTime);

			timeleft = Integer.min(timeleft, shutdownTime.intValue());
		}

		return timeleft;
	}

	private boolean updateDynamicParameters() {
		logger.log(Level.INFO, "Updating dynamic parameters of jobAgent map");

		// ttl recalculation
		final int timeleft = computeTimeLeft(Level.INFO);

		if (!checkParameters())
			return false;

		siteMap.put("TTL", Integer.valueOf(timeleft));
		siteMap.put("CPUCores", RUNNING_CPU);
		// matching is done in KB (see JOBAGENT.disk column)
		siteMap.put("Disk", Long.valueOf(RUNNING_DISK.longValue() * 1024));

		final int cvmfsRevision = CVMFS.getRevision();
		if (cvmfsRevision > 0)
			siteMap.put("CVMFS_revision", Integer.valueOf(cvmfsRevision));

		return true;
	}

	/**
	 * @return the time in seconds that the certificate is still valid for
	 */
	private int getCertTime() {
		return (int) TimeUnit.MILLISECONDS.toSeconds(commander.getUser().getUserCert()[0].getNotAfter().getTime() - System.currentTimeMillis());
	}

	private void getMemoryRequirements() {
		// By default the jobs are allowed to use up to 10GB of disk space in the sandbox

		cpuCores = TaskQueueUtils.getCPUCores(jdl);
		putJobTrace("Job requested " + cpuCores + " CPU cores to run");

		workdirMaxSizeMB = TaskQueueUtils.getWorkDirSizeMB(jdl, cpuCores);
		putJobTrace("Local disk space limit: " + workdirMaxSizeMB + " MB");
		synchronized (workDirSizeSync) {
			slotWorkdirsMaxSize.put(Long.valueOf(queueId), Long.valueOf(workdirMaxSizeMB));
		}

		// Memory use
		final String maxmemory = jdl.gets("Memorysize");

		// By default the job is limited to using 8GB of virtual memory per allocated CPU core
		jobMaxMemoryMB = cpuCores * 8 * 1024;

		if (env.containsKey("JALIEN_MEM_LIM")) {
			try {
				jobMaxMemoryMB = Integer.parseInt(env.get("JALIEN_MEM_LIM"));
			}
			catch (final NumberFormatException en) {
				final String error = "Could not read limit from JALIEN_MEM_LIM. Using default: " + jobMaxMemoryMB + "MB";
				logger.log(Level.WARNING, error, en);
				putJobTrace(error);
			}
		}
		else if (maxmemory != null) {
			final Pattern pLetter = Pattern.compile("\\p{L}+");

			final Matcher m = pLetter.matcher(maxmemory.trim().toUpperCase());
			try {
				if (m.find()) {
					final String number = maxmemory.substring(0, m.start());
					final String unit = maxmemory.substring(m.start()).toUpperCase();

					jobMaxMemoryMB = TaskQueueUtils.convertStringUnitToIntegerMB(unit, number);
				}
				else
					jobMaxMemoryMB = Integer.parseInt(maxmemory);

				putJobTrace("Virtual memory limit (JDL): " + jobMaxMemoryMB + "MB");
			}
			catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
				putJobTrace("Virtual memory limit specs are invalid: '" + maxmemory + "', using the default " + jobMaxMemoryMB + "MB");
			}
		}
		else
			putJobTrace("Virtual memory limit (default): " + jobMaxMemoryMB + "MB");
	}

	private void setStatus(final jaStatus new_status) {
		if (new_status != status) {
			if (status != null)
				monitor.sendParameter("ja_status_" + status.getValue(), Integer.valueOf(0));

			status = new_status;

			if (status != null) {
				monitor.sendParameter("ja_status_string", status.getStringValue());
				monitor.sendParameter("ja_status_" + status.getValue(), Integer.valueOf(1));
				monitor.sendParameter("ja_status", Integer.valueOf(status.getValue()));
			}
		}
	}

	private void setUsedCores(final int jobNumber) {
		if (reqCPU.longValue() > 0)
			monitor.sendParameter(reqCPU + "_cores_jobs", Integer.valueOf(jobNumber));
		if (jobNumber == 0)
			reqCPU = Long.valueOf(0);
		monitor.sendParameter("num_cores", reqCPU);
	}

	private void sendProcessResources(boolean finalReporting) {
		// runtime(date formatted) start cpu(%) mem cputime rsz vsize ncpu
		// cpufamily cpuspeed resourcecost maxrss maxvss ksi2k
		if (finalReporting)
			getFinalCPUUsage();

		final String procinfo = String.format("%s %d %.2f %.2f %.2f %.2f %.2f %d %s %s %s %.2f %.2f 1000", RES_FRUNTIME, RES_RUNTIME, RES_CPUUSAGE, RES_MEMUSAGE, RES_CPUTIME, RES_RMEM, RES_VMEM,
				RES_NOCPUS, RES_CPUFAMILY, RES_CPUMHZ, RES_RESOURCEUSAGE, RES_RMEMMAX, RES_VMEMMAX);
		logger.log(Level.INFO, "+++++ Sending resources info +++++");
		logger.log(Level.INFO, procinfo);

		putJobLog("proc", procinfo);

		if (finalReporting) {
			HashMap<String, Object> extrafields = new HashMap<>();
			extrafields.put("maxrsize", RES_RMEMMAX);
			extrafields.put("cputime", RES_CPUTIME);
			extrafields.put("cost", Float.valueOf(RES_RUNTIME.floatValue() * jobPrice.floatValue())); // walltime * price = realtime * nr_cores * price
			extrafields.put("si2k", "1"); // TODO - speedup factor (to be updated)
			extrafields.put("runtimes", RES_RUNTIME);
			extrafields.put("maxvsize", RES_VMEMMAX);
			extrafields.put("batchid", RES_BATCH_INFO); // TODO - send from sendBatchInfo();
			extrafields.put("CE", ce);
			extrafields.put("node", hostName);
			extrafields.put("killreason", killreason);

			if (!TaskQueueApiUtils.setJobStatus(queueId, resubmission, null, extrafields)) {
				jobKilled = true;
				logger.log(Level.INFO, "Could not send accounting data");
			}
		}

		lastHeartbeat = System.currentTimeMillis();
	}

	private void getFinalCPUUsage() {
		double totalCPUTime = getTotalCPUTime("execution") + getTotalCPUTime("validation");
		if (totalCPUTime > RES_CPUTIME.doubleValue())
			RES_CPUTIME = Double.valueOf(totalCPUTime);
		if (RES_RUNTIME.doubleValue() > 0)
			RES_CPUUSAGE = Double.valueOf((RES_CPUTIME.doubleValue() / RES_RUNTIME.doubleValue()) * 100);
		else
			RES_CPUUSAGE = Double.valueOf(0);
		logger.log(Level.INFO, "The last CPU time, computed as real+user time is " + RES_CPUTIME + ". Given that the job's wall time is " + RES_RUNTIME + ", the CPU usage is " + RES_CPUUSAGE);
	}

	private double getTotalCPUTime(String executionType) {
		String timeFile = tempDir + "/tmp/.jalienTimes-" + executionType;
		double cpuTime = 0;
		try (BufferedReader br = new BufferedReader(new FileReader(timeFile))) {
			String line;
			ArrayList<String> fields = new ArrayList<>();
			while ((line = br.readLine()) != null) {
				fields.add(line.split(" ")[0]);
				if (line.startsWith("sys") || line.startsWith("user")) {
					try {
						float time = Float.parseFloat(line.split(" ")[1]);
						cpuTime = cpuTime + time;
					}
					catch (NumberFormatException | IndexOutOfBoundsException e) {
						logger.log(Level.WARNING, "The file " + timeFile + " did not have the expected `time` format. \n" + e);
					}
				}
			}
			if (fields.size() != 3 || !fields.contains("real") || !fields.contains("user") || !fields.contains("sys"))
				logger.log(Level.WARNING, "The file " + timeFile + " did not have the expected `time` format. Expected to have real,user,sys fields");
		}
		catch (IOException e) {
			logger.log(Level.WARNING, "The file " + timeFile + " could not be found. \n" + e);
		}
		return cpuTime;
	}

	private boolean jobKilled = false;

	private boolean jobOOMPreempted = false;

	/**
	 * Records job preemption in the central db
	 *
	 * @param preemptionTs
	 * @param preemptionSlotMemory
	 * @param preemptionJobMemory
	 * @param numConcurrentJobs
	 * @param wouldPreempt
	 * @return
	 */
	protected boolean recordPreemption(final long preemptionTs, final double preemptionSlotMemory, final double PreemptionSlotMemsw, final double preemptionJobMemory, final String reason,
			final double parsedSlotLimit, final int numConcurrentJobs, long wouldPreempt) {
		/*
		 * synchronized(MemoryController.lockMemoryController) {
		 * logger.log(Level.INFO, "Preemption of job " + queueId + " (site: " + ce + " - host: " + hostName + ") starts - going to be killed EVENTUALLY");
		 * MemoryController.preemptingJob = true;
		 * }
		 */
		// if (!alreadyPreempted) {
		logger.log(Level.INFO, "Recording preemption of job " + wouldPreempt + " (data from co-executor " + queueId + ")");
		double memoryPerCore = Math.round(preemptionJobMemory / cpuCores * 100.0) / 100.0;
		double memHardLimitRounded = Math.round(MemoryController.memHardLimit / 1024 * 100.0) / 100.0;
		double memswHardLimitRounded = Math.round(MemoryController.memswHardLimit / 1024 * 100.0) / 100.0;
		Double growthDerivative = MemoryController.derivativePerJob.getOrDefault(Long.valueOf(queueId), Double.valueOf(0d));

		double elapsedTime = (System.currentTimeMillis() - jobAgentThreadStartTime) / 1000.; // convert to seconds
		double timePortion = elapsedTime / ttl;
		if (MemoryController.debugMemoryController)
			logger.log(Level.INFO, "Have a time elapsed of " + elapsedTime + ". With ttl of " + ttl + ", time portion is " + timePortion);
		String cgroupPath = MemoryController.cgroupId;
		if (CgroupUtils.haveCgroupsv2())
			cgroupPath = CgroupUtils.getCurrentCgroup(getWrapperPid());
		if (!commander.q_api.recordPreemption(queueId, preemptionTs, 0, preemptionSlotMemory / 1024, PreemptionSlotMemsw / 1024, preemptionJobMemory, numConcurrentJobs, resubmission, hostName, ce,
				memoryPerCore, growthDerivative.doubleValue(), timePortion, username, MemoryController.preemptionRound, wouldPreempt, memHardLimitRounded, memswHardLimitRounded, "", cgroupPath, 0d,
				0d)) {
			logger.log(Level.SEVERE, "Preemption could not be recorded in the database");
			return false;
		}
		if (queueId == wouldPreempt) {
			putJobTrace("Preemption starts. Job consuming " + Format.point(preemptionJobMemory) + " MB (slot has a total usage of " + Format.point(preemptionSlotMemory / 1024)
					+ " MB, parsed limit of " + parsedSlotLimit + " MB due to " + reason + ").");
			alreadyPreempted = true;
			String cgroupPIDs = "/sys/fs/cgroup/memory" + MemoryController.cgroupId + "/tasks";
			if (CgroupUtils.haveCgroupsv2())
				cgroupPIDs = CgroupUtils.getCurrentCgroup(childPID) + "/cgroup.procs";
			if (MemoryController.debugMemoryController)
				logger.log(Level.INFO, "Sorting processes by consumed memory from source " + cgroupPIDs);
			// List<String> processTree = MemoryController.getCgroupProcessTree(cgroupPIDs);
			// putJobTrace("Appending cgroup processes memory consumption for dbg (PSS --> process cmd) -- Please ignore colors\n -------------");
			// for (String p : processTree)
			// putJobTrace(p);
			// putJobTrace("------------");
		}
		// }
		// jobOOMPreempted = true;
		return true;
	}

	private boolean putJobLog(final String key, final String value) {
		if (jobKilled)
			return false;

		if (!commander.q_api.putJobLog(queueId, resubmission, key, value)) {
			jobKilled = true;
			return false;
		}

		return true;
	}

	protected boolean putJobTrace(final String value) {
		return putJobLog("trace", value);
	}

	protected String checkProcessResources() { // checks and maintains sandbox
		if (jobKilled) {
			killreason = 30;
			return "Job was killed";
		}

		if (jobOOMPreempted) {
			killreason = 40;
			return "Job was preempted due to memory overconsumption";
		}

		lowCpuUsageCounter = (RES_CPUTIME - prevCpuTime) < ((CHECK_RESOURCES_INTERVAL / 1000.0 / 20.0) * Double.valueOf(cpuCores)) ? lowCpuUsageCounter += 1 : 0;
		if (lowCpuUsageCounter > ((15 * 60 * 1000) / CHECK_RESOURCES_INTERVAL) && "RUNNING".equals(getWrapperJobStatus())) {
			killreason = 50;
			return "CPU time consumed by the payload has been near zero for the last 15 minutes. Aborting";
		}
		prevCpuTime = RES_CPUTIME;

		// Also check for core directories, and abort if found to avoid filling up disk space
		if (!env.getOrDefault("SKIP_CORECHECK", "").toLowerCase().contains("true")) {
			try {
				final String coreDir = checkForCoreDirectories(checkCoreUsingJava);
				if (coreDir != null) {
					killreason = 60;
					return "Core directory detected: " + coreDir + ". Aborting!";
				}
			}
			catch (final Exception e1) {
				logger.log(Level.WARNING, "Exception while checking for core directories: ", e1);
				logger.log(Level.INFO, "Switching to core check using shell instead");

				checkCoreUsingJava = false;
			}
		}

		String error = null;
		// logger.log(Level.INFO, "Checking resources usage");

		try {

			final HashMap<Long, Double> jobinfo = mj.readJobInfo();

			final HashMap<Long, Double> diskinfo = mj.readJobDiskUsage();

			if (jobinfo == null || diskinfo == null) {

				logger.log(Level.WARNING, "JobInfo or DiskInfo monitor null");
				// return "Not available"; TODO: Adjust and put back again
			}

			if (!mj.getErrorLogs().isEmpty())
				putJobLog("proc", mj.getErrorLogs());

			// getting cpu, memory and runtime info
			if (diskinfo != null)
				RES_WORKDIR_SIZE = diskinfo.get(ApMonMonitoringConstants.LJOB_WORKDIR_SIZE);

			if (jobinfo != null) {
				RES_RMEM = Double.valueOf(jobinfo.get(ApMonMonitoringConstants.LJOB_PSS).doubleValue() / 1024);
				RES_VMEM = Double.valueOf(jobinfo.get(ApMonMonitoringConstants.LJOB_SWAPPSS).doubleValue() / 1024 + RES_RMEM.doubleValue());

				RES_CPUTIME = jobinfo.get(ApMonMonitoringConstants.LJOB_CPU_TIME);
				RES_CPUUSAGE = jobinfo.get(ApMonMonitoringConstants.LJOB_CPU_USAGE);
				RES_RUNTIME = Long.valueOf(jobinfo.get(ApMonMonitoringConstants.LJOB_RUN_TIME).longValue());
				RES_MEMUSAGE = jobinfo.get(ApMonMonitoringConstants.LJOB_MEM_USAGE);
			}

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
				RES_FRUNTIME = String.format("00:00:%02d", RES_RUNTIME);
			else if (RES_RUNTIME.longValue() < 3600)
				RES_FRUNTIME = String.format("00:%02d:%02d", Long.valueOf(RES_RUNTIME.longValue() / 60), Long.valueOf(RES_RUNTIME.longValue() % 60));
			else
				RES_FRUNTIME = String.format("%02d:%02d:%02d", Long.valueOf(RES_RUNTIME.longValue() / 3600), Long.valueOf((RES_RUNTIME.longValue() - (RES_RUNTIME.longValue() / 3600) * 3600) / 60),
						Long.valueOf((RES_RUNTIME.longValue() - (RES_RUNTIME.longValue() / 3600) * 3600) % 60));

			// check disk usage
			if (workdirMaxSizeMB != 0 && RES_WORKDIR_SIZE.doubleValue() > workdirMaxSizeMB) {
				killreason = 70;
				error = "Killing the job (using more than " + workdirMaxSizeMB + "MB of diskspace (right now we were using " + RES_WORKDIR_SIZE + "))";
			}

			// check memory usage (with 20% buffer)
			if (jobMaxMemoryMB != 0 && RES_VMEM.doubleValue() > jobMaxMemoryMB * 1.2) {
				killreason = 80;
				error = "Killing the job (using more than " + jobMaxMemoryMB + " MB memory (right now ~" + Math.round(RES_VMEM.doubleValue()) + "MB))";
			}
		}
		catch (final IOException e) {
			logger.log(Level.WARNING, "Problem with the monitoring objects: " + e.toString());
		}
		catch (final NoSuchElementException e) {
			logger.log(Level.WARNING, "Warning: an error occurred reading monitoring data:  " + e.toString());
		}
		catch (final NullPointerException e) {
			logger.log(Level.WARNING, "JobInfo or DiskInfo monitor are now null. Did the JobWrapper terminate?: " + e.toString());
		}
		catch (final NumberFormatException e) {
			logger.log(Level.WARNING, "Unable to continue monitoring: " + e.toString());
		}
		catch (final ConcurrentModificationException e) {
			logger.log(Level.WARNING, "Warning: an error occurred reading monitoring data:  " + e.toString());
		}

		return error;
	}

	private void sendBatchInfo() {
		for (final String var : batchSystemVars) {
			if (env.containsKey(var)) {
				if ("_CONDOR_JOB_AD".equals(var)) {
					try {
						final List<String> lines = Files.readAllLines(Paths.get(env.get(var)));
						for (final String line : lines) {
							if (line.contains("GlobalJobId")) {
								String traceLine = "BatchId " + line;
								putJobTrace(traceLine);
								RES_BATCH_INFO += traceLine + ";";
							}
						}
					}
					catch (final IOException e) {
						logger.log(Level.WARNING, "Error getting batch info from file " + env.get(var) + ":", e);
					}
				}
				else {
					String traceLine = "BatchId " + var + ": " + env.get(var);
					putJobTrace(traceLine);
					RES_BATCH_INFO += traceLine + ";";
				}
			}
		}

		HashMap<String, Object> extrafields = new HashMap<>(Map.of("batchid", RES_BATCH_INFO));
		if (!TaskQueueApiUtils.setJobStatus(queueId, resubmission, null, extrafields)) {
			jobKilled = true;
			logger.log(Level.INFO, "Could not send batchid for accounting");
		}
	}

	// ##########################################################################
	// ######################### OTHER HELPER FUNCTIONS #########################
	// ##########################################################################

	/*
	 * private void constrainJobCPU() {
	 * //get CPU cores to constrain the job
	 * String isolCmd = addIsolation();
	 * logger.log(Level.SEVERE, "IsolCmd command" + isolCmd);
	 * NUMAExplorer.applyTaskset(isolCmd, childPID);
	 * }
	 */

	private long ttlForJob() {
		final Integer iTTL = jdl.getInteger("TTL");

		int jobTtl = (iTTL != null ? iTTL.intValue() : 3600);
		putJobTrace("Job asks for a TTL of " + jobTtl + " seconds");
		jobTtl += 300; // extra time (saving)

		final String proxyttl = jdl.gets("ProxyTTL");
		if (proxyttl != null) {
			jobTtl = ((Integer) siteMap.get("TTL")).intValue() - 600;
			putJobTrace("ProxyTTL enabled, running for " + jobTtl + " seconds");
		}

		return jobTtl;
	}

	/**
	 * @param folder
	 * @return amount of free space (in bytes) in the given folder. Or zero if there was a problem (or no free space).
	 */
	public static long getFreeSpace(final String folder) {
		final File folderFile = new File(Functions.resolvePathWithEnv(folder));

		try {
			if (!folderFile.exists())
				folderFile.mkdirs();
		}
		catch (@SuppressWarnings("unused") Exception e) {
			// ignore
		}

		long space = folderFile.getUsableSpace();
		if (space <= 0) {
			// 32b JRE returns 0 when too much space is available

			try {
				final String output = ExternalProcesses.getCmdOutput(Arrays.asList("df", "-P", "-B", "1", folder), true, 30L, TimeUnit.SECONDS);

				try (BufferedReader br = new BufferedReader(new StringReader(output))) {
					String sLine = br.readLine();

					if (sLine != null) {
						sLine = br.readLine();

						if (sLine != null) {
							final StringTokenizer st = new StringTokenizer(sLine);

							st.nextToken();
							st.nextToken();
							st.nextToken();

							space = Long.parseLong(st.nextToken());
						}
					}
				}
			}
			catch (IOException | InterruptedException ioe) {
				System.out.println("Could not extract the space information from `df`: " + ioe.getMessage());
			}
		}

		return space;
	}

	/**
	 * @return job ID being processed
	 */
	public long getQueueId() {
		return this.queueId;
	}

	/**
	 * @return resubmission counter
	 */
	public int getResubmission() {
		return this.resubmission;
	}

	/**
	 * @return payload process ID
	 */
	public int getChildPID() {
		return this.childPID;
	}

	private boolean createWorkDir() {
		logger.log(Level.INFO, "Creating sandbox directory");

		jobWorkdir = String.format("%s%s%d", workdir, defaultOutputDirPrefix, Long.valueOf(queueId));

		tempDir = new File(jobWorkdir);
		if (!tempDir.exists()) {
			final boolean created = tempDir.mkdirs();
			if (!created) {
				logger.log(Level.INFO, "Workdir does not exist and can't be created: " + jobWorkdir);

				setStatus(jaStatus.ERROR_DIRS);

				setUsedCores(0);

				return false;
			}
		}

		jobTmpDir = new File(jobWorkdir + "/tmp");

		if (!jobTmpDir.exists() && !jobTmpDir.mkdir()) {
			logger.log(Level.WARNING, "Cannot create missing tmp dir " + jobTmpDir.getAbsolutePath());
		}

		putJobTrace("Created workdir: " + jobWorkdir);

		// chdir
		System.setProperty("user.dir", jobWorkdir);

		return true;
	}

	private void setupJobWrapperLogging() {
		Properties props = new Properties();
		try {
			final ExtProperties ep = ConfigUtils.getConfiguration("logging");

			props = ep.getProperties();

			logger.log(Level.INFO, "Logging properties loaded for the JobWrapper");
		}
		catch (@SuppressWarnings("unused") final Exception e) {
			// JA doesn't have any logging defined, thus will log to stderr. JW inherits stderr and will log to it as well.
			logger.log(Level.INFO, "Logging properties for JobWrapper not found.");
			logger.log(Level.INFO, "Using fallback logging configurations for JobWrapper");

			props.put("handlers", "java.util.logging.ConsoleHandler");
			props.put("java.util.logging.SimpleFormatter.format", "JobID " + queueId + ": %1$tb %1$td, %1$tY %1$tH:%1$tM:%1$tS %2$s %n%4$s: %5$s%6$s%n");
			props.put(".level", "INFO");
			props.put("lia.level", "WARNING");
			props.put("lazyj.level", "WARNING");
			props.put("apmon.level", "WARNING");
			props.put("alien.level", "FINE");
			props.put("alien.api.DispatchSSLClient.level", "INFO");
			props.put("alien.monitoring.Monitor.level", "WARNING");
			props.put("org.apache.tomcat.util.level", "WARNING");
			props.put("use_java_logger", "true");
		}

		try (FileOutputStream str = new FileOutputStream(jobWorkdir + "/logging.properties")) {
			props.store(str, null);
		}
		catch (final IOException e1) {
			logger.log(Level.WARNING, "Failed to configure JobWrapper logging", e1);
		}
	}

	private boolean changeJobStatus(final JobStatus newStatus, final int exitCode) {
		final HashMap<String, Object> extrafields = new HashMap<>();
		extrafields.put("exechost", siteMap.getOrDefault("CEhost", ""));
		extrafields.put("node", hostName);
		extrafields.put("CE", siteMap.getOrDefault("CE", ""));

		if (jobWorkdir != null)
			extrafields.put("path", jobWorkdir);

		if (exitCode != 0)
			extrafields.put("error", Integer.valueOf(exitCode));

		if (!TaskQueueApiUtils.setJobStatus(queueId, resubmission, newStatus, extrafields)) {
			jobKilled = true;
			return false;
		}

		if (apmon != null)
			try {
				apmon.sendParameter(ce + "_Jobs", String.valueOf(queueId), "status", newStatus.getAliEnLevel());
			}
			catch (final Exception e) {
				logger.log(Level.WARNING, "JA cannot update ML of the job status change", e);
			}

		return true;
	}

	private String getWrapperJobStatus() {
		try {
			return Files.readString(Paths.get(jobTmpDir + "/" + jobstatusFile));
		}
		catch (final IOException e) {
			logger.log(Level.WARNING, "Attempt to read job status failed. Ignoring: " + e.toString());
			return "";
		}
	}

	private long getWrapperJobStatusTimestamp() {
		return new File(jobTmpDir + "/" + jobstatusFile).lastModified();
	}

	/**
	 *
	 * Identifies the JobWrapper in list of child PIDs
	 * (these may be shifted when using containers)
	 *
	 * @return JobWrapper PID
	 */
	private int getWrapperPid() {
		final ArrayList<Integer> wrapperProcs = new ArrayList<>();

		final CommandOutput output = SystemCommand.executeCommand(List.of("pgrep", "-f", "vmid=" + String.valueOf(queueId)), false);

		if (output.exitCode != 0) {
			// pgrep failed
		}

		try (BufferedReader br = output.reader()) {
			String line;

			while ((line = br.readLine()) != null) {
				try {
					wrapperProcs.add(Integer.valueOf(line.trim()));
				}
				catch (@SuppressWarnings("unused") NumberFormatException nfe) {
					logger.log(Level.WARNING, "Unexpected string in the output of `pgrep` : " + line);
				}
			}
		}
		catch (@SuppressWarnings("unused") final IOException ioe) {
			// not happening, it's a BR on a memory String
		}

		if (wrapperProcs.size() < 1)
			return 0;

		return wrapperProcs.get(wrapperProcs.size() - 1).intValue(); // may have a first entry coming from the env init in container. Ignore if present
	}

	private final Object notificationEndpoint = new Object();

	// TODO call this method when there is a positive notification that the JW has exited. Should only be called _after_ the process has exited, otherwise the checks are still done.
	void notifyOnJWCompletion() {
		synchronized (notificationEndpoint) {
			notificationEndpoint.notifyAll();
		}
	}

	/**
	 *
	 * Checks for the presence of core directories, either through Java or shell
	 *
	 * @param checkUsingJava will use shell instead of Java on false
	 * @return name of dir if found, and null otherwise
	 * @throws IOException for errors using Java check
	 */
	private final String checkForCoreDirectories(boolean checkUsingJava) throws IOException {
		if (checkUsingJava) {
			final Pattern coreDirPattern = Pattern.compile("^core(?!.*\\.inp$).*$");
			List<File> coreDirs = Files.walk(new File(jobWorkdir).toPath())
					.map(Path::toFile)
					.filter(file -> coreDirPattern.matcher(file.getName()).matches())
					.collect(Collectors.toList());

			if (coreDirs != null && coreDirs.size() != 0)
				return coreDirs.get(0).getName();
		}
		else {
			String cmd = "find -name 'core*' ! -name '*.inp'";
			CommandOutput output = SystemCommand.bash(cmd, true);

			try (BufferedReader br = output.reader()) {
				String readArg = br.readLine();
				if (readArg != null && !readArg.isBlank() && readArg.contains("core"))
					return output.toString();
			}
			catch (Exception e2) {
				logger.log(Level.WARNING, "Exception while checking for core directories using shell: ", e2);
			}
		}
		return null;
	}

	/**
	 *
	 * Gracefully kills the JobWrapper and its payload, with a one-hour window for upload
	 *
	 * @param p process for JobWrapper
	 */
	private void killGracefully(final Process p) {
		try {
			final int jobWrapperPid = getWrapperPid();
			if (jobWrapperPid != 0)
				Runtime.getRuntime().exec(new String[] { "kill", String.valueOf(jobWrapperPid) });
			else
				logger.log(Level.INFO, "Could not kill JobWrapper: not found. Already done?");
		}
		catch (final Exception e) {
			logger.log(Level.INFO, "Unable to kill the JobWrapper", e);
		}

		// Give the JW up to an hour to clean things up
		final long deadLine = System.currentTimeMillis() + 1000L * 60 * 60;

		synchronized (notificationEndpoint) {
			while (p.isAlive() && System.currentTimeMillis() < deadLine) {
				try {
					notificationEndpoint.wait(1000 * 5);
				}
				catch (final InterruptedException e) {
					logger.log(Level.WARNING, "I was interrupted while waiting for the payload to clean up", e);
					break;
				}
			}
		}

		// If still alive, kill everything, including the JW
		if (p.isAlive()) {
			killForcibly(p);
		}
	}

	/**
	 *
	 * Immediately kills the JobWrapper and its payload, without giving time for upload
	 *
	 * @param p process for JobWrapper
	 */
	private void killForcibly(final Process p) {
		final int jobWrapperPid = getWrapperPid();
		try {
			if (jobWrapperPid != 0) {
				JobWrapper.cleanupProcesses(queueId, jobWrapperPid);
				Runtime.getRuntime().exec(new String[] { "kill", "-9", String.valueOf(jobWrapperPid) });
			}
			else
				logger.log(Level.INFO, "Could not kill JobWrapper: not found. Already done?");
		}
		catch (final Exception e) {
			logger.log(Level.INFO, "Unable to kill the JobWrapper", e);
		}

		if (p.isAlive()) {
			p.destroyForcibly();
		}
	}

	private final static String[] batchSystemVars = {
			"CONDOR_PARENT_ID",
			"_CONDOR_JOB_AD",
			"SLURM_JOBID",
			"SLURM_JOB_ID",
			"LSB_BATCH_JID",
			"LSB_JOBID",
			"PBS_JOBID",
			"PBS_JOBNAME",
			"CREAM_JOBID",
			"GRID_GLOBAL_JOBID",
			"JOB_ID"
	};

	/**
	 *
	 * Reads the list of variables defined in META_VARIABLES, and returns their current value in the
	 * environment as a map.
	 *
	 * Used for propagating any additional environment variables to container/payload.
	 *
	 * @return map of env variables defined in META_VARIABLES and their current value.
	 */
	private static Map<String, String> getMetaVariables() {
		String metavars = env.getOrDefault("META_VARIABLES", "");

		List<String> metavars_list = Arrays.asList(metavars.split("\\s*,\\s*"));
		System.err.println("Detected metavars: " + metavars_list.toString());

		Map<String, String> metavars_map = new HashMap<>();
		for (final String var : metavars_list)
			metavars_map.put(var, env.getOrDefault(var, ""));

		return metavars_map;
	}

	/**
	 *
	 * Thread for checking if heartbeats are still being sent, or if something is stuck
	 *
	 * @param p process for JobWrapper/Job
	 * @return heartbeatMonitor runnable
	 */
	private final Runnable heartbeatMonitor(Process p) {
		return () -> {
			while (p.isAlive()) {
				if (System.currentTimeMillis() - lastHeartbeat > 900000)
					putJobTrace("WARNING: Something is preventing the sending of heartbeats/resource info!");
				try {
					Thread.sleep(60 * 1000);
				}
				catch (@SuppressWarnings("unused") final InterruptedException ie) {
					break;
				}
			}
		};
	}

	// #################################################################
	// ############################ SCRIPTS ############################
	// #################################################################

	/**
	 * Site Sonar controller function
	 * Updates the Site Map with Site Sonar constraint values
	 */
	private void collectSystemInformation() {

		String nodeHostName = hostName;
		String alienSiteName = siteMap.getOrDefault("Site", "UNKNOWN").toString();

		// PARENT_HOSTNAME variable is used at sites which the node hostname changes over time. eg: RAL
		if (env.containsKey("PARENT_HOSTNAME")) {
			nodeHostName = env.get("PARENT_HOSTNAME");
		}

		// Birmingham site adds a number at the end of actual hostname. eg: "hostname-25.ph.bham.ac.uk"
		// Following filter is added to obtain the actual hostname from the given hostname variable.
		if (!nodeHostName.isEmpty() && !alienSiteName.isEmpty()) {
			nodeHostName = nodeHostName.replaceAll("-[0-9]*\\(.ph.bham.ac.uk\\)$", ".ph.bham.ac.uk");
		}
		else {
			// Returning because the hostname or CE name is null
			logger.log(Level.INFO, ("Site Sonar execution is skipped because hostname or CE name is null"));
			return;
		}

		logger.log(Level.INFO, "Getting probe list from Site Sonar for : " + nodeHostName);
		JSONObject probeOutput = getProbes(nodeHostName, alienSiteName);
		if (!probeOutput.isEmpty()) {
			JSONArray probeList = (JSONArray) probeOutput.get("probes");
			if (probeList.size() > 0) {
				logger.log(Level.INFO, ("===== Running following probes on " + nodeHostName + " at " + alienSiteName + " ====="));
				for (int i = 0; i < probeList.size(); i++) {
					String testName = (String) probeList.get(i);
					JSONObject testOutput = runProbe(testName);

					if (testOutput != null)
						uploadResults(nodeHostName, alienSiteName, testName, testOutput);
				}
			}
			else {
				logger.log(Level.INFO, ("No probes returned from Site Sonar. Skipping ..."));
			}
		}
		else {
			logger.log(Level.SEVERE, "Site Sonar probe output for node " + hostName + " in " +
					alienSiteName + " is null");
		}
		addConstraintsToSiteMap(nodeHostName, alienSiteName);
	}

	/**
	 * Make HTTP request and return JSON output
	 *
	 * @param url Request URI
	 * @param nodeName Hostname
	 * @param alienSite
	 * @return Request output
	 */
	protected static JSONObject makeRequest(URL url, String nodeName, String alienSite, Logger logg) {
		try {
			logg.log(Level.FINE, "Making HTTP call to " + url + " from " + nodeName + " in " +
					alienSite);
			final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setConnectTimeout(5000);
			conn.setReadTimeout(120000);

			try (InputStream inputStream = conn.getInputStream()) {
				byte[] buffer = inputStream.readAllBytes();
				String output = new String(buffer, StandardCharsets.UTF_8);
				if (!output.isBlank()) {
					return (JSONObject) jsonParser.parse(output);
				}
			}
			catch (final ParseException e) {
				logg.log(Level.SEVERE, "Failed to parse AliMonitor response for node " + nodeName + " in " +
						alienSite, e);
			}
		}
		catch (final IOException e) {
			logg.log(Level.SEVERE, "IO Error in calling the url " + url + " for node " + nodeName + " in " +
					alienSite, e);
		}
		return new JSONObject();
	}

	/**
	 * Upload probe output to AliMonitor database
	 *
	 * @param nodeName hostname of the node
	 * @param alienSite
	 * @param siteSonarOutput Output of the probe
	 */
	private void uploadResults(String nodeName, String alienSite, String testName, JSONObject siteSonarOutput) {
		try {
			final URL url = new URL(siteSonarUrl + "uploadResults.jsp?hostname=" + URLEncoder.encode(nodeName, charSet) +
					"&ce_name=" + URLEncoder.encode(alienSite, charSet) + "&test_name=" + URLEncoder.encode(testName, charSet) +
					"&test_message=" + URLEncoder.encode(siteSonarOutput.toString(), StandardCharsets.UTF_8));
			logger.log(Level.INFO, ("Uploading Site Sonar results of " + nodeName + " to AliMonitor"));
			makeRequest(url, nodeName, alienSite, logger);
		}
		catch (final IOException e) {
			logger.log(Level.SEVERE, "Failed to upload Site sonar probe output for node " + nodeName + " in " +
					alienSite, e);
		}
	}

	/**
	 * Obtain values for additional constraints
	 *
	 * @param nodeName hostname of the node
	 * @param alienSite
	 */
	@SuppressWarnings("unchecked")
	private void addConstraintsToSiteMap(String nodeName, String alienSite) {
		try {

			final URL url = new URL(siteSonarUrl + "constraints-marta.jsp?hostname=" + URLEncoder.encode(nodeName, charSet) +
					"&ce_name=" + URLEncoder.encode(alienSite, charSet));
			JSONObject constraints = makeRequest(url, nodeName, alienSite, logger);
			if (constraints != null && constraints.keySet().size() > 0) {
				constraints.keySet().forEach(key -> {
					Object value = constraints.get(key);
					if (value != null) {
						siteMap.put((String) key, value);
						logger.log(Level.INFO, ("Added constraint - Key: " + key + ", Value: " + value.toString() + " to SiteMap"));
					}
				});
			}

		}
		catch (final IOException e) {
			logger.log(Level.SEVERE, "Failed to get Site Sonar constraints list for node " + nodeName + " in " +
					alienSite, e);
		}
	}

	/**
	 * Obtain Site sonar probes to run
	 *
	 * @param nodeName hostname of the node
	 * @param alienSite
	 * @return List of probes to be run / Results from the existing run
	 */
	private JSONObject getProbes(String nodeName, String alienSite) {
		try {
			final URL url = new URL(siteSonarUrl + "queryProbes.jsp?hostname=" + URLEncoder.encode(nodeName, charSet) +
					"&ce_name=" + URLEncoder.encode(alienSite, charSet));
			return makeRequest(url, nodeName, alienSite, logger);
		}
		catch (final IOException e) {
			logger.log(Level.SEVERE, "Failed to get Site sonar probe list for node " + nodeName + " in " +
					alienSite, e);
		}
		return new JSONObject();
	}

	/**
	 * Run site sonar probes
	 *
	 * @param probeName Test Name
	 * @return Test output
	 */
	@SuppressWarnings("unchecked")
	private JSONObject runProbe(String probeName) {
		JSONObject testOutputJson = new JSONObject();
		try {
			final String scriptPath = CVMFS.getSiteSonarProbeDirectory() + probeName + ".sh";
			final File f = new File(scriptPath);

			if (f.exists() && f.canExecute()) {
				final ProcessBuilder pBuilder = new ProcessBuilder("bash", scriptPath);
				// pBuilder.redirectError(Redirect.INHERIT);
				// pBuilder.redirectOutput(Redirect.INHERIT);
				logger.log(Level.INFO, ("Running " + probeName + ".sh..."));
				long startTime = System.currentTimeMillis();
				final Process process = pBuilder.start();

				StringBuilder output = new StringBuilder();
				BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));

				String line;
				while ((line = reader.readLine()) != null) {
					output.append(line);
				}

				final ProcessWithTimeout pTimeout = new ProcessWithTimeout(process, pBuilder);
				pTimeout.waitFor(1, TimeUnit.MINUTES);

				if (!pTimeout.exitedOk()) {
					logger.log(Level.WARNING, "Site Sonar probe " + probeName + " didn't finish in due time");
				}
				else {
					logger.log(Level.WARNING, "Output of " + probeName + ": " + output.toString());
					try {
						testOutputJson = (JSONObject) jsonParser.parse(output.toString());
					}
					catch (ParseException | ClassCastException e) {
						logger.log(Level.SEVERE, "Failed to parse the output of probe " + probeName + " in node " + hostName, e);
					}
				}
				long endTime = System.currentTimeMillis();
				long execTime = endTime - startTime;
				// Add execution time and exit code to test output
				testOutputJson.put("EXECUTION_TIME", Long.valueOf(execTime));
				testOutputJson.put("EXITCODE", Integer.valueOf(pTimeout.exitValue()));
			}
		}
		catch (final Exception e) {
			logger.log(Level.SEVERE, "Error while running the probe " + probeName + " in node" + hostName, e);
			return null;
		}
		return testOutputJson;
	}

}
