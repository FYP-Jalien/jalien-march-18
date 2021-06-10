package alien.site;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.lang.ProcessBuilder.Redirect;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Scanner;
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
import apmon.ApMon;
import apmon.ApMonException;
import apmon.ApMonMonitoringConstants;
import apmon.BkThread;
import apmon.MonitoredJob;
import lazyj.ExtProperties;
import lia.util.process.ExternalProcesses;
import utils.ProcessWithTimeout;

/**
 * Gets matched jobs, and launches JobWrapper for executing them
 */
public class JobAgent implements Runnable {

	// Variables passed through VoBox environment
	private final static Map<String, String> env = System.getenv();
	private final String ce;

	// Folders and files
	private File tempDir;
	private static final String defaultOutputDirPrefix = "/alien-job-";
	private static final String jobWrapperLogName = "jalien-jobwrapper.log";
	private String jobWorkdir;
	private String jobWrapperLogDir;

	// Job variables
	private JDL jdl;
	private long queueId;
	private String username;
	private String tokenCert;
	private String tokenKey;
	private String jobAgentId;
	private final String workdir;
	private String legacyToken;
	private HashMap<String, Object> matchedJob;
	private static HashMap<String, Object> siteMap = null;
	private int workdirMaxSizeMB;
	private int jobMaxMemoryMB;
	private MonitoredJob mj;
	private Double prevCpuTime;
	private long prevTime = 0;
	private int cpuCores = 1;

	private static AtomicInteger totalJobs = new AtomicInteger(0);
	private final int jobNumber;

	// Other
	private final String hostName;
	private final JAliEnCOMMander commander = JAliEnCOMMander.getInstance();
	private String jarPath;
	private String jarName;
	private int wrapperPID;
	private static float lhcbMarks = -1;

	private enum jaStatus {
		STARTING_JA(0, "Starting running Job Agent"), REQUESTING_JOB(1, "Asking for a job"), INSTALLING_PKGS(2, "Found a matching job"), JOB_STARTED(3,
				"Starting processing job's payload"), RUNNING_JOB(
						4, "Running job's payload"), DONE(5, "Finished running job"), FINISHING_JA(6, "Finished running Job Agent"), ERROR_IP(-1, "Error getting AliEn jar path"), ERROR_GET_JDL(-2,
								"Error getting jdl"), ERROR_DIRS(-3, "Error creating working directories"), ERROR_START(-4, "Error launching Job Wrapper to start job");

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

	// _ource monitoring vars

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
	 * Amount of free disk space in the scratch area to advertise
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

	private jaStatus status;

	/**
	 * Allow only one agent to request a job at a time
	 */
	protected static final Object requestSync = new Object();

	/**
	 * How many consecutive answers of "no job for you" we got from the broker
	 */
	protected static final AtomicInteger retries = new AtomicInteger(0);

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
			values.add(Integer.valueOf(computeTimeLeft()));

			names.add("ja_status_string");
			values.add(status.getStringValue());

			names.add("ja_status");
			values.add(Integer.valueOf(status.getValue()));
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
				RUNNING_DISK = Long.valueOf(((Number) siteMap.getOrDefault("Disk", Integer.valueOf(10 * 1024))).longValue());
				origTtl = ((Integer) siteMap.get("TTL")).intValue();
				RUNNING_JOBAGENTS = 0;

				collectSystemInformation();
			}
		}

		hostName = (String) siteMap.get("Localhost");
		// alienCm = (String) siteMap.get("alienCm");

		if (env.containsKey("ALIEN_JOBAGENT_ID"))
			jobAgentId = env.get("ALIEN_JOBAGENT_ID");
		else
			jobAgentId = Request.getVMID().toString();

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

		try {
			final File filepath = new java.io.File(JobAgent.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath());
			jarName = filepath.getName();
			jarPath = filepath.toString().replace(jarName, "");
		}
		catch (final URISyntaxException e) {
			logger.log(Level.SEVERE, "Could not obtain AliEn jar path: " + e.toString());

			setStatus(jaStatus.ERROR_IP);
		}
	}

	private static final String SITESONAR = "/cvmfs/alice.cern.ch/sitesonar/sitesonar.sh";

	private void collectSystemInformation() {
		final File f = new File(SITESONAR);

		if (f.exists() && f.canExecute()) {
			final ProcessBuilder pBuilder = new ProcessBuilder(SITESONAR);

			pBuilder.environment().put("ALIEN_JDL_CPUCORES", MAX_CPU != null ? MAX_CPU.toString() : "1");
			pBuilder.environment().put("ALIEN_SITE", siteMap.getOrDefault("Site", "UNKNOWN").toString());

			try {
				final Process p = pBuilder.start();

				if (p != null) {
					final ProcessWithTimeout ptimeout = new ProcessWithTimeout(p, pBuilder);
					ptimeout.waitFor(5, TimeUnit.MINUTES);

					if (!ptimeout.exitedOk())
						logger.log(Level.WARNING, "Sitesonar didn't finish in due time");
				}
			}
			catch (@SuppressWarnings("unused") final IOException | InterruptedException e) {
				// ignore
			}
		}
	}

	@SuppressWarnings("boxing")
	@Override
	public void run() {
		logger.log(Level.INFO, "Starting JobAgent " + jobNumber + " in " + hostName);

		logger.log(Level.INFO, siteMap.toString());
		try {
			logger.log(Level.INFO, "Resources available CPU DISK: " + RUNNING_CPU + " " + RUNNING_DISK);
			synchronized (requestSync) {
				RUNNING_JOBAGENTS += 1;
				if (!updateDynamicParameters()) {
					// requestSync.notify();
					return;
				}

				monitor.sendParameter("TTL", siteMap.get("TTL"));

				setStatus(jaStatus.REQUESTING_JOB);

				final GetMatchJob jobMatch = commander.q_api.getMatchJob(new HashMap<>(siteMap));

				matchedJob = jobMatch.getMatchJob();

				// TODELETE
				if (matchedJob == null || matchedJob.containsKey("Error")) {
					logger.log(Level.INFO,
							"We didn't get anything back. Nothing to run right now.");
					RUNNING_JOBAGENTS -= 1;

					setStatus(jaStatus.ERROR_GET_JDL);

					throw new Exception();
				}

				retries.set(0);

				jdl = new JDL(Job.sanitizeJDL((String) matchedJob.get("JDL")));

				queueId = ((Long) matchedJob.get("queueId")).longValue();
				username = (String) matchedJob.get("User");
				tokenCert = (String) matchedJob.get("TokenCertificate");
				tokenKey = (String) matchedJob.get("TokenKey");
				legacyToken = (String) matchedJob.get("LegacyToken");

				monitor.sendParameter("job_id", Long.valueOf(queueId));

				setStatus(jaStatus.INSTALLING_PKGS);

				matchedJob.entrySet().forEach(entry -> {
					logger.log(Level.INFO, entry.getKey() + " " + entry.getValue());
				});

				logger.log(Level.INFO, jdl.getExecutable());
				logger.log(Level.INFO, username);
				logger.log(Level.INFO, Long.toString(queueId));
				sendBatchInfo();

				if (jdl.getLong("CPUCores") != null)
					reqCPU = ((Number) jdl.getLong("CPUCores")).longValue();
				else
					reqCPU = Long.valueOf(1);

				reqDisk = reqCPU * 10 * 1024L;
				final String workdirMaxSize = jdl.gets("Workdirectorysize");

				final Pattern p = Pattern.compile("\\p{L}");
				if (workdirMaxSize != null) {
					final Matcher m = p.matcher(workdirMaxSize);
					try {
						if (m.find()) {
							final String number = workdirMaxSize.substring(0, m.start());
							final String unit = workdirMaxSize.substring(m.start());

							reqDisk = Long.valueOf(convertStringUnitToIntegerMB(unit, number));
						}
						else
							reqDisk = Long.parseLong(workdirMaxSize);
					}
					catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
						logger.log(Level.INFO, "Local disk space specs are invalid: '" + workdirMaxSize + "', using the default " + reqDisk + "MB");
					}
				}

				logger.log(Level.INFO, "Job requested CPU Disk: " + reqCPU + " " + reqDisk);

				RUNNING_CPU -= reqCPU;
				RUNNING_DISK -= reqDisk;
				logger.log(Level.INFO, "Currently available CPUCores: " + RUNNING_CPU);
				requestSync.notifyAll();
			}

			// TODO: commander.setUser(username);
			// commander.setSite(site);

			logger.log(Level.INFO, jdl.getExecutable());
			logger.log(Level.INFO, username);
			logger.log(Level.INFO, Long.toString(queueId));

			setStatus(jaStatus.JOB_STARTED);

			// process payload
			handleJob();

			cleanup();

			synchronized (requestSync) {
				RUNNING_CPU += reqCPU;
				RUNNING_DISK += reqDisk;
				RUNNING_JOBAGENTS -= 1;

				requestSync.notifyAll();
			}
		}
		catch (final Exception e) {
			logger.log(Level.INFO, "Error getting a matching job: ", e);

			setStatus(jaStatus.ERROR_GET_JDL);

			if (RUNNING_CPU.equals(MAX_CPU))
				retries.getAndIncrement();
			// synchronized (requestSync) {
			// requestSync.notify();
			// }
		}

		setStatus(jaStatus.FINISHING_JA);

		logger.log(Level.INFO, "JobAgent finished, id: " + jobAgentId + " totalJobs: " + totalJobs.get());
	}

	private void handleJob() {

		try {

			if (!createWorkDir()) {
				// changeStatus(JobStatus.ERROR_IB);
				logger.log(Level.INFO, "Error. Workdir for job could not be created");
				return;
			}
			jobWrapperLogDir = jobWorkdir + "/" + jobWrapperLogName;

			logger.log(Level.INFO, "Started JA with: " + jdl);

			final String version = !Version.getTagFromEnv().isEmpty() ? Version.getTagFromEnv() : "/Git: " + Version.getGitHash() + ". Builddate: " + Version.getCompilationTimestamp();
			commander.q_api.putJobLog(queueId, "trace", "Running JAliEn JobAgent" + version);
			commander.q_api.putJobLog(queueId, "trace", "Job preparing to run in: " + hostName);

			// Set up constraints
			getMemoryRequirements();

			final List<String> launchCommand = generateLaunchCommand();

			setupJobWrapperLogging();

			commander.q_api.putJobLog(queueId, "trace", "Starting JobWrapper");

			launchJobWrapper(launchCommand, true);

		}
		catch (final Exception e) {
			logger.log(Level.SEVERE, "Unable to handle job", e);
		}
	}

	private void cleanup() {
		logger.log(Level.INFO, "Sending monitoring values...");

		setStatus(jaStatus.DONE);

		monitor.sendParameter("job_id", Integer.valueOf(0));

		logger.log(Level.INFO, "Cleaning up after execution...");

		try {
			Files.walk(tempDir.toPath())
					.map(Path::toFile)
					.sorted(Comparator.reverseOrder()) // or else dir will appear before its contents
					.forEach(File::delete);
		}
		catch (final IOException e) {
			logger.log(Level.WARNING, "Error deleting the job workdir: " + e.toString());
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

		logger.log(Level.INFO, "Done!");
	}

	/**
	 * @param folder
	 * @return amount of free space (in bytes) in the given folder. Or zero if there was a problem (or no free space).
	 */
	public static long getFreeSpace(final String folder) {
		long space = new File(Functions.resolvePathWithEnv(folder)).getFreeSpace();

		if (space <= 0) {
			// 32b JRE returns 0 when too much space is available

			try {
				final String output = ExternalProcesses.getCmdOutput(Arrays.asList("df", "-P", "-B", "1024", folder), true, 30L, TimeUnit.SECONDS);

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
	 * updates jobagent parameters that change between job requests
	 *
	 * @return false if we can't run because of current conditions, true if positive
	 */
	public boolean checkParameters() {
		final int timeleft = computeTimeLeft();
		if (timeleft <= 0)
			return false;

		if (RUNNING_DISK.longValue() <= 10 * 1024) {
			if (!System.getenv().containsKey("JALIEN_IGNORE_STORAGE")) {
				logger.log(Level.WARNING, "There is not enough space left: " + RUNNING_DISK);
				return false;
			}

			logger.log(Level.INFO, "Ignoring the reported local disk space of " + RUNNING_DISK);
		}

		if (RUNNING_CPU.longValue() <= 0)
			return false;

		return true;
	}

	private int computeTimeLeft() {
		final long jobAgentCurrentTime = System.currentTimeMillis();
		final int time_subs = (int) (jobAgentCurrentTime - jobAgentStartTime) / 1000; // convert to seconds
		int timeleft = origTtl - time_subs;

		logger.log(Level.INFO, "Still have " + timeleft + " seconds to live (" + jobAgentCurrentTime + "-" + jobAgentStartTime + "=" + time_subs + ")");

		// we check if the cert timeleft is smaller than the ttl itself
		final int certTime = getCertTime();
		logger.log(Level.INFO, "Certificate timeleft is " + certTime);
		timeleft = Math.min(timeleft, certTime - 900); // (-15min)

		// safety time for saving, etc
		timeleft -= 600;

		Long shutdownTime = MachineJobFeatures.getFeatureNumber("shutdowntime",
				MachineJobFeatures.FeatureType.MACHINEFEATURE);
		if (shutdownTime != null) {
			shutdownTime = Long.valueOf(shutdownTime.longValue() - System.currentTimeMillis() / 1000);
			logger.log(Level.INFO, "Shutdown is" + shutdownTime);

			timeleft = Integer.min(timeleft, shutdownTime.intValue());
		}

		return timeleft;
	}

	private boolean updateDynamicParameters() {
		logger.log(Level.INFO, "Updating dynamic parameters of jobAgent map");

		// ttl recalculation
		final int timeleft = computeTimeLeft();

		if (checkParameters() == false)
			return false;

		siteMap.put("TTL", Integer.valueOf(timeleft));
		siteMap.put("CPUCores", RUNNING_CPU);
		siteMap.put("Disk", RUNNING_DISK);

		return true;
	}

	/**
	 * @return the time in seconds that the certificate is still valid for
	 */
	private int getCertTime() {
		return (int) TimeUnit.MILLISECONDS.toSeconds(commander.getUser().getUserCert()[0].getNotAfter().getTime() - System.currentTimeMillis());
	}

	private void getMemoryRequirements() {
		// Sandbox size
		final String workdirMaxSize = jdl.gets("Workdirectorysize");

		// By default the jobs are allowed to use up to 10GB of disk space in the sandbox
		workdirMaxSizeMB = 10 * 1024;

		final Pattern p = Pattern.compile("\\p{L}");

		if (workdirMaxSize != null) {
			final Matcher m = p.matcher(workdirMaxSize);
			try {
				if (m.find()) {
					final String number = workdirMaxSize.substring(0, m.start());
					final String unit = workdirMaxSize.substring(m.start());

					workdirMaxSizeMB = convertStringUnitToIntegerMB(unit, number);
				}
				else
					workdirMaxSizeMB = Integer.parseInt(workdirMaxSize);

				commander.q_api.putJobLog(queueId, "trace", "Local disk space limit (JDL): " + workdirMaxSizeMB + "MB");
			}
			catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
				commander.q_api.putJobLog(queueId, "trace", "Local disk space specs are invalid: '" + workdirMaxSize + "', using the default " + workdirMaxSizeMB + "MB");
			}
		}
		else
			commander.q_api.putJobLog(queueId, "trace", "Local disk space limit (default): " + "Unlimited");

		final Integer requestedCPUCores = jdl.getInteger("CPUCores");

		if (requestedCPUCores != null && requestedCPUCores.intValue() > 0)
			cpuCores = requestedCPUCores.intValue();

		// Memory use
		final String maxmemory = jdl.gets("Memorysize");

		// By default the job is limited to using 8GB of virtual memory per allocated CPU core
		jobMaxMemoryMB = cpuCores * 8 * 1024;

		if (maxmemory != null) {
			final Matcher m = p.matcher(maxmemory);
			try {
				if (m.find()) {
					final String number = maxmemory.substring(0, m.start());
					final String unit = maxmemory.substring(m.start()).toUpperCase();

					jobMaxMemoryMB = convertStringUnitToIntegerMB(unit, number);
				}
				else
					jobMaxMemoryMB = Integer.parseInt(maxmemory);

				commander.q_api.putJobLog(queueId, "trace", "Virtual memory limit (JDL): " + jobMaxMemoryMB + "MB");
			}
			catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
				commander.q_api.putJobLog(queueId, "trace", "Virtual memory limit specs are invalid: '" + maxmemory + "', using the default " + jobMaxMemoryMB + "MB");
			}
		}
		else
			commander.q_api.putJobLog(queueId, "trace", "Virtual memory limit (default): " + jobMaxMemoryMB + "MB");
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(final String[] args) throws IOException {
		ConfigUtils.setApplicationName("JobAgent");
		ConfigUtils.switchToForkProcessLaunching();

		final JobAgent jao = new JobAgent();
		jao.run();
	}

	/**
	 * @return Command w/arguments for starting the JobWrapper, based on the command used for the JobAgent
	 * @throws InterruptedException
	 */
	public List<String> generateLaunchCommand() throws InterruptedException {
		try {
			// Main cmd for starting the JobWrapper
			final List<String> launchCmd = new ArrayList<>();

			final Process cmdChecker = Runtime.getRuntime().exec("ps -p " + MonitorFactory.getSelfProcessID() + " -o command=");
			cmdChecker.waitFor();
			try (Scanner cmdScanner = new Scanner(cmdChecker.getInputStream())) {
				String readArg;
				while (cmdScanner.hasNext()) {
					readArg = (cmdScanner.next());
					switch (readArg) {
						case "-cp":
							cmdScanner.next();
							break;
						case "alien.site.JobRunner":
						case "alien.site.JobAgent":
							launchCmd.add("-Djobagent.vmid=" + queueId);
							launchCmd.add("-DAliEnConfig=" + jobWorkdir);
							launchCmd.add("-cp");
							launchCmd.add(jarPath + jarName);
							launchCmd.add("alien.site.JobWrapper");
							break;
						default:
							launchCmd.add(readArg);
					}
				}
			}

			// Check if there is container support at present on site. If yes, add to launchCmd
			final Containerizer cont = ContainerizerFactory.getContainerizer();
			if (cont != null) {
				commander.q_api.putJobLog(queueId, "trace", "Support for containers detected. Will use: " + cont.getContainerizerName());
				monitor.sendParameter("canRunContainers", Integer.valueOf(1));
				monitor.sendParameter("containerLayer", Integer.valueOf(1));
				cont.setWorkdir(jobWorkdir);
				return cont.containerize(String.join(" ", launchCmd));
			}
			monitor.sendParameter("canRunContainers", Integer.valueOf(0));
			return launchCmd;
		}
		catch (final IOException e) {
			logger.log(Level.SEVERE, "Could not generate JobWrapper launch command: " + e.toString());
			return null;
		}
	}

	private int launchJobWrapper(final List<String> launchCommand, final boolean monitorJob) {
		logger.log(Level.INFO, "Launching jobwrapper using the command: " + launchCommand.toString());
		final long ttl = ttlForJob();

		final ProcessBuilder pBuilder = new ProcessBuilder(launchCommand);
		pBuilder.environment().remove("JALIEN_TOKEN_CERT");
		pBuilder.environment().remove("JALIEN_TOKEN_KEY");
		pBuilder.environment().put("TMPDIR", "tmp");
		pBuilder.redirectError(Redirect.INHERIT);
		pBuilder.directory(tempDir);

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
				stdinObj.writeObject(tokenCert);
				stdinObj.writeObject(tokenKey);
				stdinObj.writeObject(ce);
				stdinObj.writeObject(siteMap);
				stdinObj.writeObject(defaultOutputDirPrefix);
				stdinObj.writeObject(legacyToken);
				stdinObj.writeObject(Long.valueOf(ttl));

				stdinObj.flush();
			}

			logger.log(Level.INFO, "JDL info sent to JobWrapper");
			commander.q_api.putJobLog(queueId, "trace", "JobWrapper started");

			// Wait for JobWrapper to start
			try (InputStream stdout = p.getInputStream()) {
				stdout.read();
			}

		}
		catch (final Exception ioe) {
			logger.log(Level.SEVERE, "Exception running " + launchCommand + " : " + ioe.getMessage());

			setStatus(jaStatus.ERROR_START);

			return 1;
		}

		if (monitorJob) {
			final String process_res_format = "FRUNTIME | RUNTIME | CPUUSAGE | MEMUSAGE | CPUTIME | RMEM | VMEM | NOCPUS | CPUFAMILY | CPUMHZ | RESOURCEUSAGE | RMEMMAX | VMEMMAX";
			logger.log(Level.INFO, process_res_format);
			commander.q_api.putJobLog(queueId, "procfmt", process_res_format);

			wrapperPID = (int) p.pid();

			apmon.setNumCPUs(cpuCores);
			apmon.addJobToMonitor(wrapperPID, jobWorkdir, ce + "_Jobs", matchedJob.get("queueId").toString());
			mj = new MonitoredJob(wrapperPID, jobWorkdir, ce + "_Jobs", matchedJob.get("queueId").toString(), cpuCores);

			final String fs = checkProcessResources();
			if (fs == null)
				sendProcessResources();
		}

		final TimerTask killPayload = new TimerTask() {
			@Override
			public void run() {
				logger.log(Level.SEVERE, "Timeout has occurred. Killing job!");
				commander.q_api.putJobLog(queueId, "trace", "Killing the job (it was running for longer than its TTL)");
				killJobWrapperAndPayload(p);
			}
		};

		final Timer t = new Timer();
		t.schedule(killPayload, TimeUnit.MILLISECONDS.convert(ttl, TimeUnit.SECONDS)); // TODO: ttlForJob

		int code = 0;

		logger.log(Level.INFO, "About to enter monitor loop. Is the JobWrapper process alive?: " + p.isAlive());

		int monitor_loops = 0;
		try {
			while (p.isAlive()) {
				logger.log(Level.INFO, "Waiting for the JobWrapper process to finish");

				if (monitorJob) {
					monitor_loops++;
					final String error = checkProcessResources();
					if (error != null) {
						logger.log(Level.SEVERE, "Process overusing resources: " + error);
						commander.q_api.putJobLog(queueId, "trace", "ERROR[FATAL]: Process overusing resources");
						commander.q_api.putJobLog(queueId, "trace", error);
					//	t.cancel();
					//	killJobWrapperAndPayload(p);
					//	return 1;
					}
					if (monitor_loops == 120) {
						monitor_loops = 0;
						sendProcessResources();
					}
				}
				try {
					Thread.sleep(5 * 1000);
				}
				catch (final InterruptedException ie) {
					logger.log(Level.WARNING, "Interrupted while waiting for the JobWrapper to finish execution: " + ie.getMessage());
					return 1;
				}
			}
			code = p.exitValue();

			logger.log(Level.INFO, "JobWrapper has finished execution. Exit code: " + code);
			logger.log(Level.INFO, "All done for job " + queueId  + ". Final status: " + readWrapperStatus());
			commander.q_api.putJobLog(queueId, "trace", "JobWrapper exit code: " + code);
			if (code != 0)
				logger.log(Level.WARNING, "Error encountered: see the JobWrapper logs in: " + env.getOrDefault("TMPDIR", "/tmp") + "/jalien-jobwrapper.log " + " for more details");

			return code;
		}
		finally {
			try {
				t.cancel();
				stdin.close();
			}
			catch (final Exception e) {
				logger.log(Level.WARNING, "Not all resources from the current job could be cleared: " + e);
			}
			apmon.removeJobToMonitor(wrapperPID);
			if (code != 0) {
				// Looks like something went wrong. Let's check the last reported status
				final String lastStatus = readWrapperStatus();
				if ("STARTED".equals(lastStatus) || "RUNNING".equals(lastStatus)) {
					commander.q_api.putJobLog(queueId, "trace", "ERROR: The JobWrapper was killed before job could complete");
					changeJobStatus(JobStatus.ERROR_E, null); // JobWrapper was killed before the job could be completed
				}
				else if ("SAVING".equals(lastStatus)) {
					commander.q_api.putJobLog(queueId, "trace", "ERROR: The JobWrapper was killed during saving");
					changeJobStatus(JobStatus.ERROR_SV, null); // JobWrapper was killed during saving
				}
			}
		}
	}

	private void setStatus(final jaStatus new_status) {
		status = new_status;
		monitor.sendParameter("ja_status_string", status.getStringValue());
		monitor.sendParameter("ja_status", Integer.valueOf(status.getValue()));
	}

	private void sendProcessResources() {
		// runtime(date formatted) start cpu(%) mem cputime rsz vsize ncpu
		// cpufamily cpuspeed resourcecost maxrss maxvss ksi2k
		final String procinfo = String.format("%s %d %.2f %.2f %.2f %.2f %.2f %d %s %s %s %.2f %.2f 1000", RES_FRUNTIME, RES_RUNTIME, RES_CPUUSAGE, RES_MEMUSAGE, RES_CPUTIME, RES_RMEM, RES_VMEM,
				RES_NOCPUS, RES_CPUFAMILY, RES_CPUMHZ, RES_RESOURCEUSAGE, RES_RMEMMAX, RES_VMEMMAX);
		logger.log(Level.INFO, "+++++ Sending resources info +++++");
		logger.log(Level.INFO, procinfo);

		commander.q_api.putJobLog(queueId, "proc", procinfo);
	}

	private String checkProcessResources() { // checks and maintains sandbox
		String error = null;
		logger.log(Level.INFO, "Checking resources usage");

		try {

			final HashMap<Long, Double> jobinfo = mj.readJobInfo();

			final HashMap<Long, Double> diskinfo = mj.readJobDiskUsage();

			if (jobinfo == null || diskinfo == null) {

				logger.log(Level.WARNING, "JobInfo or DiskInfo monitor null");
				// return "Not available"; TODO: Adjust and put back again
			}

			// getting cpu, memory and runtime info
			if (diskinfo != null)
				RES_WORKDIR_SIZE = diskinfo.get(ApMonMonitoringConstants.LJOB_WORKDIR_SIZE);

			if (jobinfo != null) {
				RES_RMEM = Double.valueOf(jobinfo.get(ApMonMonitoringConstants.LJOB_PSS).doubleValue() / 1024);
				RES_VMEM = Double.valueOf(jobinfo.get(ApMonMonitoringConstants.LJOB_SWAPPSS).doubleValue() /1024 + RES_RMEM.doubleValue());

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
			if (workdirMaxSizeMB != 0 && RES_WORKDIR_SIZE.doubleValue() > workdirMaxSizeMB)
				error = "Killing the job (using more than " + workdirMaxSizeMB + "MB of diskspace (right now we were using " + RES_WORKDIR_SIZE + "))";

			// check memory usage
			if (jobMaxMemoryMB != 0 && RES_VMEM.doubleValue() > jobMaxMemoryMB)
				error = "Killing the job (using more than " + jobMaxMemoryMB + " memory (right now " + RES_VMEM + "))";

			// cpu
			final long time = System.currentTimeMillis();

			if (prevTime != 0 && prevTime + (20 * 60 * 1000) < time && RES_CPUTIME.equals(prevCpuTime))
				error = "Killing the job (due to zero CPU consumption in the last 20 minutes!)";
			else {
				prevCpuTime = RES_CPUTIME;
				prevTime = time;
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

		return error;
	}

	private long ttlForJob() {
		final Integer iTTL = jdl.getInteger("TTL");

		int ttl = (iTTL != null ? iTTL.intValue() : 3600);
		commander.q_api.putJobLog(queueId, "trace", "Job asks for a TTL of " + ttl + " seconds");
		ttl += 300; // extra time (saving)

		final String proxyttl = jdl.gets("ProxyTTL");
		if (proxyttl != null) {
			ttl = ((Integer) siteMap.get("TTL")).intValue() - 600;
			commander.q_api.putJobLog(queueId, "trace", "ProxyTTL enabled, running for " + ttl + " seconds");
		}

		return ttl;
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

				return false;
			}
			final File jobTmpDir = new File(jobWorkdir + "/tmp");
			jobTmpDir.mkdir();
		}

		commander.q_api.putJobLog(queueId, "trace", "Created workdir: " + jobWorkdir);

		// chdir
		System.setProperty("user.dir", jobWorkdir);

		return true;
	}

	private void setupJobWrapperLogging() {
		Properties props = new Properties();
		try {
			final ExtProperties ep = ConfigUtils.getConfiguration("logging");

			props = ep.getProperties();

			props.setProperty("java.util.logging.FileHandler.pattern", jobWrapperLogDir);

			logger.log(Level.INFO, "Logging properties loaded for the JobWrapper");
		}
		catch (@SuppressWarnings("unused") final Exception e) {

			logger.log(Level.INFO, "Logging properties for JobWrapper not found.");
			logger.log(Level.INFO, "Using fallback logging configurations for JobWrapper");

			props.put("handlers", "java.util.logging.FileHandler");
			props.put("java.util.logging.FileHandler.pattern", jobWrapperLogDir);
			props.put("java.util.logging.FileHandler.limit", "0");
			props.put("java.util.logging.FileHandler.count", "1");
			props.put("alien.log.WarningFileHandler.append", "true");
			props.put("java.util.logging.FileHandler.formatter", "java.util.logging.SimpleFormatter");
			props.put(".level", "FINEST");
			props.put("lia.level", "WARNING");
			props.put("lazyj.level", "WARNING");
			props.put("apmon.level", "WARNING");
			props.put("alien.level", "FINEST");
			props.put("alien.monitoring.Monitor.level", "WARNING");
			props.put("use_java_logger", "true");
		}

		try (FileOutputStream str = new FileOutputStream(jobWorkdir + "/logging.properties")) {
			props.store(str, null);
		}
		catch (final IOException e1) {
			logger.log(Level.WARNING, "Failed to configure JobWrapper logging", e1);
		}
	}

	private void changeJobStatus(final JobStatus newStatus, final HashMap<String, Object> extrafields) {
		TaskQueueApiUtils.setJobStatus(queueId, newStatus, extrafields);

		if (apmon != null)
			try {
				apmon.sendParameter(ce + "_Jobs", String.valueOf(queueId), "status", newStatus.getAliEnLevel());
			}
			catch (final Exception e) {
				logger.log(Level.WARNING, "JA cannot update ML of the job status change", e);
			}
	}

	private String readWrapperStatus() {
		try {
			return Files.readString(Paths.get(jobWorkdir + "/.jobstatus"));
		}
		catch (final IOException e) {
			logger.log(Level.WARNING, "Attempt to read job status failed. Ignoring: " + e.toString());
			return "";
		}
	}

	private static int convertStringUnitToIntegerMB(final String unit, final String number) {
		switch (unit) {
			case "KB":
				return Integer.parseInt(number) / 1024;
			case "GB":
				return Integer.parseInt(number) * 1024;
			default: // MB
				return Integer.parseInt(number);
		}
	}

	/**
	 * Get LhcbMarks, using a specialized script in CVMFS
	 *
	 * @return script output, or null in case of error
	 */
	public Float getLhcbMarks() {
		if (lhcbMarks > 0)
			return Float.valueOf(lhcbMarks);

		final File lhcbMarksScript = new File(CVMFS.getLhcbMarksScript());

		if (!lhcbMarksScript.exists()) {
			logger.log(Level.WARNING, "Script for lhcbMarksScript not found in: " + lhcbMarksScript.getAbsolutePath());
			return null;
		}

		try {
			String out = ExternalProcesses.getCmdOutput(lhcbMarksScript.getAbsolutePath(), true, 300L, TimeUnit.SECONDS);
			out = out.substring(out.lastIndexOf(":") + 1);
			lhcbMarks = Float.parseFloat(out);
			return Float.valueOf(lhcbMarks);
		}
		catch (final Exception e) {
			logger.log(Level.WARNING, "An error occurred while attempting to run process cleanup: ", e);
			return null;
		}
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

		try {
			final Process getWrapperProcs = Runtime.getRuntime().exec("pgrep -f " + queueId);
			getWrapperProcs.waitFor();
			try (Scanner cmdScanner = new Scanner(getWrapperProcs.getInputStream())) {
				while (cmdScanner.hasNext()) {
					wrapperProcs.add(Integer.valueOf(cmdScanner.next()));
				}
			}
		}
		catch (final Exception e) {
			logger.log(Level.WARNING, "Could not get JobWrapper PID", e);
			return 0;
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
	 * Gracefully kills the JobWrapper and its payload, with a one-hour window for upload
	 *
	 * @param p process for JobWrapper
	 */
	private void killJobWrapperAndPayload(final Process p) {
		try {
			final int jobWrapperPid = getWrapperPid();
			if (jobWrapperPid != 0)
				Runtime.getRuntime().exec("kill " + jobWrapperPid);
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
			"JOB_ID",
			"CREAM_JOBID",
			"GRID_GLOBAL_JOBID",
			"JOB_ID"
	};

	private void sendBatchInfo() {
		for (final String var : batchSystemVars) {
			if (env.containsKey(var)) {
				if ("_CONDOR_JOB_AD".equals(var)) {
					try {
						final List<String> lines = Files.readAllLines(Paths.get(env.get(var)));
						for (final String line : lines) {
							if (line.contains("GlobalJobId"))
								commander.q_api.putJobLog(queueId, "trace", "BatchId " + line);
						}
					}
					catch (final IOException e) {
						logger.log(Level.WARNING, "Error getting batch info from file " + env.get(var) + ":", e);
					}
				}
				else
					commander.q_api.putJobLog(queueId, "trace", "BatchId " + var + ": " + env.get(var));
			}
		}
	}
}
