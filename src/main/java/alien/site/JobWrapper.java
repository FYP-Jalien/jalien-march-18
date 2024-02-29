package alien.site;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.lang.ProcessBuilder.Redirect;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import alien.api.Request;
import alien.api.TomcatServer;
import alien.api.catalogue.CatalogueApiUtils;
import alien.api.taskQueue.TaskQueueApiUtils;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.catalogue.XmlCollection;
import alien.config.ConfigUtils;
import alien.io.IOUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.MonitoringObject;
import alien.monitoring.Timing;
import alien.se.SE;
import alien.shell.commands.JAliEnCOMMander;
import alien.shell.commands.JAliEnCommandcp;
import alien.site.containers.ApptainerCVMFS;
import alien.site.packman.CVMFS;
import alien.site.packman.PackMan;
import alien.taskQueue.JDL;
import alien.taskQueue.JobStatus;
import alien.taskQueue.TaskQueueUtils;
import alien.user.JAKeyStore;
import alien.user.UserFactory;
import apmon.ApMon;
import apmon.ApMonMonitoringConstants;
import apmon.BkThread;
import lazyj.Format;
import lazyj.commands.CommandOutput;
import lazyj.commands.SystemCommand;
import lia.util.process.ExternalProcesses;
import utils.Signals;

/**
 * Job execution wrapper, running an embedded Tomcat server for in/out-bound communications
 */
public final class JobWrapper implements MonitoringObject, Runnable {

	// Folders and files
	private final File currentDir = new File(Paths.get(".").toAbsolutePath().normalize().toString());
	private final String tmpDir = currentDir + "/tmp";
	private final String timeFilePrefix = ".jalienTimes";
	private final String jobstatusFile = ".jalienJobstatus";
	private String defaultOutputDirPrefix;

	// Job variables
	/**
	 * @uml.property name="jdl"
	 * @uml.associationEnd
	 */
	private JDL jdl;
	private long queueId;
	private int resubmission;
	private String username;
	private String tokenCert;
	private String tokenKey;
	private HashMap<String, Object> siteMap;
	private String ce;
	private String legacyToken;
	private long ttl;

	/**
	 * @uml.property name="jobStatus"
	 * @uml.associationEnd
	 */
	private JobStatus jobStatus;

	private boolean killSigReceived = false;

	private final Long masterjobID;

	// Other
	/**
	 * @uml.property name="packMan"
	 * @uml.associationEnd
	 */
	private final PackMan packMan;
	private final String hostName;
	private final int pid;
	private final String ceHost;
	private final String parentHostname;
	private final Map<String, String> metavars;
	/**
	 * @uml.property name="commander"
	 * @uml.associationEnd
	 */
	private final JAliEnCOMMander commander;

	/**
	 * @uml.property name="c_api"
	 * @uml.associationEnd
	 */
	private final CatalogueApiUtils c_api;

	/**
	 * logger object
	 */
	static final Logger logger = ConfigUtils.getLogger(JobWrapper.class.getCanonicalName());

	/**
	 * Streams for data transfer
	 */
	private ObjectInputStream inputFromJobAgent;

	/**
	 * ML monitor object
	 */
	static Monitor monitor = MonitorFactory.getMonitor(JobAgent.class.getCanonicalName());

	/**
	 * ApMon sender
	 */
	static final ApMon apmon = MonitorFactory.getApMonSender();

	/**
	 * Payload process
	 */
	private Process payload;

	private final Thread statusSenderThread = new Thread("JobWrapper.statusSenderThread") {
		@Override
		public void run() {
			if (apmon == null)
				return;

			while (!jobKilled) {
				final Vector<String> paramNames = new Vector<>(5);
				final Vector<Object> paramValues = new Vector<>(5);

				paramNames.add("host_pid");
				paramValues.add(Double.valueOf(MonitorFactory.getSelfProcessID()));

				if (username != null) {
					paramNames.add("job_user");
					paramValues.add(username);
				}

				paramNames.add("host");
				paramValues.add(ConfigUtils.getLocalHostname());

				if (jobStatus != null) {
					paramNames.add("status");
					paramValues.add(Double.valueOf(jobStatus.getAliEnLevel()));
				}

				if (masterjobID != null) {
					paramNames.add("masterjob_id");
					paramValues.add(Double.valueOf(masterjobID.longValue()));
				}

				try {
					apmon.sendParameters(ce + "_Jobs", String.valueOf(queueId), paramNames.size(), paramNames, paramValues);
				}
				catch (final Exception e) {
					logger.log(Level.WARNING, "Cannot send status updates to ML", e);
				}

				synchronized (this) {
					try {
						wait(1000 * 60);
					}
					catch (@SuppressWarnings("unused") final InterruptedException e) {
						return;
					}
				}
			}
		}
	};

	/**
	 * @throws Exception anything bad happening during startup
	 */
	@SuppressWarnings("unchecked")
	public JobWrapper() throws Exception {

		pid = MonitorFactory.getSelfProcessID();

		try {
			inputFromJobAgent = new ObjectInputStream(System.in);
			jdl = (JDL) inputFromJobAgent.readObject();
			username = (String) inputFromJobAgent.readObject();
			queueId = ((Long) inputFromJobAgent.readObject()).longValue();
			resubmission = ((Integer) inputFromJobAgent.readObject()).intValue();
			tokenCert = (String) inputFromJobAgent.readObject();
			tokenKey = (String) inputFromJobAgent.readObject();
			ce = (String) inputFromJobAgent.readObject();
			siteMap = (HashMap<String, Object>) inputFromJobAgent.readObject();
			defaultOutputDirPrefix = (String) inputFromJobAgent.readObject();
			legacyToken = (String) inputFromJobAgent.readObject();
			ttl = ((Long) inputFromJobAgent.readObject()).longValue();
			parentHostname = (String) inputFromJobAgent.readObject();
			metavars = (HashMap<String, String>) inputFromJobAgent.readObject();

			if (logger.isLoggable(Level.FINEST)) {
				logger.log(Level.FINEST, "We received the following tokenCert: " + tokenCert);
				logger.log(Level.FINEST, "We received the following tokenKey: " + tokenKey);
			}

			logger.log(Level.INFO, "We received the following username: " + username);
			logger.log(Level.INFO, "We received the following CE " + ce);

			masterjobID = jdl.getLong("MasterjobID");
		}
		catch (final IOException | ClassNotFoundException e) {
			logger.log(Level.SEVERE, "Error. Could not receive data from JobAgent: " + e);
			throw e;
		}

		if ((tokenCert != null) && (tokenKey != null)) {
			try {
				JAKeyStore.createTokenFromString(tokenCert, tokenKey);
				logger.log(Level.INFO, "Token successfully created");
				JAKeyStore.loadKeyStore();
			}
			catch (final Exception e) {
				logger.log(Level.SEVERE, "Error. Could not load tokenCert and/or tokenKey: " + e);
				throw e;
			}
		}

		hostName = (String) siteMap.getOrDefault(("Localhost"), "");
		ceHost = (String) siteMap.getOrDefault(("CEhost"), siteMap.getOrDefault("Host", ""));
		packMan = (PackMan) siteMap.getOrDefault(("PackMan"), new CVMFS(""));

		commander = JAliEnCOMMander.getInstance();
		c_api = new CatalogueApiUtils(commander);

		// use same tmpdir everywhere
		System.setProperty("java.io.tmpdir", tmpDir);

		statusSenderThread.setDaemon(true);
		statusSenderThread.start();

		logger.log(Level.INFO, "JobWrapper initialised. Running as the following user: " + commander.getUser().getName());

		try {
			final String osRelease = Files.readString(Paths.get("/etc/os-release"));
			final String osName = osRelease.substring(osRelease.indexOf("PRETTY_NAME=") + 12, osRelease.length()).split("\\r?\\n")[0];

			putJobTrace("The following OS has been detected: " + osName);
			logger.log(Level.INFO, "The following OS has been detected: " + osName);
		}
		catch (@SuppressWarnings("unused") final IOException e1) {
			// Ignore
		}

		monitor.addMonitoring("JobWrapper", this);
	}

	@Override
	public void run() {

		logger.log(Level.INFO, "Starting JobWrapper in " + hostName);

		// We start, if needed, the node Tomcat server
		// Does it check a previous one is already running?
		try {
			logger.log(Level.INFO, "Trying to start Tomcat");
			TomcatServer.startTomcatServer();
		}
		catch (final Exception e) {
			logger.log(Level.WARNING, "Unable to start Tomcat." + e);
		}

		logger.log(Level.INFO, "Tomcat started");

		// process payload
		final int runCode = runJob();

		logger.log(Level.INFO, "JobWrapper has finished execution");
		putJobTrace("JobWrapper has finished execution");

		// Positive runCodes originate from the payload. Ignore. All OK here as far as we're concerned.
		System.exit((runCode > 0) ? 0 : Math.abs(runCode));
	}

	private int runJob() {
		try {
			logger.log(Level.INFO, "Started JobWrapper for: " + jdl);

			changeStatus(JobStatus.STARTED);

			final PackagesResolver packResolver = new PackagesResolver();
			packResolver.start();

			final InputFilesDownloader downloader = new InputFilesDownloader();
			downloader.start();

			downloader.join();
			packResolver.join();

			if (downloader.downloadExitCode != 0) {
				logger.log(Level.SEVERE, "Failed to get inputfiles");
				changeStatus(JobStatus.ERROR_IB, downloader.downloadExitCode);
				return -3;
			}

			// run payload
			final int execExitCode = executeJob(packResolver.environment_packages);

			getTraceFromFile();

			if (execExitCode != 0) {
				logger.log(Level.SEVERE, "Failed to run payload");

				if (execExitCode < 0)
					putJobTrace("Failed to start execution of payload. Exit code: " + Math.abs(execExitCode));
				else {
					if (killSigReceived)
						putJobTrace("Payload killed. Executable exit code was " + execExitCode);
					else
						putJobTrace("Warning: executable exit code was " + execExitCode);
				}

				return uploadOutputFiles(JobStatus.ERROR_E, execExitCode) ? execExitCode : -1;
			}

			final int valExitCode = validateJob(packResolver.environment_packages);

			getTraceFromFile();

			if (valExitCode != 0) {
				logger.log(Level.SEVERE, "Validation failed");

				if (valExitCode < 0)
					putJobTrace("Failed to start validation. Exit code: " + Math.abs(valExitCode));
				else
					putJobTrace("Validation failed. Exit code: " + valExitCode);

				final JobStatus valEndState = !killSigReceived ? JobStatus.ERROR_V : JobStatus.ERROR_VT;
				final int valUploadExitCode = uploadOutputFiles(valEndState, valExitCode) ? valExitCode : -1;

				return valUploadExitCode;
			}

			if (!uploadOutputFiles(JobStatus.DONE)) {
				logger.log(Level.SEVERE, "Failed to upload output files");
				return -1;
			}

			return 0;
		}
		catch (final Exception e) {
			logger.log(Level.SEVERE, "Unable to handle job", e);
			final StringBuilder sb = new StringBuilder("ERROR! Unable to handle job: " + e + "\n\r");
			for (final StackTraceElement elem : e.getStackTrace()) {
				sb.append(elem);
				sb.append("\n\r");
			}
			putJobTrace(sb.toString());
			return -1;
		}
	}

	private int executeJob(final Map<String, String> environment_packages) {
		putJobTrace("Starting execution");

		changeStatus(JobStatus.RUNNING);

		final int code = executeCommand(jdl.gets("Executable"), jdl.getArguments(), environment_packages, "execution");

		return code;
	}

	private int validateJob(final Map<String, String> environment_packages) {
		int code = 0;

		final String validation = jdl.gets("ValidationCommand");

		if (validation != null) {
			putJobTrace("Starting validation");
			code = executeCommand(validation, null, environment_packages, "validation");
		}

		return code;
	}

	/**
	 * @param command
	 * @param arguments
	 * @param timeout
	 * @return <code>0</code> if everything went fine, a positive number with the process exit code (which would mean a problem) and a negative error code in case of timeout or other supervised
	 *         execution errors
	 */
	private int executeCommand(final String command, final List<String> arguments, final Map<String, String> environment_packages, final String executionType) {

		logger.log(Level.INFO, "Starting execution of command: " + command);

		List<String> cmd = new LinkedList<>();

		boolean trackTime = false;
		try {
			final String supportsTime = ExternalProcesses.getCmdOutput(Arrays.asList("time", "echo"), true, 30L, TimeUnit.SECONDS);
			if (!supportsTime.contains("command not found"))
				trackTime = true;
		}
		catch (@SuppressWarnings("unused") final Exception e) {
			// ignore
		}

		if (trackTime) {
			cmd.add("/usr/bin/time");
			cmd.add("-p");
			cmd.add("-o");
			cmd.add(tmpDir + "/" + timeFilePrefix + "-" + queueId + "-" + executionType);
		}

		final int idx = command.lastIndexOf('/');

		final String cmdStrip = idx < 0 ? command : command.substring(idx + 1);

		final File fExe = new File(currentDir, cmdStrip);

		if (!fExe.exists()) {
			logger.log(Level.SEVERE, "ERROR. Executable was not found");
			return -2;
		}

		fExe.setExecutable(true);

		cmd.add(fExe.getAbsolutePath());

		if (arguments != null && arguments.size() > 0)
			for (final String argument : arguments)
				if (argument.trim().length() > 0) {
					final StringTokenizer st = new StringTokenizer(argument);

					while (st.hasMoreTokens())
						cmd.add(st.nextToken());
				}

		logger.log(Level.INFO, "Checking if we can use nested containers...");
		final ApptainerCVMFS appt = new ApptainerCVMFS();
		if (!metavars.containsKey("DISABLE_NESTED_CONTAINERS") && appt.isSupported()) {
			logger.log(Level.INFO, "Using nested containers");
			putJobTrace("Using nested containers");
			cmd = appt.containerize(String.join(" ", cmd), false);
		}

		logger.log(Level.INFO, "Executing: " + cmd + ", arguments is " + arguments + " pid: " + pid);

		final ProcessBuilder pBuilder = new ProcessBuilder(cmd);

		final Map<String, String> processEnv = pBuilder.environment();
		final HashMap<String, String> jBoxEnv = ConfigUtils.exportJBoxVariables();

		try {
			processEnv.putAll(environment_packages);
			processEnv.putAll(loadJDLEnvironmentVariables());
			processEnv.putAll(jBoxEnv);
			processEnv.put("APPTAINERENV_PREPEND_PATH", processEnv.get("PATH")); // in case of nested containers
			processEnv.put("APPTAINERENV_LD_LIBRARY_PATH", processEnv.get("LD_LIBRARY_PATH"));
			processEnv.put("JALIEN_TOKEN_CERT", saveToFile(tokenCert));
			processEnv.put("JALIEN_TOKEN_KEY", saveToFile(tokenKey));
			processEnv.put("ALIEN_JOB_TOKEN", legacyToken); // add legacy token
			processEnv.put("ALIEN_PROC_ID", String.valueOf(queueId));
			processEnv.put("ALIEN_MASTERJOB_ID", String.valueOf(masterjobID != null ? masterjobID.longValue() : queueId));
			processEnv.put("ALIEN_SITE", siteMap.get("Site").toString());
			processEnv.put("ALIEN_USER", username);

			processEnv.put("HOME", currentDir.getAbsolutePath());
			processEnv.put("TMP", currentDir.getAbsolutePath() + "/tmp");
			processEnv.put("TMPDIR", currentDir.getAbsolutePath() + "/tmp");

			// Same values used in the Xrootd class for xrdcp command line. Solves deadlocked FST situation.
			processEnv.put("XRD_CONNECTIONWINDOW", "3");
			processEnv.put("XRD_CONNECTIONRETRY", "1");
			processEnv.put("XRD_TIMEOUTRESOLUTION", "1");

			processEnv.putAll(metavars);

			if (!parentHostname.isBlank())
				processEnv.put("PARENT_HOSTNAME", parentHostname);
		}
		catch (final Exception ex) {
			logger.log(Level.WARNING, "One or more environment entries could not be loaded", ex);
			putJobTrace("Warning: One or more environment entries could not be loaded");
		}

		pBuilder.redirectOutput(Redirect.appendTo(new File(currentDir, "stdout")));
		pBuilder.redirectError(Redirect.appendTo(new File(currentDir, "stderr")));

		try {
			payload = pBuilder.start();
		}
		catch (final IOException ioe) {
			logger.log(Level.INFO, "Exception running " + cmd + " : " + ioe.getMessage());
			return -5;
		}

		if (!payload.isAlive()) {
			logger.log(Level.INFO, "The process for: " + cmd + " has terminated. Failed to execute?");
			return payload.exitValue();
		}

		try {
			Signals.addHandler("INT", () -> {
				killSigReceived = true;
				logger.log(Level.SEVERE, "JobWrapper: SIGINT received. Shutting down NOW!"); // Handled by JA
				putJobTrace("JobWrapper: SIGINT received. Shutting down NOW!");
			});

			Signals.addHandler("TERM", () -> {
				killSigReceived = true;
				System.err.println("SIGTERM received. Killing payload and proceeding to upload.");
				putJobTrace("JobWrapper: SIGTERM received. Killing payload and proceeding to upload.");
				if (payload.isAlive()) {
					payload.destroyForcibly();
				}
			});

			payload.waitFor(!executionType.contains("validation") ? ttl : 900, TimeUnit.SECONDS);

			if (payload.isAlive()) {
				payload.destroyForcibly();
				killSigReceived = true;
				logger.log(Level.SEVERE, "Payload process destroyed by timeout in wrapper!");
				putJobTrace("JobWrapper: Payload process destroyed by timeout in wrapper!");
			}

			if (payload.exitValue() != 0) {
				boolean detectedOOM = checkOOMEvents();

				if (detectedOOM)
					logger.log(Level.SEVERE, "An OOM Error has been detected running the job " + queueId);
			}

			logger.log(Level.SEVERE, "Payload has finished execution.");
		}
		catch (final InterruptedException e) {
			logger.log(Level.INFO, "Interrupted while waiting for process to finish execution" + e);
		}

		if (trackTime) {
			try {
				putJobLog("proc", "Execution completed. Time spent: " + Files.readString(Paths.get(tmpDir + "/" + timeFilePrefix + "-" + queueId + "-" + executionType)).replace("\n", ", "));
			}
			catch (@SuppressWarnings("unused") final Exception te) {
				// Ignore
			}
		}
		return payload.exitValue();
	}

	/**
	 * Records job killed by the system due to oom
	 *
	 * @return <code>true</code> if the recording could be done
	 */
	protected boolean recordKilling(double usedMemory, double limitMemory, double usedSwap, double limitSwap, String killedProcessCmd, String cgroupId) {
		logger.log(Level.INFO, "Recording system kill of job " + queueId + " (site: " + ce + " - host: " + hostName + ")");
		putJobTrace("System might have killed job due to OOM");
		if (!commander.q_api.recordPreemption(0l, 0l, System.currentTimeMillis(), 0d, 0d, 0d, 0, resubmission, hostName, ce, 0d, 0d, 0d, username, 0, queueId, limitMemory, limitSwap, killedProcessCmd, cgroupId,usedMemory, usedSwap)) {
			logger.log(Level.SEVERE, "System oom kill could not be recorded in the database");
			return false;
		}
		// To be taken out when not killing
		// changeStatus(JobStatus.ERROR_E);
		return true;
	}

	/**
	 * Analyzes if job was killed by the system due to oom
	 *
	 * @return <code>true</code> if the job ended in oom
	 */
	private boolean checkOOMEvents() {
		boolean usingCgroupsv2 = new File("/sys/fs/cgroup/cgroup.controllers").isFile();
		String cgroupId = MemoryController.parseCgroupsPath(usingCgroupsv2);
		String output = getDmesgOutput("dmesg | egrep -i " + cgroupId + " | egrep -Ei 'oom-kill|Memory cgroup stats'");
		String lastLog = "";
		if (!output.isEmpty())
			lastLog = output.split("\n")[output.split("\n").length - 1];
		if (!lastLog.isEmpty()) {
			Double timing = Double.valueOf(lastLog.substring(lastLog.indexOf("[") + 1, lastLog.indexOf("]")));
			double uptime = MemoryController.parseUptime();
			double elapsedTime = uptime - timing.doubleValue();
			if (MemoryController.debugMemoryController)
				logger.log(Level.INFO, "Parsed time from boot in dmesg is " + timing + ", with a system uptime of " + uptime + ". From hast dmesg log, " + elapsedTime + "s elapsed");
			if (elapsedTime < 5 * 60) { // We check if it is from less than five minutes ago
				double usedMemory = 0d, limitMemory = 0d, usedSwap = 0d, limitSwap = 0d;
				String memoryOutput = getDmesgOutput("dmesg | egrep -i 'memory: usage|swap: usage' | tail -n 2");
				String[] memoryComponents = memoryOutput.split("\n");
				for (String component : memoryComponents) {
					String patternStr = "^.*?usage (\\d+)kB, limit (\\d+)kB, failcnt (\\d+).*?";
					Pattern pattern = Pattern.compile(patternStr);
					Matcher matcher = pattern.matcher(component);
					if (matcher.matches()) {
						if (component.contains("memory") && !component.contains("swap")) {
							usedMemory = Double.parseDouble(matcher.group(1));
							if (Double.parseDouble(matcher.group(2)) != MemoryController.DEFAULT_CGROUP_NOT_CONFIG)
								limitMemory = Double.parseDouble(matcher.group(2));
							else
								limitMemory = 0;
						} else if (component.contains("swap")) {
							usedSwap = Double.parseDouble(matcher.group(1));
							if (Double.parseDouble(matcher.group(2)) != MemoryController.DEFAULT_CGROUP_NOT_CONFIG) {
								limitSwap = Double.parseDouble(matcher.group(2));
							} else
								limitSwap = 0;
						}
					}
				}
				String killedProcessLine = getDmesgOutput("dmesg | egrep -i 'out of memory: Killed process' | tail -n 1");
				String killedProcessCmd = "";
				String patternStr = "^.*? out of memory: Killed process (\\d+) \\((.+)\\).*?";
				Pattern pattern = Pattern.compile(patternStr);
				Matcher matcher = pattern.matcher(killedProcessLine);
				if (matcher.matches()) {
					killedProcessCmd = matcher.group(2);
				}

				return recordKilling(usedMemory/1024, limitMemory/1024, usedSwap/1024, limitSwap/1024, killedProcessCmd, cgroupId);
			}
		}
		else
			logger.log(Level.INFO, "Did not get any OOM error from dmesg");
		return false;
	}

	private static String getDmesgOutput(String dmesgCmd) {
		if (MemoryController.debugMemoryController)
			logger.log(Level.INFO, "Parsing dmesg logs with: " + dmesgCmd);
		CommandOutput co = SystemCommand.bash(dmesgCmd);
		if (co != null) {
			if (co.stderr != null && !co.stderr.isEmpty())
				logger.log(Level.WARNING, "Could not execute dmesg on the host to parse OOM");
			if (MemoryController.debugMemoryController)
				logger.log(Level.INFO, "Dmesg output " + co);
			return co.stdout;
		}
		return "";
	}

	private String saveToFile(final String content) {
		File f;
		try {
			f = File.createTempFile("jobtoken", ".pem", currentDir);

		}
		catch (final IOException e) {
			logger.log(Level.WARNING, "Exception creating temporary file", e);
			return content;
		}
		f.deleteOnExit();

		try (FileWriter fo = new FileWriter(f)) {
			final Set<PosixFilePermission> attrs = new HashSet<>();
			attrs.add(PosixFilePermission.OWNER_READ);
			attrs.add(PosixFilePermission.OWNER_WRITE);

			try {
				Files.setPosixFilePermissions(f.toPath(), attrs);
			}
			catch (final IOException io2) {
				logger.log(Level.WARNING, "Could not protect your keystore " + f.getAbsolutePath() + " with POSIX attributes", io2);
			}

			fo.write(content);

			return f.getCanonicalPath();
		}
		catch (final IOException e1) {
			logger.log(Level.WARNING, "Exception saving content to file", e1);
		}

		return content;
	}

	private Map<String, String> installPackages(final ArrayList<String> packToInstall) {
		if (packMan == null) {
			logger.log(Level.WARNING, "Packman is null!");
			return null;
		}

		try (Timing t = new Timing()) {
			final Map<String, String> env = packMan.installPackage(username, String.join(",", packToInstall), null);
			if (env == null) {
				logger.log(Level.SEVERE, "Error installing " + packToInstall);
				putJobTrace("Error setting the environment for " + packToInstall);
				changeStatus(JobStatus.ERROR_IB);
				System.exit(1);
			}

			logger.log(Level.INFO, "It took " + t + " to generate the environment for " + packToInstall);

			return env;
		}
	}

	private class PackagesResolver extends Thread {
		private Map<String, String> environment_packages;

		@Override
		public void run() {
			environment_packages = getJobPackagesEnvironment();
		}
	}

	private class InputFilesDownloader extends Thread {
		private int downloadExitCode;

		@Override
		public void run() {
			downloadExitCode = getInputFiles();
		}
	}

	private int getInputFiles() {
		final Set<String> filesToDownload = new HashSet<>();

		List<String> list = jdl.getInputFiles(false);

		if (list != null)
			filesToDownload.addAll(list);

		list = jdl.getInputData(false);

		if (list != null)
			filesToDownload.addAll(list);

		String inputDataList;
		try {
			inputDataList = createInputDataList();
		}
		catch (final Exception e) {
			logger.log(Level.SEVERE, "Problem creating XML: ", e);
			return -2;
		}

		String s = jdl.getExecutable();

		if (s != null)
			filesToDownload.add(s);

		s = jdl.gets("ValidationCommand");

		if (s != null)
			filesToDownload.add(s);

		final List<LFN> iFiles = c_api.getLFNs(filesToDownload, true, false);

		if (iFiles == null) {
			logger.log(Level.WARNING, "No requested files could be located");
			putJobTrace("ERROR: No requested files could be located: getLFNs returned null");
			return -1;
		}

		if (iFiles.size() != filesToDownload.size()) {
			logger.log(Level.WARNING, "Not all requested files could be located");

			// diff
			while (!iFiles.isEmpty()) {
				filesToDownload.removeIf(slfn -> (slfn.contains(iFiles.get(iFiles.size() - 1).lfn)));
				iFiles.remove(iFiles.size() - 1);
			}
			putJobTrace("ERROR: Not all requested files could be located in the catalogue. Missing files: " + Arrays.toString(filesToDownload.toArray()));
			return -1;
		}

		final Map<LFN, File> localFiles = new HashMap<>();

		for (final LFN l : iFiles) {
			File localFile = new File(currentDir, l.getFileName());

			int i = 0;

			while (localFile.exists() && i < 100000) {
				localFile = new File(currentDir, l.getFileName() + "." + i);
				i++;
			}

			if (localFile.exists()) {
				logger.log(Level.WARNING, "Too many occurences of " + l.getFileName() + " in " + currentDir.getAbsolutePath());
				putJobTrace("ERROR: Too many occurences of " + l.getFileName() + " in " + currentDir.getAbsolutePath());
				return -1;
			}

			localFiles.put(l, localFile);
		}

		int duplicates = 0;
		for (final Map.Entry<LFN, File> entry : localFiles.entrySet()) {
			File f = entry.getValue();

			if (f.exists()) {
				duplicates++;
				f = new File(currentDir + "/" + duplicates, f.getName());
				f.mkdir();
				logger.log(Level.WARNING, "Warning: Could not download to " + entry.getValue().getAbsolutePath() + ". Already exists. Will instead use: " + f.getAbsolutePath());
				// putJobTrace("Warning: Could not download to " + entry.getValue().getAbsolutePath() + ". Already exists. Will instead use: " + f.getAbsolutePath());
			}

			if (inputDataList != null) {
				if (inputDataList.startsWith("<?xml"))
					inputDataList = inputDataList.replace("turl=\"alien://" + entry.getKey().getCanonicalName(), "turl=\"file:///" + f.getAbsolutePath()); // xmlcollection format here does not match AliEn
				else
					inputDataList = Format.replace(inputDataList, "alien://" + entry.getKey().getCanonicalName() + "\n", "file:///" + f.getAbsolutePath() + "\n");
			}

			putJobTrace("Getting InputFile: " + entry.getKey().getCanonicalName() + " to " + f.getAbsolutePath() + " (" + Format.size(entry.getKey().size) + ")");

			commander.clearLastError();

			final JAliEnCommandcp cp = new JAliEnCommandcp(commander, Arrays.asList(entry.getKey().getCanonicalName(), "file:" + f.getAbsolutePath()));

			final File copyResult = cp.copyGridToLocal(entry.getKey(), f);

			if (copyResult == null) {
				final String commanderError = commander.getLastErrorMessage();

				logger.log(Level.WARNING, "Could not download " + entry.getKey().getCanonicalName() + " to " + entry.getValue().getAbsolutePath() + ":\n" + commanderError);

				String traceLine = "ERROR: ";

				if (commanderError != null)
					traceLine += commanderError;
				else
					traceLine += "Could not download " + entry.getKey().getCanonicalName() + " to " + entry.getValue().getAbsolutePath();

				putJobTrace(traceLine);

				return commander.getLastExitCode();
			}
		}

		logger.log(Level.INFO, "Sandbox populated: " + currentDir.getAbsolutePath());

		if (inputDataList != null) {
			// Dump inputDataList XML
			try {
				String collectionName = jdl.gets("InputDataList");

				if (collectionName == null || collectionName.isBlank())
					collectionName = "wn.xml";

				Files.write(Paths.get(currentDir.getAbsolutePath() + "/" + collectionName), inputDataList.getBytes());
			}
			catch (final Exception e) {
				logger.log(Level.SEVERE, "Problem dumping XML: ", e);
			}
		}

		return 0;
	}

	private String createInputDataList() {
		logger.log(Level.INFO, "Starting XML creation");

		// creates xml file with the InputData
		final String list = jdl.gets("InputDataList");

		if (list == null || list.isBlank()) {
			logger.log(Level.WARNING, "InputDataList is NULL!");
			return null;
		}

		String format = jdl.gets("InputDataListFormat");

		if (format == null || format.isBlank()) {
			if (list.toLowerCase().endsWith(".xml"))
				format = "xml-single";
			else if (list.toLowerCase().endsWith(".txt"))
				format = "txt-list";
			else if (list.toLowerCase().endsWith(".json"))
				format = "json";
		}

		if (!"xml-single".equals(format) && !"txt-list".equals(format) && !"json".equals(format)) {
			logger.log(Level.WARNING, "Data list format not understood: " + format);
			return null;
		}

		logger.log(Level.INFO, "Going to create: " + list + ", format " + format);

		final List<String> datalist = jdl.getInputData(true);

		if ("txt-list".equals(format)) {
			final StringBuilder sb = new StringBuilder();
			for (final String s : datalist)
				sb.append("alien://").append(s).append('\n');

			return sb.toString();
		}

		final XmlCollection c = new XmlCollection();
		c.setName("jobinputdata");
		c.setCommand("JobWrapper.createInputDataList");
		c.setOwner(String.valueOf(pid));

		c.addAll(c_api.getLFNs(datalist, true, false));

		return "json".equals(format) ? c.toJSON() : c.toString();
	}

	private HashMap<String, String> getJobPackagesEnvironment() {
		final String voalice = "VO_ALICE@";
		final StringBuilder packages = new StringBuilder();
		final Map<String, String> packs = jdl.getPackages();
		HashMap<String, String> envmap = new HashMap<>();

		logger.log(Level.INFO, "Preparing to install packages");
		if (packs != null) {
			for (final Map.Entry<String, String> entry : packs.entrySet())
				packages.append(voalice + entry.getKey() + "::" + entry.getValue() + ",");

			// if (!packs.containsKey("APISCONFIG"))
			// packages.append(voalice + "APISCONFIG,");

			final String packagestring = packages.substring(0, packages.length() - 1);

			final ArrayList<String> packagesList = new ArrayList<>();
			packagesList.add(packagestring);

			logger.log(Level.INFO, packagestring);

			envmap = (HashMap<String, String>) installPackages(packagesList);
		}

		logger.log(Level.INFO, envmap.toString());
		return envmap;
	}

	private boolean uploadOutputFiles(final JobStatus exitStatus) {
		return uploadOutputFiles(exitStatus, 0);
	}

	private boolean uploadOutputFiles(final JobStatus exitStatus, final int exitCode) {
		cleanupProcesses(queueId, pid);

		boolean uploadedAllOutFiles = true;
		boolean uploadedNotAllCopies = false;

		logger.log(Level.INFO, "Uploading output for: " + jdl);

		final String outputDir = getJobOutputDir(exitStatus);

		putJobTrace("Going to uploadOutputFiles(exitStatus=" + exitStatus + ", outputDir=" + outputDir + ")");

		contextualizeJDL();

		boolean jobExecutedSuccessfully = true;
		if (exitStatus.toString().contains("ERROR")) {
			putJobTrace("Registering temporary log files in " + outputDir + ". You must do 'registerOutput " + queueId
					+ "' within 24 hours of the job termination to preserve them. After this period, they are automatically deleted.");
			jobExecutedSuccessfully = false;
		}

		changeStatus(JobStatus.SAVING);

		logger.log(Level.INFO, "queueId: " + queueId);
		logger.log(Level.INFO, "resubmission: " + resubmission);
		logger.log(Level.INFO, "outputDir: " + outputDir);
		logger.log(Level.INFO, "We are the current user: " + commander.getUser().getName());

		final ArrayList<OutputEntry> toUpload = getUploadEntries(getOutputTags(exitStatus), jobExecutedSuccessfully);
		if (toUpload == null) {
			if (jobExecutedSuccessfully)
				changeStatus(JobStatus.ERROR_S);
			else
				changeStatus(exitStatus, exitCode);
			return false;
		}

		for (final OutputEntry entry : toUpload) {
			try {
				final File localFile = new File(currentDir.getAbsolutePath() + "/" + entry.getName());
				logger.log(Level.INFO, "Processing output file: " + localFile);

				if (localFile.exists() && localFile.isFile() && localFile.canRead() && localFile.length() > 0) {
					putJobTrace("Uploading: " + entry.getName() + " to " + outputDir);

					final List<String> cpOptions = new ArrayList<>();
					cpOptions.add("-m");
					cpOptions.add("-S");

					if (entry.getOptions() != null && entry.getOptions().length() > 0)
						cpOptions.add(entry.getOptions());
					else
						cpOptions.add("disk:2");

					if (entry.getAsyncTargets() != null && entry.getAsyncTargets().length() > 0) {
						cpOptions.add("-q");
						cpOptions.add(entry.getAsyncTargets());
					}

					cpOptions.add("-j");
					cpOptions.add(String.valueOf(queueId));

					// Don't commit in case of ERROR_E or ERROR_V
					if (exitStatus == JobStatus.ERROR_E || exitStatus == JobStatus.ERROR_V)
						cpOptions.add("-nc");

					final ByteArrayOutputStream out = new ByteArrayOutputStream();
					final LFN uploadResult = IOUtils.upload(localFile, outputDir + "/" + entry.getName(), UserFactory.getByUsername(username), out, cpOptions.toArray(new String[0]));

					final String output_upload = out.toString();
					logger.log(Level.INFO,
							"Output result of " + localFile.getAbsolutePath() + " to " + outputDir + "/" + entry.getName() + " is:\nLFN = " + uploadResult + "\nFull `cp` output:\n"
									+ output_upload);

					if (uploadResult == null) {
						// complete failure to upload the file, mark the job as failed, not trying further to upload anything
						uploadedAllOutFiles = false;
						putJobTrace("Failed to upload to " + outputDir + "/" + entry.getName() + ": " + out.toString());
						break;
					}

					// success, but could all the copies requested in the JDL be created as per user specs?
					if (output_upload.contains("requested replicas could be uploaded")) {
						// partial success, will lead to a DONE_WARN state
						uploadedNotAllCopies = true;
						putJobTrace(output_upload);
					}
					else
						putJobTrace(uploadResult.getCanonicalName() + ": uploaded as requested");

					// archive entries are only booked when committed, so we have to do it ourselves since -nc
					if ((exitStatus == JobStatus.ERROR_E || exitStatus == JobStatus.ERROR_V) && entry.isArchive())
						CatalogueApiUtils.bookArchiveEntries(entry, uploadResult, outputDir + "/", UserFactory.getByUsername(username));
				}
				else {
					logger.log(Level.WARNING, "Can't upload output file " + localFile.getName() + ", does not exist or has zero size.");
					putJobTrace("Can't upload output file " + localFile.getName() + ", does not exist or has zero size.");
				}
			}
			catch (final IOException e) {
				logger.log(Level.WARNING, "IOException received while attempting to upload " + entry.getName(), e);
				putJobTrace("Failed to upload " + entry.getName() + " due to: " + e.getMessage());
				uploadedAllOutFiles = false;
			}
		}

		if (!uploadedAllOutFiles && jobExecutedSuccessfully) {
			changeStatus(JobStatus.ERROR_SV);
			return false;
		}
		if (jobExecutedSuccessfully) {
			if (!registerEntries(toUpload, outputDir))
				changeStatus(JobStatus.ERROR_SV);
			else if (uploadedNotAllCopies)
				changeStatus(JobStatus.DONE_WARN);
			else
				changeStatus(JobStatus.DONE);
		}
		else
			changeStatus(exitStatus, exitCode);

		return uploadedAllOutFiles;
	}

	private void contextualizeJDL() {
		final Set<String> tagsToPatch = new HashSet<>();

		for (final String key : jdl.keySet()) {
			if (key.toUpperCase().startsWith("OUTPUT")) {
				if (jdl.get(key).toString().contains("@inheritlocation"))
					tagsToPatch.add(key);
			}
		}

		if (tagsToPatch.size() > 0) {
			final List<String> inputData = jdl.getInputData(true);

			final Set<String> sesToReplaceWith = new HashSet<>();

			if (inputData != null && inputData.size() > 0) {
				int replicas = 0;

				final Map<SE, AtomicInteger> ses = new HashMap<>();

				final Map<LFN, Set<PFN>> locations = commander.c_api.getPFNs(inputData);

				if (locations != null && locations.size() > 0) {
					for (final Set<PFN> pfns : locations.values()) {
						replicas += pfns.size();

						for (final PFN p : pfns)
							ses.computeIfAbsent(p.getSE(), (k) -> new AtomicInteger(0)).incrementAndGet();
					}

					final int targetReplicas = Math.round((float) replicas / locations.size());

					final List<Map.Entry<SE, AtomicInteger>> sortedLocations = new ArrayList<>(ses.entrySet());
					sortedLocations.sort((e1, e2) -> Integer.compare(e2.getValue().intValue(), e1.getValue().intValue()));

					for (int i = 0; i < targetReplicas && i < sortedLocations.size(); i++)
						sesToReplaceWith.add(sortedLocations.get(i).getKey().getName());
				}
			}

			final String valueToReplaceWith = sesToReplaceWith.size() > 0 ? "@" + String.join(",", sesToReplaceWith) : "";

			putJobTrace("Output will be saved together with the input files in `" + valueToReplaceWith + "`");

			for (final String tag : tagsToPatch) {
				final Collection<String> values = jdl.getList(tag);

				jdl.delete(tag);

				for (final String value : values)
					jdl.append(tag, Format.replace(value, "@inheritlocation", valueToReplaceWith));
			}
		}
	}

	@SuppressWarnings("unchecked")
	private HashMap<String, String> loadJDLEnvironmentVariables() {
		final HashMap<String, String> hashret = new HashMap<>();

		try {
			final HashMap<String, Object> vars = (HashMap<String, Object>) jdl.getJDLVariables();

			if (vars != null)
				for (final String s : vars.keySet()) {
					String value = "";
					final Object val = jdl.get(s);

					if (val == null) {
						logger.log(Level.WARNING, "Skipping the JDLVariable `" + s + "` as it is not defined in the JDL");
						continue;
					}

					if (val instanceof Collection<?>)
						value = String.join("##", (Collection<String>) val);
					else
						value = val.toString();

					hashret.put("ALIEN_JDL_" + s.toUpperCase(), value);
				}
		}
		catch (final Exception e) {
			logger.log(Level.WARNING, "There was a problem getting JDLVariables: " + e);
		}

		return hashret;
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(final String[] args) throws Exception {
		ConfigUtils.setApplicationName("JobWrapper");
		ConfigUtils.switchToForkProcessLaunching();

		final JobWrapper jw = new JobWrapper();
		jw.run();
	}

	/**
	 * Updates the current state of the job.
	 *
	 * @param newStatus
	 * @return <code>false</code> if the job was killed and execution should not continue
	 */
	public boolean changeStatus(final JobStatus newStatus) {
		return changeStatus(newStatus, 0);
	}

	/**
	 * Updates the current state of the job.
	 *
	 * @param newStatus
	 * @param exitCode
	 * @return <code>false</code> if the job was killed and execution should not continue
	 */
	public boolean changeStatus(final JobStatus newStatus, final int exitCode) {
		if (jobKilled)
			return false;

		jobStatus = newStatus;

		final HashMap<String, Object> extrafields = new HashMap<>();
		extrafields.put("exechost", ceHost);
		extrafields.put("CE", ce);
		extrafields.put("node", hostName);

		// if final status with saved files, we set the path
		if (jobStatus == JobStatus.DONE || jobStatus == JobStatus.DONE_WARN || jobStatus == JobStatus.ERROR_E || jobStatus == JobStatus.ERROR_V) {
			extrafields.put("path", getJobOutputDir(newStatus));
			if (exitCode != 0)
				extrafields.put("error", Integer.valueOf(exitCode));
		}
		else if (jobStatus == JobStatus.RUNNING) {
			extrafields.put("spyurl", hostName + ":" + TomcatServer.getPort());
			extrafields.put("node", hostName);
			extrafields.put("agentuuid", Request.getVMID().toString());

			// TODO add "cpu" with columns from QUEUE_CPU
			HashMap<String, Object> cpuExtraFields = getStaticCPUAccounting();
			extrafields.put("cpu", cpuExtraFields);

			extrafields.put("ncpu", Integer.valueOf(TaskQueueUtils.getCPUCores(jdl)));
			extrafields.put("mem", Long.valueOf(Runtime.getRuntime().maxMemory()));
		}

		try {
			// Write status to file for the JobAgent to see
			Files.writeString(Paths.get(tmpDir + "/" + jobstatusFile), newStatus.name());

			// Set the updated status
			if (!TaskQueueApiUtils.setJobStatus(queueId, resubmission, newStatus, extrafields)) {
				jobKilled = true;
				return false;
			}
		}
		catch (final Exception e) {
			logger.log(Level.WARNING, "An error occurred when attempting to change current job status: " + e);
		}

		synchronized (statusSenderThread) {
			statusSenderThread.notifyAll();
		}

		return true;
	}

	/**
	 * @param exitStatus the target job status, affecting the booked directory (`~/recycle` if any error)
	 * @return job output dir (as indicated in the JDL if OK, or the recycle path if not)
	 */
	public String getJobOutputDir(final JobStatus exitStatus) {
		String outputDir = jdl.getOutputDir();

		if (exitStatus == JobStatus.ERROR_V || exitStatus == JobStatus.ERROR_E)
			outputDir = FileSystemUtils.getAbsolutePath(username, null, "~" + "recycle" + "/alien-job-" + queueId);
		else if (outputDir == null)
			outputDir = FileSystemUtils.getAbsolutePath(username, null, "~" + defaultOutputDirPrefix + queueId);

		return outputDir;
	}

	private void getTraceFromFile() {
		final File traceFile = new File(".alienValidation.trace");

		if (traceFile.exists() && traceFile.length() > 0) {
			try {
				final String trace = new String(Files.readAllBytes(traceFile.toPath()));

				if (!trace.isBlank())
					putJobTrace(trace);

				traceFile.delete();
			}
			catch (final Exception e) {
				logger.log(Level.WARNING, "An error occurred when reading .alienValidation.trace: " + e);
			}
		}

		logger.log(Level.INFO, ".alienValidation.trace does not exist.");
	}

	@Override
	public void fillValues(final Vector<String> paramNames, final Vector<Object> paramValues) {
		if (queueId > 0 && !jobKilled) {
			paramNames.add("jobID");
			paramValues.add(Double.valueOf(queueId));

			paramNames.add("statusID");
			paramValues.add(Double.valueOf(jobStatus.getAliEnLevel()));
		}
	}

	/**
	 * Cleanup processes, using a specialized script in CVMFS
	 *
	 * @param queueId AliEn job ID
	 * @param pid child process ID to start from
	 *
	 * @return script exit code, or -1 in case of error
	 */
	public static int cleanupProcesses(final long queueId, final int pid) {

		// Attempt cleanup using Java first
		int cleanupAttempts = 0;
		while (ProcessHandle.current().descendants().count() > 0 && cleanupAttempts < 10) {
			ProcessHandle.current().descendants().forEach(descendantProc -> {
				descendantProc.destroyForcibly();
			});
			cleanupAttempts += 1;
		}

		final File cleanupScript = new File(CVMFS.getCleanupScript());
		
		if (!cleanupScript.exists()) {
			logger.log(Level.WARNING, "Script for process cleanup not found in: " + cleanupScript.getAbsolutePath());
			return -1;
		}

		try {
			final ProcessBuilder pb = new ProcessBuilder(cleanupScript.getAbsolutePath(), "-v", "-m", "ALIEN_PROC_ID=" + queueId, String.valueOf(pid), "-KILL");
			pb.redirectError(Redirect.INHERIT);

			final Process process = pb.start();

			process.waitFor(60, TimeUnit.SECONDS);
			if (process.isAlive())
				process.destroyForcibly();

			try (InputStream is = process.getInputStream()) {
				final String result = new String(is.readAllBytes());
				logger.log(Level.INFO, result);
			}

			return process.exitValue();
		}
		catch (IOException | InterruptedException e) {
			logger.log(Level.WARNING, "An error occurred while attempting to run process cleanup: " + e);
			return -1;
		}
	}

	/**
	 * Register lfn links to archive
	 *
	 * @param entries
	 * @param outputDir
	 */
	private boolean registerEntries(final ArrayList<OutputEntry> entries, final String outputDir) {
		boolean registeredAll = true;
		for (final OutputEntry entry : entries) {
			try {
				final boolean registered = CatalogueApiUtils.registerEntry(entry, outputDir + "/", UserFactory.getByUsername(username));
				putJobTrace("Registering: " + entry.getName() + ". Return status: " + registered);

				if (!registered)
					registeredAll = false;
			}
			catch (final NullPointerException npe) {
				logger.log(Level.WARNING, "An error occurred while registering " + entry + ". Bad connection?", npe);
				putJobTrace("An error occurred while registering " + entry + ". Bad connection?");

				changeStatus(JobStatus.ERROR_SV);
				System.exit(1);
			}
		}
		return registeredAll;
	}

	private ArrayList<String> getOutputTags(final JobStatus exitStatus) {
		final ArrayList<String> tags = new ArrayList<>();

		if (exitStatus == JobStatus.ERROR_E || exitStatus == JobStatus.ERROR_V) {

			// Protect against uploading large std* logfiles for error jobs
			for (final String entry : Arrays.asList("stdout", "stderr")) {
				final File logFile = new File(currentDir.getAbsolutePath() + "/" + entry);
				if (logFile.exists()) {
					try (final FileOutputStream fos = new FileOutputStream(logFile, true); final FileChannel out = fos.getChannel()) {
						if (Files.size(logFile.toPath()) > 1073741824L) {
							out.truncate(1073741824L);
						}
					}
					catch (@SuppressWarnings("unused") Exception e) {
						// ignore
					}
				}
			}
			if (exitStatus == JobStatus.ERROR_E) {
				if (jdl.gets("OutputErrorE") == null) {
					putJobTrace("No output given for " + exitStatus + " in JDL. Defaulting to std*");
					jdl.set("OutputErrorE", "log_archive.zip:std*@disk=1");
				}
				tags.add("OutputErrorE");
				return tags;
			}
		}

		if (jdl.gets("OutputArchive") != null)
			tags.add("OutputArchive");
		if (jdl.gets("OutputFile") != null)
			tags.add("OutputFile");
		if (jdl.gets("Output") != null)
			tags.add("Output");

		return tags;
	}

	/**
	 * @param outputTags
	 * @param jobExecutedSuccessfully
	 * @return List of entries to upload, based on the outputtags. Null if at least one required file is missing
	 */
	private ArrayList<OutputEntry> getUploadEntries(final ArrayList<String> outputTags, final boolean jobExecutedSuccessfully) {
		final ArrayList<OutputEntry> archivesToUpload = new ArrayList<>();
		final ArrayList<OutputEntry> standaloneFilesToUpload = new ArrayList<>();
		final ArrayList<String> allArchiveEntries = new ArrayList<>();

		for (final String tag : outputTags) {
			try {
				final ParsedOutput filesTable = new ParsedOutput(queueId, jdl, currentDir.getAbsolutePath(), tag, jobExecutedSuccessfully);
				for (final OutputEntry entry : filesTable.getEntries()) {
					if (entry.isArchive()) {
						logger.log(Level.INFO, "This is an archive: " + entry.getName());
						final ArrayList<String> archiveEntries = entry.createZip(currentDir.getAbsolutePath());
						if (archiveEntries.size() == 0) {
							logger.log(Level.WARNING, "Ignoring empty archive: " + entry.getName());
							putJobTrace("Ignoring empty archive: " + entry.getName());
						}
						else {
							for (final String archiveEntry : archiveEntries) {
								allArchiveEntries.add(archiveEntry);
								logger.log(Level.INFO, "Adding to archive members: " + archiveEntry);
							}
							archivesToUpload.add(entry);
						}
					}
					else {
						logger.log(Level.INFO, "This is not an archive: " + entry.getName());
						final File entryFile = new File(currentDir.getAbsolutePath() + "/" + entry.getName());
						if (entryFile.length() <= 0) { // archive files are checked for this during createZip, but standalone files still need to be checked
							logger.log(Level.WARNING, "The following file has size 0 and will be ignored: " + entry.getName());
							putJobTrace("The following file has size 0 and will be ignored: " + entry.getName());
						}
						else {
							standaloneFilesToUpload.add(entry);
							logger.log(Level.INFO, "Adding to standalone: " + entry.getName());
						}
					}
				}
			}
			catch (final NullPointerException ex) {
				logger.log(Level.SEVERE, "A required outputfile was NOT found! Aborting: " + ex.getMessage());
				putJobTrace("Error: A required outputfile was NOT found! Aborting: " + ex.getMessage());
				return null;
			}
		}
		return mergeAndRemoveDuplicateEntries(standaloneFilesToUpload, archivesToUpload, allArchiveEntries);

	}

	private static ArrayList<OutputEntry> mergeAndRemoveDuplicateEntries(final ArrayList<OutputEntry> filesToMerge, final ArrayList<OutputEntry> fileList, final ArrayList<String> allArchiveEntries) {
		for (final OutputEntry file : filesToMerge) {
			if (!allArchiveEntries.contains(file.getName())) {
				logger.log(Level.INFO, "Standalone file not in any archive. To be uploaded separately: " + file.getName());
				fileList.add(file);
			}
		}
		return fileList;
	}

	private boolean jobKilled = false;

	private boolean putJobLog(final String key, final String value) {
		if (jobKilled)
			return false;

		if (!commander.q_api.putJobLog(queueId, resubmission, key, value)) {
			jobKilled = true;
			return false;
		}

		return true;
	}

	private boolean putJobTrace(final String value) {
		return putJobLog("trace", value);
	}

	private static HashMap<String, Object> getStaticCPUAccounting() {
		HashMap<String, Object> cpuExtraFields = new HashMap<>();

		try {
			Hashtable<Long, String> cpuinfo;
			cpuinfo = BkThread.getCpuInfo();
			cpuExtraFields.put("processor_count", Integer.valueOf(BkThread.getNumCPUs()));
			cpuExtraFields.put("model_name", cpuinfo.getOrDefault(ApMonMonitoringConstants.LGEN_CPU_MODEL_NAME, null));
			cpuExtraFields.put("vendor_id", cpuinfo.getOrDefault(ApMonMonitoringConstants.LGEN_CPU_VENDOR_ID, null));
			cpuExtraFields.put("cpu_family", cpuinfo.getOrDefault(ApMonMonitoringConstants.LGEN_CPU_FAMILY, null));
			cpuExtraFields.put("model", cpuinfo.getOrDefault(ApMonMonitoringConstants.LGEN_CPU_MODEL, null));
		}
		catch (Exception e) {
			logger.log(Level.SEVERE, "Could not get information from BkThread: " + e.toString());
		}

		boolean ht = false;
		try {
			final List<String> lines = Files.readAllLines(Paths.get("/proc/cpuinfo"));
			String microcode = null;

			final Pattern physicalIdPattern = Pattern.compile("^physical id\\s*:\\s*([\\w ]+)");
			final Pattern flagsPattern = Pattern.compile("^flags\\s*:([\\w ]+)");
			final Pattern coresPattern = Pattern.compile("^cpu cores\\s*:\\s*([0-9]+)\\s*");
			final Pattern siblingsPattern = Pattern.compile("^siblings\\s*:\\s*([0-9]+)\\s*");
			final Pattern microcodePattern = Pattern.compile("^microcode\\s*:(\\s*[\\w\\s]+)");

			HashMap<Integer, HashMap<String, Integer>> physicalCPUs = new HashMap<>();

			HashMap<String, Integer> physicalCPU = null;

			Matcher m;
			for (final String line : lines) {
				m = physicalIdPattern.matcher(line);
				if (m.matches()) {
					Integer physicalId = Integer.valueOf(m.group(1));
					physicalCPU = physicalCPUs.get(physicalId);
					if (physicalCPU == null) {
						physicalCPU = new HashMap<>();
						physicalCPUs.put(physicalId, physicalCPU);
					}
				}

				m = flagsPattern.matcher(line);
				if (m.matches() && (line.contains("ht ") || line.contains(" ht"))) {
					ht = true;
				}

				m = coresPattern.matcher(line);
				if (m.matches()) {
					Integer coresLine = Integer.valueOf(m.group(1));
					if (physicalCPU != null) {
						Integer coresPerPhysicalCPU = physicalCPU.get("cores");
						if (coresPerPhysicalCPU == null || coresLine.intValue() > coresPerPhysicalCPU.intValue())
							physicalCPU.put("cores", coresLine);
					}
				}

				m = siblingsPattern.matcher(line);
				if (m.matches()) {
					Integer siblingsLine = Integer.valueOf(m.group(1));
					if (physicalCPU != null) {
						Integer siblingsPerPhysicalCPU = physicalCPU.get("siblings");
						if (siblingsPerPhysicalCPU == null || siblingsLine.intValue() > siblingsPerPhysicalCPU.intValue())
							physicalCPU.put("siblings", siblingsLine);
					}
				}

				m = microcodePattern.matcher(line);
				if (m.matches()) {
					microcode = m.group(1);
					cpuExtraFields.put("microcode", microcode.trim());
				}
			}

			int cores = 0;
			int siblings = 0;

			for (final HashMap<String, Integer> cpuEntry : physicalCPUs.values()) {
				cores += cpuEntry.getOrDefault("cores", Integer.valueOf(0)).intValue();
				siblings += cpuEntry.getOrDefault("siblings", Integer.valueOf(0)).intValue();
			}

			cpuExtraFields.put("cores", Integer.valueOf(cores));
			cpuExtraFields.put("siblings", Integer.valueOf(siblings));
			cpuExtraFields.put("ht", Integer.valueOf(ht ? 1 : 0));
		}
		catch (Exception e) {
			logger.log(Level.SEVERE, "Could not read/parse /proc/cpuinfo: " + e.toString());
		}

		return cpuExtraFields;
	}
}
