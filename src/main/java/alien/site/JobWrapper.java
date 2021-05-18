package alien.site;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.ProcessBuilder.Redirect;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.api.TomcatServer;
import alien.api.catalogue.CatalogueApiUtils;
import alien.api.taskQueue.TaskQueueApiUtils;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.GUID;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
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
import alien.taskQueue.JobStatus;
import alien.user.JAKeyStore;
import alien.user.UserFactory;
import apmon.ApMon;

/**
 * Job execution wrapper, running an embedded Tomcat server for in/out-bound communications
 */
public final class JobWrapper implements MonitoringObject, Runnable {

	// Folders and files
	private final File currentDir = new File(Paths.get(".").toAbsolutePath().normalize().toString());
	private String defaultOutputDirPrefix;

	// Job variables
	/**
	 * @uml.property name="jdl"
	 * @uml.associationEnd
	 */
	private JDL jdl;
	private long queueId;
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
	static final Monitor monitor = MonitorFactory.getMonitor(JobAgent.class.getCanonicalName());

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

			while (true) {
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
			tokenCert = (String) inputFromJobAgent.readObject();
			tokenKey = (String) inputFromJobAgent.readObject();
			ce = (String) inputFromJobAgent.readObject();
			siteMap = (HashMap<String, Object>) inputFromJobAgent.readObject();
			defaultOutputDirPrefix = (String) inputFromJobAgent.readObject();
			legacyToken = (String) inputFromJobAgent.readObject();
			ttl = ((Long) inputFromJobAgent.readObject()).longValue();

			logger.log(Level.INFO, "We received the following tokenCert: " + tokenCert);
			logger.log(Level.INFO, "We received the following tokenKey: " + tokenKey);
			logger.log(Level.INFO, "We received the following username: " + username);
			logger.log(Level.INFO, "We received the following CE " + ce);

			masterjobID = jdl.getLong("MasterjobID");
		}
		catch (final IOException | ClassNotFoundException e) {
			logger.log(Level.SEVERE, "Error: Could not receive data from JobAgent" + e);
			throw e;
		}

		if ((tokenCert != null) && (tokenKey != null)) {
			try {
				JAKeyStore.createTokenFromString(tokenCert, tokenKey);
				logger.log(Level.INFO, "Token successfully created");
				JAKeyStore.loadKeyStore();
			}
			catch (final Exception e) {
				logger.log(Level.SEVERE, "Error. Could not load tokenCert and/or tokenKey" + e);
				throw e;
			}
		}

		hostName = (String) Objects.requireNonNullElse(siteMap.get("Host"), "");
		ceHost = (String) Objects.requireNonNullElse(siteMap.get("CEhost"), hostName);
		packMan = (PackMan) Objects.requireNonNullElse(siteMap.get("PackMan"), new CVMFS(""));

		commander = JAliEnCOMMander.getInstance();
		c_api = new CatalogueApiUtils(commander);

		// use same tmpdir everywhere
		System.setProperty("java.io.tmpdir", currentDir.getAbsolutePath() + "/tmp");

		statusSenderThread.setDaemon(true);
		statusSenderThread.start();

		logger.log(Level.INFO, "JobWrapper initialised. Running as the following user: " + commander.getUser().getName());

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
		commander.q_api.putJobLog(queueId, "trace", "JobWrapper has finished execution");

		if (runCode > 0)
			System.exit(0); // Positive runCodes originate from the payload. Ignore. All OK here as far as we're concerned.
		else
			System.exit(Math.abs(runCode));
	}

	private Map<String, String> installPackages(final ArrayList<String> packToInstall) {
		if (packMan == null) {
			logger.log(Level.WARNING, "Packman is null!");
			return null;
		}

		final Map<String, String> env = packMan.installPackage(username, String.join(",", packToInstall), null);
		if (env == null) {
			logger.log(Level.INFO, "Error installing the package " + packToInstall);
			// monitor.sendParameter("ja_status", "ERROR_IP");
			logger.log(Level.SEVERE, "Error installing " + packToInstall);
			System.exit(1);
		}

		return env;
	}

	private int runJob() {
		try {
			logger.log(Level.INFO, "Started JobWrapper for: " + jdl);

			changeStatus(JobStatus.STARTED);

			if (!getInputFiles()) {
				logger.log(Level.SEVERE, "Failed to get inputfiles");
				changeStatus(JobStatus.ERROR_IB);
				return -1;
			}

			// run payload
			final int execExitCode = execute();
			if (execExitCode != 0) {
				logger.log(Level.SEVERE, "Failed to run payload");

				if (execExitCode < 0)
					commander.q_api.putJobLog(queueId, "trace", "Failed to start execution of payload. Exit code: " + Math.abs(execExitCode));
				else
					commander.q_api.putJobLog(queueId, "trace", "Marking the job as ERROR_E since executable exit code was " + execExitCode);

				// if (jdl.gets("OutputErrorE") != null)
				return uploadOutputFiles(JobStatus.ERROR_E) ? execExitCode : -1;

				// changeStatus(JobStatus.ERROR_E);
				// return execExitCode;
			}

			final int valExitCode = validate();
			if (valExitCode != 0) {
				logger.log(Level.SEVERE, "Validation failed");

				if (valExitCode < 0)
					commander.q_api.putJobLog(queueId, "trace", "Failed to start validation. Exit code: " + Math.abs(valExitCode));
				else
					commander.q_api.putJobLog(queueId, "trace", "Validation failed. Exit code: " + valExitCode);

				final String fileTrace = getTraceFromFile();
				if (fileTrace != null)
					commander.q_api.putJobLog(queueId, "trace", fileTrace);

				final int valUploadExitCode = uploadOutputFiles(JobStatus.ERROR_V) ? valExitCode : -1;

				changeStatus(JobStatus.ERROR_V);
				return valUploadExitCode;
			}

			if (!uploadOutputFiles(JobStatus.DONE)) {
				logger.log(Level.SEVERE, "Failed to upload output files");
				return -1;
			}

			cleanupProcesses();

			return 0;
		}
		catch (final Exception e) {
			logger.log(Level.SEVERE, "Unable to handle job", e);
			final StringBuilder sb = new StringBuilder("ERROR! Unable to handle job: " + e + "\n\r");
			for (final StackTraceElement elem : e.getStackTrace()) {
				sb.append(elem);
				sb.append("\n\r");
			}
			commander.q_api.putJobLog(queueId, "trace", sb.toString());
			return -1;
		}
	}

	/**
	 * @param command
	 * @param arguments
	 * @param timeout
	 * @return <code>0</code> if everything went fine, a positive number with the process exit code (which would mean a problem) and a negative error code in case of timeout or other supervised
	 *         execution errors
	 */
	private int executeCommand(final String command, final List<String> arguments) {

		logger.log(Level.INFO, "Starting execution of command: " + command);

		final List<String> cmd = new LinkedList<>();

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

		// Check if we can put the payload in its own container
		// TODO: Put back later
		// Containerizer cont = ContainerizerFactory.getContainerizer();
		// if (cont != null) {
		// monitor.sendParameter("containerLayer", Integer.valueOf(2));
		// cmd = cont.containerize(String.join(" ", cmd));
		// }

		logger.log(Level.INFO, "Executing: " + cmd + ", arguments is " + arguments + " pid: " + pid);

		final ProcessBuilder pBuilder = new ProcessBuilder(cmd);

		final HashMap<String, String> environment_packages = getJobPackagesEnvironment();
		final Map<String, String> processEnv = pBuilder.environment();
		final HashMap<String, String> jBoxEnv = ConfigUtils.exportJBoxVariables();

		processEnv.putAll(environment_packages);
		processEnv.putAll(loadJDLEnvironmentVariables());
		processEnv.putAll(jBoxEnv);
		processEnv.put("JALIEN_TOKEN_CERT", tokenCert);
		processEnv.put("JALIEN_TOKEN_KEY", tokenKey);
		processEnv.put("ALIEN_JOB_TOKEN", legacyToken); // add legacy token
		processEnv.put("ALIEN_PROC_ID", String.valueOf(queueId));
		processEnv.put("ALIEN_SITE", siteMap.get("Site").toString());
		processEnv.put("TMPDIR", currentDir.getAbsolutePath() + "/tmp");

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
			sun.misc.Signal.handle(new sun.misc.Signal("TERM"), sig -> {
				if (payload.isAlive()) {
					logger.log(Level.SEVERE, "SIGTERM received. Killing payload");
					payload.destroyForcibly();
				}
			});

			payload.waitFor(ttl, TimeUnit.SECONDS);

			if (payload.isAlive()) {
				payload.destroyForcibly();
				logger.log(Level.SEVERE, "Payload process destroyed by timeout in wrapper!");
			}
			logger.log(Level.SEVERE, "Payload has finished execution.");
		}
		catch (final InterruptedException e) {
			logger.log(Level.INFO, "Interrupted while waiting for process to finish execution" + e);
		}

		return payload.exitValue();
	}

	private int execute() {
		commander.q_api.putJobLog(queueId, "trace", "Starting execution");

		changeStatus(JobStatus.RUNNING);
		final int code = executeCommand(jdl.gets("Executable"), jdl.getArguments());

		return code;
	}

	private int validate() {
		int code = 0;

		final String validation = jdl.gets("ValidationCommand");

		if (validation != null) {
			commander.q_api.putJobLog(queueId, "trace", "Starting validation");
			code = executeCommand(validation, null);
		}

		return code;
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

		if (iFiles == null) {
			logger.log(Level.WARNING, "No requested files could be located");
			commander.q_api.putJobLog(queueId, "trace", "ERROR: No requested files could be located: getLFNs returned null");
			return false;
		}

		if (iFiles.size() != filesToDownload.size()) {
			logger.log(Level.WARNING, "Not all requested files could be located");
			commander.q_api.putJobLog(queueId, "trace", "ERROR: Not all requested files could be located. Located files " + iFiles.size() + ", but expected " + filesToDownload.size());
			return false;
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
				commander.q_api.putJobLog(queueId, "trace", "ERROR: Too many occurences of " + l.getFileName() + " in " + currentDir.getAbsolutePath());
				return false;
			}

			localFiles.put(l, localFile);
		}

		for (final Map.Entry<LFN, File> entry : localFiles.entrySet()) {
			final List<PFN> pfns = c_api.getPFNsToRead(entry.getKey(), null, null);

			if (pfns == null || pfns.size() == 0) {
				logger.log(Level.WARNING, "No replicas of " + entry.getKey().getCanonicalName() + " to read from");
				commander.q_api.putJobLog(queueId, "trace", "ERROR: No replicas of " + entry.getKey().getCanonicalName() + " to read from");
				return false;
			}

			final GUID g = pfns.iterator().next().getGuid();

			commander.q_api.putJobLog(queueId, "trace", "Getting InputFile: " + entry.getKey().getCanonicalName());

			logger.log(Level.INFO, g + ". entry.getvalue(): " + entry.getValue());
			commander.q_api.putJobLog(queueId, "trace", g + ". entry.getvalue(): " + entry.getValue());

			final StringBuilder errorMessage = new StringBuilder();

			final File f = IOUtils.get(g, entry.getValue(), errorMessage);

			if (f == null) {
				logger.log(Level.WARNING, "Could not download " + entry.getKey().getCanonicalName() + " to " + entry.getValue().getAbsolutePath());

				commander.q_api.putJobLog(queueId, "trace",
						"ERROR: Could not download " + entry.getKey().getCanonicalName() + " to " + entry.getValue().getAbsolutePath() + " due to:\n" + errorMessage);

				return false;
			}
		}

		dumpInputDataList();

		logger.log(Level.INFO, "Sandbox populated: " + currentDir.getAbsolutePath());

		return true;

	}

	private void dumpInputDataList() {
		// creates xml file with the InputData
		try {
			final String list = jdl.gets("InputDataList");

			if (list == null)
				return;

			logger.log(Level.INFO, "Going to create XML: " + list);

			final String format = jdl.gets("InputDataListFormat");
			if (format == null || !"xml-single".equals(format)) {
				logger.log(Level.WARNING, "XML format not understood");
				return;
			}

			final XmlCollection c = new XmlCollection();
			c.setName("jobinputdata");
			final List<String> datalist = jdl.getInputData(true);

			// TODO: Change
			for (final String s : datalist) {
				final LFN l = c_api.getLFN(s);
				if (l == null)
					continue;
				c.add(l);
			}

			final String content = c.toString();

			Files.write(Paths.get(currentDir.getAbsolutePath() + "/" + list), content.getBytes());

		}
		catch (final Exception e) {
			logger.log(Level.WARNING, "Problem dumping XML: " + e.toString());
		}

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

			if (!packs.containsKey("APISCONFIG"))
				packages.append(voalice + "APISCONFIG,");

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
		boolean uploadedAllOutFiles = true;
		boolean uploadedNotAllCopies = false;

		logger.log(Level.INFO, "Uploading output for: " + jdl);

		final String outputDir = getJobOutputDir(exitStatus);

		commander.q_api.putJobLog(queueId, "trace", "Going to uploadOutputFiles(exitStatus=" + exitStatus + ", outputDir=" + outputDir + ")");

		changeStatus(JobStatus.SAVING);

		logger.log(Level.INFO, "queueId: " + queueId);
		logger.log(Level.INFO, "outputDir: " + outputDir);
		logger.log(Level.INFO, "We are the current user: " + commander.getUser().getName());

		// `mkdir -p`, returning the new or existing LFN, when it's a directory or `null` in case of any error creating it
		final LFN outDir = c_api.createCatalogueDirectory(outputDir, true);
		if (outDir == null) {
			logger.log(Level.SEVERE, "Error creating the OutputDir [" + outputDir + "].");
			commander.q_api.putJobLog(queueId, "trace", "Can't create the output directory " + outputDir);
			if (!exitStatus.toString().contains("ERROR"))
				changeStatus(JobStatus.ERROR_SV);
			return false;
		}

		final ArrayList<OutputEntry> archivesToUpload = new ArrayList<>();
		final ArrayList<OutputEntry> standaloneFilesToUpload = new ArrayList<>();
		final ArrayList<String> allArchiveEntries = new ArrayList<>();

		final ArrayList<String> outputTags = getOutputTags(exitStatus);
		for (final String tag : outputTags) {
			try {
				final ParsedOutput filesTable = new ParsedOutput(queueId, jdl, currentDir.getAbsolutePath(), tag);
				for (final OutputEntry entry : filesTable.getEntries()) {
					if (entry.isArchive()) {
						logger.log(Level.INFO, "This is an archive: " + entry.getName());
						final ArrayList<String> archiveEntries = entry.createZip(currentDir.getAbsolutePath());
						if (archiveEntries.size() == 0) {
							logger.log(Level.WARNING, "Ignoring empty archive: " + entry.getName());
							commander.q_api.putJobLog(queueId, "trace", "Ignoring empty archive: " + entry.getName());
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
							commander.q_api.putJobLog(queueId, "trace", "The following file has size 0 and will be ignored: " + entry.getName());
						}
						else {
							standaloneFilesToUpload.add(entry);
							logger.log(Level.INFO, "Adding to standalone: " + entry.getName());
						}
					}
				}
			}
			catch (final NullPointerException ex) {
				logger.log(Level.SEVERE,
						"A required outputfile for an archive was NOT found! Aborting: " + ex.getMessage());
				commander.q_api.putJobLog(queueId, "trace",
						"Error: A required outputfile for an archive was NOT found! Aborting: " + ex.getMessage());
				if (!exitStatus.toString().contains("ERROR"))
					changeStatus(JobStatus.ERROR_S);
				return false;
			}
		}

		final ArrayList<OutputEntry> toUpload = mergeAndRemoveDuplicateEntries(standaloneFilesToUpload, archivesToUpload, allArchiveEntries);
		for (final OutputEntry entry : toUpload) {
			try {
				final File localFile = new File(currentDir.getAbsolutePath() + "/" + entry.getName());
				logger.log(Level.INFO, "Processing output file: " + localFile);

				if (localFile.exists() && localFile.isFile() && localFile.canRead() && localFile.length() > 0) {
					commander.q_api.putJobLog(queueId, "trace", "Uploading: " + entry.getName() + " to " + outputDir);

					final List<String> cpOptions = new ArrayList<>();
					cpOptions.add("-w");
					cpOptions.add("-S");

					if (entry.getOptions() != null && entry.getOptions().length() > 0)
						cpOptions.add(entry.getOptions());
					else
						cpOptions.add("disk:2");

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
						commander.q_api.putJobLog(queueId, "trace", "Failed to upload to " + outputDir + "/" + entry.getName() + ": " + out.toString());
						break;
					}

					// success, but could all the copies requested in the JDL be created as per user specs?
					if (output_upload.contains("requested replicas could be uploaded")) {
						// partial success, will lead to a DONE_WARN state
						uploadedNotAllCopies = true;
						commander.q_api.putJobLog(queueId, "trace", output_upload);
					}
					else
						commander.q_api.putJobLog(queueId, "trace", uploadResult.getCanonicalName() + ": uploaded as requested");
				}
				else {
					logger.log(Level.WARNING, "Can't upload output file " + localFile.getName() + ", does not exist or has zero size.");
					commander.q_api.putJobLog(queueId, "trace", "Can't upload output file " + localFile.getName() + ", does not exist or has zero size.");
				}
			}
			catch (final IOException e) {
				logger.log(Level.WARNING, "IOException received while attempting to upload " + entry.getName(), e);
				commander.q_api.putJobLog(queueId, "trace", "Failed to upload " + entry.getName() + " due to: " + e.getMessage());
				uploadedAllOutFiles = false;
			}
		}
		createAndAddResultsJDL(null); // Not really used. Set to null for now.

		if (!uploadedAllOutFiles && exitStatus == JobStatus.DONE) {
			changeStatus(JobStatus.ERROR_SV);
			return false;
		} // else
			// changeStatus(JobStatus.SAVED); TODO: To be put back later if still needed
		if (exitStatus == JobStatus.DONE) {
			registerEntries(toUpload, outputDir);

			if (uploadedNotAllCopies)
				changeStatus(JobStatus.DONE_WARN);
			else
				changeStatus(JobStatus.DONE);
		}
		else
			changeStatus(exitStatus);

		return uploadedAllOutFiles;
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
	 */
	public void changeStatus(final JobStatus newStatus) {
		jobStatus = newStatus;

		final HashMap<String, Object> extrafields = new HashMap<>();
		extrafields.put("exechost", ceHost);

		// if final status with saved files, we set the path
		if (jobStatus == JobStatus.DONE || jobStatus == JobStatus.DONE_WARN || jobStatus == JobStatus.ERROR_E || jobStatus == JobStatus.ERROR_V)
			extrafields.put("path", getJobOutputDir(newStatus));
		else if (jobStatus == JobStatus.RUNNING) {
			extrafields.put("spyurl", hostName + ":" + TomcatServer.getPort());
			extrafields.put("node", hostName);
		}

		try {
			// Set the updated status
			TaskQueueApiUtils.setJobStatus(queueId, newStatus, extrafields);

			// TODO: Confirm(?) and remove
			// Wait 10s, and set status once more, in case the first attempt was not registered (high load?)
			// Thread.sleep(10 * 1000);
			// TaskQueueApiUtils.setJobStatus(queueId, newStatus, extrafields);

			// Also write status to file for the JobAgent to see
			Files.writeString(Paths.get(currentDir.getAbsolutePath() + "/.jobstatus"), newStatus.name());
		}
		catch (final Exception e) {
			logger.log(Level.WARNING, "An error occurred when attempting to change current job status: " + e);
		}

		synchronized (statusSenderThread) {
			statusSenderThread.notifyAll();
		}
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

	private static String getTraceFromFile() {
		final File traceFile = new File(".alienValidation.trace");

		if (traceFile.exists()) {
			try {
				return new String(Files.readAllBytes(traceFile.toPath()));
			}
			catch (final Exception e) {
				logger.log(Level.WARNING, "An error occurred when reading .alienValidation.trace: " + e);
			}
		}
		logger.log(Level.INFO, ".alienValidation.trace does not exist.");
		return null;
	}

	private void createAndAddResultsJDL(@SuppressWarnings("unused") final ParsedOutput filesTable) {

		/*
		 * final ArrayList<String> jdlOutput = new ArrayList<>();
		 * for (final OutputEntry entry : filesTable.getEntries()) {
		 *
		 * String entryString = entry.getName();
		 * File entryFile = new File(currentDir.getAbsolutePath() + "/" + entryString);
		 *
		 * entryString += ";" + String.valueOf(entryFile.length());
		 *
		 * try {
		 * entryString += ";" + IOUtils.getMD5(entryFile);
		 * }
		 * catch (IOException e) {
		 * logger.log(Level.WARNING, "Could not generate MD5 for a file: " + e);
		 * }
		 *
		 * jdlOutput.add(entryString);
		 *
		 * // Also add the archive files to outputlist
		 * if (entry.isArchive()) {
		 *
		 * final ArrayList<String> archiveFiles = entry.getFilesIncluded();
		 * final HashMap<String, Long> archiveSizes = entry.getSizesIncluded();
		 * final HashMap<String, String> archiveMd5s = entry.getMD5sIncluded();
		 * for (final String archiveEntry : archiveFiles) {
		 *
		 * String archiveEntryString = archiveEntry;
		 *
		 * archiveEntryString += ";" + archiveSizes.get(archiveEntry);
		 * archiveEntryString += ";" + archiveMd5s.get(archiveEntry);
		 * archiveEntryString += ";" + entry.getName(); // name of its archive
		 *
		 * jdlOutput.add(archiveEntryString);
		 * }
		 * }
		 *
		 * }
		 * jdl.set("OutputFiles", "\n" + String.join("\n", jdlOutput));
		 */

		TaskQueueApiUtils.addResultsJdl(jdl, queueId);
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

	/**
	 * Cleanup processes, using a specialised script in CVMFS
	 *
	 * @return script exit code, or -1 in case of error
	 */
	private int cleanupProcesses() {
		final File cleanupScript = new File(CVMFS.getCleanupScript());

		if (!cleanupScript.exists()) {
			logger.log(Level.WARNING, "Script for process cleanup not found in: " + cleanupScript.getAbsolutePath());
			return -1;
		}

		try {
			final Process process = Runtime.getRuntime().exec((cleanupScript.getAbsolutePath() + " -v -m ALIEN_PROC_ID=" + queueId + " $$ -KILL"));
			return process.waitFor();
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
	private void registerEntries(final ArrayList<OutputEntry> entries, final String outputDir) {
		for (final OutputEntry entry : entries) {
			final boolean status = CatalogueApiUtils.registerEntry(entry, outputDir + "/", UserFactory.getByUsername(username));
			commander.q_api.putJobLog(queueId, "trace", "Registering: " + entry.getName() + ". Return status: " + status);
		}
	}

	private ArrayList<String> getOutputTags(final JobStatus exitStatus) {
		final ArrayList<String> tags = new ArrayList<>();

		if (exitStatus == JobStatus.ERROR_E) {
			if (jdl.gets("OutputErrorE") == null) {
				commander.q_api.putJobLog(queueId, "trace", "No output given for ERROR_E in JDL. Defaulting to std*");
				jdl.set("OutputErrorE", "log_archive.zip:std*@disk=1"); // set a default if nothing is provided
			}
			tags.add("OutputErrorE");
		}
		else {
			if (jdl.gets("OutputArchive") != null)
				tags.add("OutputArchive");
			if (jdl.gets("OutputFile") != null)
				tags.add("OutputFile");
			if (jdl.gets("Output") != null)
				tags.add("Output");
		}

		return tags;
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

}
