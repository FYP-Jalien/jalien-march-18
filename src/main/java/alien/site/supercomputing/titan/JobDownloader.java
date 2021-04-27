package alien.site.supercomputing.titan;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.sqlite.SQLiteConnection;

import alien.api.JBoxServer;
import alien.api.catalogue.CatalogueApiUtils;
import alien.api.taskQueue.GetMatchJob;
import alien.api.taskQueue.TaskQueueApiUtils;
import alien.catalogue.LFN;
import alien.catalogue.XmlCollection;
import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.shell.commands.JAliEnCOMMander;
import alien.site.TitanJobService;
import alien.site.packman.PackMan;
import alien.taskQueue.JDL;
import alien.taskQueue.Job;
import alien.taskQueue.JobStatus;
import apmon.ApMon;

/**
 * @author psvirin
 */
public class JobDownloader extends Thread {
	private final TitanJobStatus js;
	private String dbname;
	private JDL jdl;
	private Long queueId;
	private String username;
	private String jobToken;
	private String masterJobId;
	private String jobWorkdir;
	private File tempDir;
	private String workdir = null;
	private HashMap<String, Object> siteMap = new HashMap<>();

	private int workdirMaxSizeMB;
	private int jobMaxMemoryMB;
	private HashMap<String, Object> matchedJob = null;
	private int current_rank;

	private static boolean noMoreJobs = false;
	/**
	 * CE name
	 */
	public static String ce;
	/**
	 * Host name
	 */
	public static String hostName;
	/**
	 * Output prefix
	 */
	public static String defaultOutputDirPrefix;
	// private static List<String> fetchedJobs = new List<>();
	// private static List<Long> idleRanksToMark = new List<>();

	private static final Logger logger = ConfigUtils.getLogger(TitanJobService.class.getCanonicalName());
	private static final Monitor monitor = MonitorFactory.getMonitor(TitanJobService.class.getCanonicalName());
	private static final ApMon apmon = MonitorFactory.getApMonSender();
	private final JAliEnCOMMander commander = JAliEnCOMMander.getInstance();
	private final CatalogueApiUtils c_api = new CatalogueApiUtils(commander);

	/**
	 * PackMan instance
	 */
	public static PackMan packMan;

	/**
	 * @param js
	 * @param smap
	 */
	public JobDownloader(final TitanJobStatus js, final HashMap<String, Object> smap) {
		this.js = js;
		workdir = js.batch.jobWorkdir;
		siteMap = new HashMap<>(smap);
		dbname = js.batch.dbName;
		// System.out.println("dbname: " + dbname);
	}

	/**
	 *
	 */
	public static void initialize() {
		noMoreJobs = false;
	}

	@Override
	public void run() {
		try {
			logger.log(Level.INFO, "Trying to get a match...");
			System.out.println("Trying to get a match...");
			final long current_timestamp = System.currentTimeMillis() / 1000L;
			siteMap.put("TTL", Long.valueOf(js.batch.getTtlLeft(current_timestamp)));
			if (noMoreJobs) {
				System.out.println("no more jobs in the queue, not doing a call");
				System.out.println("Finishing downloader thread");
				return;
			}

			final GetMatchJob jobMatch = commander.q_api.getMatchJob(siteMap);
			System.out.println("Matching done");
			matchedJob = jobMatch.getMatchJob();

			// TODELETE
			if (matchedJob != null)
				System.out.println(matchedJob.toString());

			if (matchedJob != null && !matchedJob.containsKey("Error")) {
				jdl = new JDL(Job.sanitizeJDL((String) matchedJob.get("JDL")));
				// queueId = ((Long) matchedJob.get("queueId")).intValue();
				queueId = ((Long) matchedJob.get("queueId"));
				username = (String) matchedJob.get("User");
				jobToken = (String) matchedJob.get("jobToken");
				masterJobId = jdl.gets("MasterJobID");
				if (masterJobId == null)
					masterJobId = "0";

				// TODO: commander.setUser(username); commander.setSite(site);

				System.out.println(jdl.getExecutable());
				System.out.println(jdl.toString());
				logger.log(Level.INFO, jdl.toString());
				System.out.println("====================");
				System.out.println(username);
				System.out.println(queueId);
				System.out.println(jobToken);
				System.out.println("Masterjob id: " + masterJobId);

				// EXPERIMENTAL
				// for ORNL Titan
				current_rank = js.rank;

				System.out.println("Handling job");

				// process payload
				handleJob();
			}
			else if (matchedJob != null && matchedJob.containsKey("Error")) {
				logger.log(Level.INFO, (String) matchedJob.get("Error"));

				if (Integer.valueOf(3).equals(matchedJob.get("Code"))) {
					@SuppressWarnings("unchecked")
					final ArrayList<String> packToInstall = (ArrayList<String>) matchedJob.get("Packages");
					// monitor.sendParameter("ja_status", getJaStatusForML("INSTALLING_PKGS"));
					installPackages(packToInstall);
				}
				else if (Integer.valueOf(-2).equals(matchedJob.get("Code"))) {
					noMoreJobs = true;
					logger.log(Level.INFO, "Nothing to run for now, idling for a while");
					// count = 1; // breaking the loop
				}
			}
			/*
			 * else{
			 * // EXPERIMENTAL
			 * // for ORNL Titan
			 * logger.log(Level.INFO, "We didn't get anything back. Nothing to run right now. Idling 20secs zZz...");
			 * //break;
			 * }
			 */
		}
		catch (final Exception e) {
			logger.log(Level.INFO, "Error getting a matching job: " + e);
		}
		System.out.println("Finishing downloader thread");
	}

	private void handleJob() {
		// totalJobs++;
		try {
			logger.log(Level.INFO, "Started JA with: " + jdl);

			commander.q_api.putJobLog(queueId.longValue(), "trace", "Job preparing to run in: " + hostName);

			changeStatus(queueId, JobStatus.STARTED);

			Vector<String> varnames = new Vector<>();
			varnames.add("host");
			varnames.add("statusID");
			varnames.add("jobID");
			Vector<Object> varvalues = new Vector<>();
			varvalues.add(hostName);
			varvalues.add("7");
			varvalues.add(queueId);
			apmon.sendParameters(ce + "_Jobs", String.format("%d", queueId), 2, varnames, varvalues);

			if (!createWorkDir() || !getInputFiles()) {
				changeStatus(queueId, JobStatus.ERROR_IB);
				varnames = new Vector<>();
				varnames.add("host");
				varnames.add("statusID");
				varnames.add("jobID");
				varvalues = new Vector<>();
				varvalues.add(hostName);
				varvalues.add("-4");
				varvalues.add(queueId);
				apmon.sendParameters(ce + "_Jobs", String.format("%d", queueId), 2, varnames, varvalues);
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
			changeStatus(queueId, JobStatus.RUNNING);
			// also send Apmon message to alimonitor
			System.out.println("============== now running apmon call =========");
			try {
				// apmon.addJobToMonitor((int)(long)queueId, "" /*jobWorkdir*/, ce, hostName);
				varnames = new Vector<>();
				varnames.add("host");
				varnames.add("statusID");
				varnames.add("jobID");
				varnames.add("exechost");
				varvalues = new Vector<>();
				varvalues.add(hostName);
				varvalues.add("10");
				varvalues.add(queueId);
				// varvalues.add(ce);
				varvalues.add(hostName);
				apmon.sendParameters(ce + "_Jobs", String.format("%d", queueId), 2, varnames, varvalues);
			}
			catch (final Exception e) {
				System.err.println("Error running ApMon call: " + e.getMessage());
			}

			if (execute() < 0) {
				changeStatus(queueId, JobStatus.ERROR_E);
				varnames = new Vector<>();
				varnames.add("host");
				varnames.add("statusID");
				varnames.add("jobID");
				varvalues = new Vector<>();
				varvalues.add(hostName);
				varvalues.add("-3");
				varvalues.add(queueId);
				apmon.sendParameters(ce + "_Jobs", String.format("%d", queueId), 2, varnames, varvalues);
			}

		}
		catch (final Exception e) {
			System.err.println("Unable to handle job");
			e.printStackTrace();
		}
	}

	private int execute() {
		commander.q_api.putJobLog(queueId.longValue(), "trace", "Starting execution");
		int numRetries = 20;
		// EXPERIMENTAL
		// for ORNL Titan

		while (numRetries-- > 0)
			try {
				// Connection connection = DriverManager.getConnection(dbname);
				final Connection connection = DriverManager.getConnection(js.batch.dbName);
				((SQLiteConnection) connection).setBusyTimeout(3000);
				try (Statement statement = connection.createStatement()) {
					// setting variables
					final HashMap<String, String> alice_environment_packages = loadJDLEnvironmentVariables();

					// setting variables for packages
					final HashMap<String, String> environment_packages = getJobPackagesEnvironment();

					// try(PrintWriter out = new PrintWriter(tempDir + "/environment")){
					try (PrintWriter out = new PrintWriter(tempDir + "/environment")) {
						for (final Entry<String, String> e : alice_environment_packages.entrySet())
							out.println(String.format("export %s=%s", e.getKey(), e.getValue()));

						for (final Entry<String, String> e : environment_packages.entrySet())
							out.println(String.format(" export %s=%s", e.getKey(), e.getValue()));
					}

					final String validationCommand = jdl.gets("ValidationCommand");
					statement.executeUpdate(String.format(
							"UPDATE alien_jobs SET queue_id=%d, job_folder='%s', status='%s', executable='%s', validation='%s', environment='%s',user='%s', masterjob_id='%s' WHERE rank=%d",
							queueId, tempDir, "Q", getLocalCommand(jdl.gets("Executable"), jdl.getArguments()), validationCommand != null ? getLocalCommand(validationCommand, null) : "", "", username,
							masterJobId, Integer.valueOf(current_rank)));
				}

				// String.format("%d, %d, '%s', '%s', '%s', '%s', '%s','%s', '%s', %d, %d", Integer.valueOf(current_rank), queueId, "", "", tempDir, "Q",
				// getLocalCommand(jdl.gets("Executable"), jdl.getArguments()), validationCommand != null ? getLocalCommand(validationCommand, null) : "", "", Integer.valueOf(-1),
				// Integer.valueOf(-1));

				// queueId, tempDir, "Q",
				// getLocalCommand(jdl.gets("Executable"), jdl.getArguments()),
				// validationCommand!=null ? getLocalCommand(validationCommand, null) : "",
				// "", current_rank )

				break;
			}
			catch (final SQLException e) {
				System.err.println("Failed to insert job: " + e.getMessage());
				logger.log(Level.INFO, "Failed to insert job: " + e.getMessage());
				System.out.println("DBname: " + dbname);
				System.out.println("DBname: " + js.batch.dbName);
				System.out.println("Retrying...");
				try {
					Thread.sleep(2000);
				}
				catch (@SuppressWarnings("unused") final InterruptedException ei) {
					System.err.println("Sleep in DispatchSSLMTClient.getInstance has been interrupted");
				}
			}
			catch (final FileNotFoundException e) {
				System.err.println("Failed to write variables file: " + e.getMessage());
			}

		return 0;
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

		// JAVA 8
		String argString = "";
		if (arguments != null)
			for (final String s : arguments)
				argString += " " + s;

		return (fExe.getAbsolutePath() + argString);
	}
	// end EXPERIMENTAL

	private boolean createWorkDir() {
		logger.log(Level.INFO, "Creating sandbox and chdir");

		jobWorkdir = String.format("%s%s%d", workdir, defaultOutputDirPrefix, queueId);

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

		commander.q_api.putJobLog(queueId.longValue(), "trace", "Created workdir: " + jobWorkdir);
		// TODO: create the extra directories

		return true;
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
			commander.q_api.putJobLog(queueId.longValue(), "trace", "Disk requested: " + workdirMaxSizeMB);
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

			commander.q_api.putJobLog(queueId.longValue(), "trace", "Memory requested: " + jobMaxMemoryMB);
		}
		else
			jobMaxMemoryMB = 0;

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

	/*
	 * private boolean getInputFiles() {
	 * final Set<String> filesToDownload = new HashSet<>();
	 *
	 * List<String> list = jdl.getInputFiles(false);
	 *
	 * if (list != null)
	 * filesToDownload.addAll(list);
	 *
	 * list = jdl.getInputData(false);
	 *
	 * if (list != null)
	 * filesToDownload.addAll(list);
	 *
	 * String s = jdl.getExecutable();
	 *
	 * if (s != null)
	 * filesToDownload.add(s);
	 *
	 * s = jdl.gets("ValidationCommand");
	 *
	 * if (s != null)
	 * filesToDownload.add(s);
	 *
	 * final List<LFN> iFiles = c_api.getLFNs(filesToDownload, true, false);
	 *
	 * if (iFiles == null || iFiles.size() != filesToDownload.size()) {
	 * System.out.println("Not all requested files could be located");
	 * return false;
	 * }
	 *
	 * final Map<LFN, File> localFiles = new HashMap<>();
	 *
	 * for (final LFN l : iFiles) {
	 * File localFile = new File(tempDir, l.getFileName());
	 * System.out.println("Getting file: " + localFile.getAbsolutePath());
	 *
	 * final int i = 0;
	 *
	 * while (localFile.exists() && i < 100000)
	 * localFile = new File(tempDir, l.getFileName() + "." + i);
	 *
	 * if (localFile.exists()) {
	 * System.out.println("Too many occurences of " + l.getFileName() + " in "
	 * + tempDir.getAbsolutePath());
	 * return false;
	 * }
	 *
	 * localFiles.put(l, localFile);
	 * }
	 *
	 * for (final Map.Entry<LFN, File> entry : localFiles.entrySet()) {
	 * final List<PFN> pfns = c_api.getPFNsToRead(entry.getKey(), null, null);
	 *
	 * if (pfns == null || pfns.size() == 0) {
	 * System.out.println("No replicas of " + entry.getKey().getCanonicalName() +
	 * " to read from");
	 * return false;
	 * }
	 *
	 * final GUID g = pfns.iterator().next().getGuid();
	 * commander.q_api.putJobLog(queueId, "trace", "Getting InputFile: " +
	 * entry.getKey().getCanonicalName());
	 * final File f = IOUtils.get(g, entry.getValue());
	 *
	 * if (f == null) {
	 * System.out.println("Could not download " + entry.getKey().getCanonicalName() +
	 * " to " + entry.getValue().getAbsolutePath());
	 * return false;
	 * }
	 * }
	 *
	 * dumpInputDataList();
	 *
	 * System.out.println("Sandbox prepared : " + tempDir.getAbsolutePath());
	 *
	 * return true;
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

		final FileDownloadController fdc = FileDownloadController.getInstance();
		final FileDownloadApplication fda = fdc.applyForDownload(iFiles);
		System.out.println("We've applied for downloads: " + fda);
		fda.print();

		try {
			synchronized (fda) {
				while (!fda.isCompleted())
					fda.wait(1000 * 10);
			}
		}
		catch (@SuppressWarnings("unused") final InterruptedException e) {
			// ignore
		}
		dumpInputDataList();
		System.out.println("Finalizing download.");

		final List<Pair<LFN, String>> fList = fda.getResults();
		if (fda.isCompleted()) {
			for (final Pair<LFN, String> p : fList) {
				if (p.getSecond() == null) {
					System.out.println(p.getFirst().getCanonicalName() + " is null");
					return false;
				}
				System.out.println(p.getFirst().getFileName() + " : " + p.getSecond());
				// copy files to local folder

				System.out.println("Now copying: " + p.getSecond() + " to " + tempDir.getAbsolutePath() + "/" + p.getFirst().getFileName());

				try (FileInputStream fis = new FileInputStream(p.getSecond());
						FileChannel source = fis.getChannel();
						FileOutputStream fos = new FileOutputStream(tempDir.getAbsolutePath() + "/" + p.getFirst().getFileName());
						FileChannel destination = fos.getChannel()) {
					// source = new FileInputStream(TempFileManager.getAny(guid)).getChannel();
					destination.transferFrom(source, 0, source.size());
				}
				catch (final Exception e) {
					System.err.println("Exception happened on file copy: " + e.getMessage());
					e.printStackTrace();
				}
			}
			System.out.println("Sandbox prepared : " + tempDir.getAbsolutePath());
			return true;
		}

		return false;

		/*
		 * for (final LFN l : iFiles) {
		 * File localFile = new File(tempDir, l.getFileName());
		 * System.out.println("Getting file: " + localFile.getAbsolutePath());
		 *
		 * final int i = 0;
		 *
		 * while (localFile.exists() && i < 100000)
		 * localFile = new File(tempDir, l.getFileName() + "." + i);
		 *
		 * if (localFile.exists()) {
		 * System.out.println("Too many occurences of " + l.getFileName() + " in "
		 * + tempDir.getAbsolutePath());
		 * return false;
		 * }
		 *
		 * localFiles.put(l, localFile);
		 * }
		 */

		/*
		 * for (final Map.Entry<LFN, File> entry : localFiles.entrySet()) {
		 * final List<PFN> pfns = c_api.getPFNsToRead(entry.getKey(), null, null);
		 *
		 * if (pfns == null || pfns.size() == 0) {
		 * System.out.println("No replicas of " + entry.getKey().getCanonicalName() +
		 * " to read from");
		 * return false;
		 * }
		 *
		 * final GUID g = pfns.iterator().next().getGuid();
		 * commander.q_api.putJobLog(queueId, "trace", "Getting InputFile: " +
		 * entry.getKey().getCanonicalName());
		 * final File f = IOUtils.get(g, entry.getValue());
		 *
		 * if (f == null) {
		 * System.out.println("Could not download " + entry.getKey().getCanonicalName() +
		 * " to " + entry.getValue().getAbsolutePath());
		 * return false;
		 * }
		 * }
		 */

		// System.out.println("Sandbox prepared : " + tempDir.getAbsolutePath());

		// return true;
	}

	private void dumpInputDataList() {
		// creates xml file with the InputData
		try {
			final String list = jdl.gets("InputDataList");

			if (list == null)
				return;

			System.out.println("Going to create XML: " + list);

			final String format = jdl.gets("InputDataListFormat");
			if (format == null || !"xml-single".equals(format)) {
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

	/*
	 * private long ttlForJob() {
	 * final Integer iTTL = jdl.getInteger("TTL");
	 *
	 * int ttl = (iTTL != null ? iTTL.intValue() : 0) + 300;
	 * commander.q_api.putJobLog(queueId.longValue(), "trace", "Job asks to run for " + ttl + " seconds");
	 *
	 * final String proxyttl = jdl.gets("ProxyTTL");
	 * if (proxyttl != null) {
	 * ttl = ((Integer) siteMap.get("TTL")).intValue() - 600;
	 * commander.q_api.putJobLog(queueId.longValue(), "trace", "ProxyTTL enabled, running for " + ttl + " seconds");
	 * }
	 *
	 * return ttl;
	 * }
	 */

	/**
	 * @param queueId
	 * @param newStatus
	 */
	/*
	 * public void changeStatus(final Long queueId, final JobStatus newStatus) {
	 * // if final status with saved files, we set the path
	 * //if (newStatus == JobStatus.DONE || newStatus == JobStatus.DONE_WARN || newStatus == JobStatus.ERROR_E || newStatus == JobStatus.ERROR_V) {
	 * // final HashMap<String, Object> extrafields = new HashMap<>();
	 * // extrafields.put("path", getJobOutputDir());
	 * //
	 * // TaskQueueApiUtils.setJobStatus(queueId, newStatus, extrafields);
	 * //} else
	 * if (newStatus == JobStatus.RUNNING) {
	 * final HashMap<String, Object> extrafields = new HashMap<>();
	 *
	 * extrafields.put("spyurl", hostName + ":" + JBoxServer.getPort());
	 * extrafields.put("node", hostName);
	 *
	 * TaskQueueApiUtils.setJobStatus(queueId, newStatus, extrafields);
	 * } else
	 * TaskQueueApiUtils.setJobStatus(queueId, newStatus);
	 *
	 * jobStatus = newStatus;
	 *
	 * return;
	 * }
	 */

	/*
	 * public void changeStatus(final Long queueId, final JobStatus newStatus) {
	 * // if final status with saved files, we set the path
	 * if (newStatus == JobStatus.DONE || newStatus == JobStatus.DONE_WARN || newStatus == JobStatus.ERROR_E || newStatus == JobStatus.ERROR_V) {
	 * final HashMap<String, Object> extrafields = new HashMap<>();
	 * //extrafields.put("path", getJobOutputDir());
	 * extrafields.put("path","/tmp");
	 *
	 * TaskQueueApiUtils.setJobStatus(queueId, newStatus, extrafields);
	 * }
	 * else
	 * if (newStatus == JobStatus.RUNNING) {
	 * final HashMap<String, Object> extrafields = new HashMap<>();
	 * extrafields.put("spyurl", hostName + ":" + JBoxServer.getPort());
	 * extrafields.put("node", hostName);
	 *
	 * TaskQueueApiUtils.setJobStatus(queueId, newStatus, extrafields);
	 * }
	 * else
	 * TaskQueueApiUtils.setJobStatus(queueId, newStatus);
	 *
	 * jobStatus = newStatus;
	 *
	 * return;
	 * }
	 */

	public static void changeStatus(final Long queueId, final JobStatus newStatus) {
		final HashMap<String, Object> extrafields = new HashMap<>();
		System.out.println("Exechost for changeStatus: " + ce);
		// extrafields.put("exechost", ce);
		extrafields.put("exechost", hostName);
		// if final status with saved files, we set the path
		if (newStatus == JobStatus.DONE || newStatus == JobStatus.DONE_WARN || newStatus == JobStatus.ERROR_E || newStatus == JobStatus.ERROR_V) {
			// extrafields.put("path", getJobOutputDir());
			extrafields.put("path", "/tmp");

			TaskQueueApiUtils.setJobStatus(queueId.longValue(), newStatus, extrafields);
		}
		else if (newStatus == JobStatus.RUNNING) {
			extrafields.put("spyurl", hostName + ":" + JBoxServer.getPort());
			extrafields.put("node", hostName);

			TaskQueueApiUtils.setJobStatus(queueId.longValue(), newStatus, extrafields);
		}
		else
			TaskQueueApiUtils.setJobStatus(queueId.longValue(), newStatus);

		// jobStatus = newStatus;

	}

	/**
	 * @param dbn
	 */
	public void setDbName(final String dbn) {
		dbname = dbn;
	}
}
// =========================================================================================================
// ================ JobDownloader finished
