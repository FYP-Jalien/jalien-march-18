package alien.site.supercomputing.titan;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;

import alien.api.JBoxServer;
import alien.api.catalogue.CatalogueApiUtils;
import alien.api.taskQueue.TaskQueueApiUtils;
import alien.catalogue.BookingTable.BOOKING_STATE;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.io.IOUtils;
import alien.io.Transfer;
import alien.io.protocols.Protocol;
import alien.monitoring.MonitorFactory;
import alien.shell.commands.JAliEnCOMMander;
import alien.site.OutputEntry;
import alien.site.ParsedOutput;
import alien.taskQueue.JDL;
import alien.taskQueue.Job;
import alien.taskQueue.JobStatus;
import apmon.ApMon;
import apmon.ApMonException;
import lia.util.Utils;

/**
 * @author psvirin
 */
public class JobUploader extends Thread {
	private TitanJobStatus js;
	private String dbname;
	private Long queueId;
	private Integer resubmission;
	private JDL jdl;

	private String jobWorkdir;
	private JobStatus jobStatus;

	private final String username;

	/**
	 * CE name
	 */
	public static String ce;

	/**
	 * Host name
	 */
	public static String hostName;

	/**
	 * Default output prefix
	 */
	public static String defaultOutputDirPrefix;

	private static final ApMon apmon = MonitorFactory.getApMonSender();
	private final JAliEnCOMMander commander = JAliEnCOMMander.getInstance();
	private final CatalogueApiUtils c_api = new CatalogueApiUtils(commander);

	/**
	 * @param js
	 */
	public JobUploader(final TitanJobStatus js) {
		this.js = js;
		if (js.executionCode != 0)
			jobStatus = JobStatus.ERROR_E;
		else if (js.validationCode != 0)
			jobStatus = JobStatus.ERROR_V;
		else
			jobStatus = JobStatus.DONE;
		dbname = js.batch.dbName;
		username = "";
	}

	@Override
	public void run() {
		queueId = js.queueId;
		resubmission = js.resubmission;
		System.err.println(String.format("Uploading job: %d", queueId));
		jobWorkdir = js.jobFolder;
		// File tempDir = new File(js.jobFolder);

		String jdl_content = null;
		try {
			final byte[] encoded = Files.readAllBytes(Paths.get(js.jobFolder + "/jdl"));
			jdl_content = new String(encoded, Charset.defaultCharset());
		}
		catch (final IOException e) {
			System.err.println("Unable to read JDL file: " + e.getMessage());
		}
		if (jdl_content != null) {
			jdl = null;
			try {
				jdl = new JDL(Job.sanitizeJDL(jdl_content));
			}
			catch (final IOException e) {
				System.err.println("Unable to parse JDL: " + e.getMessage());
			}
			if (jdl != null) {
				if (js.executionCode != 0) {
					changeStatus(queueId.longValue(), resubmission.intValue(), JobStatus.ERROR_E);
					final Vector<String> varnames = new Vector<>();
					varnames.add("host");
					varnames.add("statusID");
					varnames.add("jobID");
					final Vector<Object> varvalues = new Vector<>();
					varvalues.add(hostName);
					varvalues.add("-3");
					varvalues.add(queueId);
					try {
						apmon.sendParameters(ce + "_Jobs", String.format("%d", queueId), 2, varnames, varvalues);
					}
					catch (@SuppressWarnings("unused") ApMonException | IOException e) {
						// ignore
					}
				}
				else if (js.validationCode != 0) {
					changeStatus(queueId.longValue(), resubmission.intValue(), JobStatus.ERROR_V);
					final Vector<String> varnames = new Vector<>();
					varnames.add("host");
					varnames.add("statusID");
					varnames.add("jobID");
					final Vector<Object> varvalues = new Vector<>();
					varvalues.add(hostName);
					varvalues.add("-10");
					varvalues.add(queueId);
					try {
						apmon.sendParameters(ce + "_Jobs", String.format("%d", queueId), 2, varnames, varvalues);
					}
					catch (@SuppressWarnings("unused") ApMonException | IOException e) {
						// ignore
					}
				}
				else {
					changeStatus(queueId.longValue(), resubmission.intValue(), JobStatus.SAVING);
					final Vector<String> varnames = new Vector<>();
					varnames.add("host");
					varnames.add("statusID");
					varnames.add("jobID");
					final Vector<Object> varvalues = new Vector<>();
					varvalues.add(hostName);
					varvalues.add("11");
					varvalues.add(queueId);
					try {
						apmon.sendParameters(ce + "_Jobs", String.format("%d", queueId), 2, varnames, varvalues);
					}
					catch (@SuppressWarnings("unused") ApMonException | IOException e) {
						// ignore
					}
				}
				uploadOutputFiles(); // upload data
				cleanup();
				System.err.println(String.format("Upload job %d finished", queueId));

				int i = 50;
				while (i-- > 0) {
					try (Connection connection = DriverManager.getConnection(dbname); Statement statement = connection.createStatement()) {
						statement.executeUpdate(String.format("UPDATE alien_jobs SET status='I' WHERE rank=%d", Integer.valueOf(js.rank)));
					}
					catch (final SQLException e) {
						System.err.println("Update job state to I failed: " + e.getMessage());
						try {
							Thread.sleep(2000);
						}
						catch (@SuppressWarnings("unused") final InterruptedException ei) {
							System.err.println("Sleep in DispatchSSLMTClient.getInstance has been interrupted");
						}
						continue;
					}
					return;
				}
			}
		}
	}

	/**
	 * @param dbn
	 *            database name
	 */
	public void setDbName(final String dbn) {
		dbname = dbn;
	}

	private void cleanup() {
		System.out.println("Cleaning up after execution...Removing sandbox: " + jobWorkdir);
		// Remove sandbox, TODO: use Java builtin
		// Utils.getOutput("rm -rf " + jobWorkdir);
		Utils.getOutput("cp -r " + jobWorkdir + " /lustre/atlas/scratch/psvirin/csc108/cleanup_folder");
		/*
		 * RES_WORKDIR_SIZE = ZERO;
		 * RES_VMEM = ZERO;
		 * RES_RMEM = ZERO;
		 * RES_VMEMMAX = ZERO;
		 * RES_RMEMMAX = ZERO;
		 * RES_MEMUSAGE = ZERO;
		 * RES_CPUTIME = ZERO;
		 * RES_CPUUSAGE = ZERO;
		 * RES_RESOURCEUSAGE = "";
		 * RES_RUNTIME = Long.valueOf(0);
		 * RES_FRUNTIME = "";
		 */
	}

	private boolean uploadOutputFiles() {
		boolean uploadedAllOutFiles = true;
		boolean uploadedNotAllCopies = false;

		commander.q_api.putJobLog(queueId.longValue(), resubmission.intValue(), "trace", "Going to uploadOutputFiles");

		// EXPERIMENTAL
		final String outputDir = getJobOutputDir();
		// final String outputDir = getJobOutputDir() + "/" + queueId;

		System.out.println("queueId: " + queueId);
		System.out.println("outputDir: " + outputDir);

		if (c_api.getLFN(outputDir) != null) {
			System.err.println("OutputDir [" + outputDir + "] already exists.");
			changeStatus(queueId.longValue(), resubmission.intValue(), JobStatus.ERROR_SV);

			final Vector<String> varnames = new Vector<>();
			varnames.add("host");
			varnames.add("statusID");
			varnames.add("jobID");
			final Vector<Object> varvalues = new Vector<>();
			varvalues.add(hostName);
			varvalues.add("-9");
			varvalues.add(queueId);
			try {
				apmon.sendParameters(ce + "_Jobs", String.format("%d", queueId), 2, varnames, varvalues);
				apmon.sendParameters("TaskQueue_Jobs_ALICE", String.format("%d", queueId), 3, varnames, varvalues);
			}
			catch (@SuppressWarnings("unused") ApMonException | IOException e) {
				// ignore
			}
			return false;
		}

		final LFN outDir = c_api.createCatalogueDirectory(outputDir, true);

		if (outDir == null) {
			System.err.println("Error creating the OutputDir [" + outputDir + "].");
			uploadedAllOutFiles = false;
		}
		else {
			String tag = "Output";
			if (jobStatus == JobStatus.ERROR_E)
				tag = "OutputErrorE";

			final ParsedOutput filesTable = new ParsedOutput(queueId.longValue(), jdl, jobWorkdir, tag);

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
						lfn.jobid = queueId.longValue();
						lfn.type = 'f';
						final GUID guid = GUIDUtils.createGuid(localFile, commander.getUser());
						lfn.guid = guid.guid;
						final ArrayList<String> exses = entry.getSEsDeprioritized();

						final List<PFN> pfns = c_api.getPFNsToWrite(lfn, guid, entry.getSEsPrioritized(), exses, entry.getQoS());

						System.out.println("LFN :" + lfn + "\npfns: " + pfns);

						commander.q_api.putJobLog(queueId.longValue(), resubmission.intValue(), "trace", "Uploading: " + lfn.getName());

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
		}

		if (jobStatus != JobStatus.ERROR_E && jobStatus != JobStatus.ERROR_V)
			if (uploadedNotAllCopies) {
				changeStatus(queueId.longValue(), resubmission.intValue(), JobStatus.DONE_WARN);
				final Vector<String> varnames = new Vector<>();
				varnames.add("host");
				varnames.add("statusID");
				varnames.add("jobID");
				final Vector<Object> varvalues = new Vector<>();
				varvalues.add(hostName);
				varvalues.add("16");
				varvalues.add(queueId);
				try {
					apmon.sendParameters(ce + "_Jobs", String.format("%d", queueId), 2, varnames, varvalues);
				}
				catch (@SuppressWarnings("unused") ApMonException | IOException e) {
					// ignore
				}
			}
			else if (uploadedAllOutFiles) {
				changeStatus(queueId.longValue(), resubmission.intValue(), JobStatus.DONE);
				final Vector<String> varnames = new Vector<>();
				varnames.add("host");
				varnames.add("statusID");
				varnames.add("jobID");
				final Vector<Object> varvalues = new Vector<>();
				varvalues.add(hostName);
				varvalues.add("15");
				varvalues.add(queueId);
				try {
					apmon.sendParameters(ce + "_Jobs", String.format("%d", queueId), 2, varnames, varvalues);
					apmon.sendParameters("TaskQueue_Jobs_ALICE", String.format("%d", queueId), 3, varnames, varvalues);
				}
				catch (@SuppressWarnings("unused") ApMonException | IOException e) {
					// ignore
				}
			}
			else {
				changeStatus(queueId.longValue(), resubmission.intValue(), JobStatus.ERROR_SV);
				final Vector<String> varnames = new Vector<>();
				varnames.add("host");
				varnames.add("statusID");
				varnames.add("jobID");
				final Vector<Object> varvalues = new Vector<>();
				varvalues.add(hostName);
				varvalues.add("-9");
				varvalues.add(queueId);
				try {
					apmon.sendParameters(ce + "_Jobs", String.format("%d", queueId), 2, varnames, varvalues);
				}
				catch (@SuppressWarnings("unused") ApMonException | IOException e) {
					// ignore
				}
			}

		return uploadedAllOutFiles;
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

	/**
	 * @param jobId
	 * @param resubmissionCount 
	 * @param newStatus
	 */
	public void changeStatus(final long jobId, final int resubmissionCount, final JobStatus newStatus) {
		// if final status with saved files, we set the path
		if (newStatus == JobStatus.DONE || newStatus == JobStatus.DONE_WARN || newStatus == JobStatus.ERROR_E || newStatus == JobStatus.ERROR_V) {
			final HashMap<String, Object> extrafields = new HashMap<>();
			extrafields.put("path", getJobOutputDir());

			TaskQueueApiUtils.setJobStatus(jobId, resubmissionCount, newStatus, extrafields);
		}
		else if (newStatus == JobStatus.RUNNING) {
			final HashMap<String, Object> extrafields = new HashMap<>();
			extrafields.put("spyurl", hostName + ":" + JBoxServer.getPort());
			extrafields.put("node", hostName);
			extrafields.put("exechost", hostName);

			TaskQueueApiUtils.setJobStatus(jobId, resubmissionCount, newStatus, extrafields);
		}
		else
			TaskQueueApiUtils.setJobStatus(jobId, resubmissionCount, newStatus);

		jobStatus = newStatus;

		return;
	}

}
