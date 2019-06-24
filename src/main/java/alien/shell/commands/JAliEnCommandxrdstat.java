package alien.shell.commands;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import alien.catalogue.FileSystemUtils;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.io.IOUtils;
import alien.io.protocols.Factory;
import alien.io.protocols.TempFileManager;
import alien.io.protocols.Xrootd;
import alien.se.SE;
import alien.se.SEUtils;
import alien.shell.ShellColor;
import alien.taskQueue.JDL;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import lazyj.Format;

/**
 * @author costing
 * @since 2018-08-21
 */
public class JAliEnCommandxrdstat extends JAliEnBaseCommand {
	private LinkedHashSet<String> alPaths = null;

	private ArrayList<Long> jobIDs = null;

	private boolean bDownload = false;

	private final Set<SE> ses = new HashSet<>();

	private boolean printCommand = false;

	private boolean ignoreStat = false;

	private boolean verbose = false;

	@Override
	public void run() {
		Xrootd xrootd = null;

		if (jobIDs != null) {
			for (final Long jobID : jobIDs) {
				final String jdl = commander.q_api.getJDL(jobID.longValue());

				if (jdl == null) {
					commander.printErrln("Cannot retrieve JDL of job ID " + jobID);
				}
				else {
					try {
						final JDL j = new JDL(jdl);

						final List<String> dataFiles = j.getInputData(true);

						if (dataFiles != null && dataFiles.size() > 0)
							this.alPaths.addAll(dataFiles);
						else
							commander.printErrln("Job ID " + jobID + " doesn't have input data, nothing to check for it");
					}
					catch (final IOException ioe) {
						commander.printErrln("Cannot parse the JDL of job ID " + jobID + " : " + ioe.getMessage());
					}
				}
			}
		}

		for (final String lfnName : this.alPaths) {
			final LFN lfn = commander.c_api.getRealLFN(FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), lfnName));

			final GUID referenceGUID;

			PFN onePfnToCheck = null;

			if (lfn == null) {
				if (GUIDUtils.isValidGUID(lfnName)) {
					referenceGUID = commander.c_api.getGUID(lfnName);

					if (referenceGUID == null) {
						commander.printErrln("This GUID does not exist in the catalogue: " + lfnName);
						continue;
					}
				}
				else {
					if (lfnName.startsWith("root://")) {
						// is it a GUID-based PFN?

						final int idx = lfnName.lastIndexOf('/');

						final String lastToken = lfnName.substring(idx + 1);

						if (GUIDUtils.isValidGUID(lastToken))
							referenceGUID = commander.c_api.getGUID(lastToken, true, false);
						else
							referenceGUID = GUIDUtils.createGuid();

						if (referenceGUID.exists()) {
							for (final PFN p : referenceGUID.getPFNs())
								if (p.pfn.equals(lfnName)) {
									onePfnToCheck = p;
									break;
								}
						}

						if (onePfnToCheck == null) {
							SE fallbackSE = null;
							SE oneSE = null;

							for (final SE se : SEUtils.getSEs(null)) {
								if (lfnName.startsWith(se.seioDaemons + "/" + se.seStoragePath)) {
									oneSE = se;
								}
								else
									if (lfnName.startsWith(se.seioDaemons))
										fallbackSE = se;
							}

							onePfnToCheck = new PFN(lfnName, referenceGUID, oneSE != null ? oneSE : fallbackSE);
						}
					}
					else {
						commander.printErrln("This LFN does not exist in the catalogue: " + lfnName);
						continue;
					}
				}
			}
			else
				if (lfn.guid != null)
					referenceGUID = commander.c_api.getGUID(lfn.guid.toString());
				else {
					commander.printErrln("Could not get the GUID of " + lfn.getCanonicalName());
					continue;
				}

			Collection<PFN> pfnsToCheck;

			if (onePfnToCheck != null) {
				commander.printOutln("Checking this PFN: " + onePfnToCheck.pfn);

				pfnsToCheck = Arrays.asList(onePfnToCheck);
			}
			else {
				if (bDownload)
					pfnsToCheck = commander.c_api.getPFNsToRead(referenceGUID, null, null);
				else
					pfnsToCheck = referenceGUID.getPFNs();

				commander.printOutln("Checking the replicas of " + (lfn != null ? lfn.getCanonicalName() : referenceGUID.guid));
			}

			for (final PFN p : pfnsToCheck) {
				final SE se = p.getSE();

				if (ses.size() > 0 && !ses.contains(se))
					continue;

				if (se != null)
					commander.printOut("\t" + padRight(p.getSE().originalName, 20) + "\t" + p.getPFN() + "\t");
				else
					commander.printOut("\t(unknown SE)\t" + p.getPFN() + "\t");

				if (xrootd == null)
					xrootd = (Xrootd) Factory.xrootd.clone();

				try {
					final String status = (bDownload && ignoreStat && (onePfnToCheck == null)) ? null : xrootd.xrdstat(p, false, false, false);

					// xrdstat was ok at this point

					if (bDownload && onePfnToCheck == null) {
						File f = null;

						String warning = null;

						long timing = -1;

						try {
							f = File.createTempFile("xrdcheck-", "-download.tmp", IOUtils.getTemporaryDirectory());

							if (!f.delete())
								warning = "Could not create and delete the temporary file " + f.getCanonicalPath();
							else {
								final long lStart = System.currentTimeMillis();

								xrootd.get(p, f);

								timing = System.currentTimeMillis() - lStart;

								if (f.length() != referenceGUID.size)
									throw new IOException("Downloaded file size is different from the catalogue (" + f.length() + " vs " + referenceGUID.size + ")");

								if (referenceGUID.md5 != null && referenceGUID.md5.length() > 0) {
									final String fileMD5 = IOUtils.getMD5(f);

									if (!fileMD5.equalsIgnoreCase(referenceGUID.md5))
										throw new IOException("The MD5 checksum of the downloaded file is not the expected one (" + fileMD5 + " vs " + referenceGUID.md5 + ")");
								}
							}
						}
						finally {
							if (f != null) {
								TempFileManager.release(f);

								f.delete();
							}
						}

						if (warning != null) {
							commander.printOutln(ShellColor.jobStateYellow() + "WARNING" + ShellColor.reset());
							commander.printOutln("\t\t" + warning);
						}
						else {
							commander.printOutln(ShellColor.jobStateGreen() + "OK" + ShellColor.reset());
							commander.printOutln("\t\tDownloaded file matches the catalogue details"
									+ (timing > 0 ? ", retrieving took " + Format.toInterval(timing) + " (" + Format.size(referenceGUID.size * 1000. / timing) + "/s)" : ""));
						}
					}
					else {
						// just namespace check, no actual IO
						commander.printOutln(ShellColor.jobStateGreen() + "OK" + ShellColor.reset());

						if (verbose && status != null)
							commander.printOutln(status);
					}
				}
				catch (final Throwable t) {
					final String error = t.getMessage();

					commander.printOutln(ShellColor.jobStateRed() + "ERR" + ShellColor.reset());

					if (verbose)
						commander.printOutln(error);

					if (printCommand) {
						final String cmd = xrootd.getFormattedLastCommand();

						if (!error.contains(cmd))
							commander.printOutln(cmd);
					}
				}
			}
		}

	}

	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("xrdstat", "[-d [-i]] [-v] [-p PID,PID,...] [-s SE1,SE2,...] [-c] <filename1> [<or UUID>] ..."));
		commander.printOutln(helpStartOptions());
		commander.printOutln(helpOption("-d", "Check by physically downloading each replica and checking its content. Without this a stat (metadata) check is done only."));
		commander.printOutln(helpOption("-i", "When downloading each replica, ignore `stat` calls and directly try to fetch the content."));
		commander.printOutln(helpOption("-s", "Comma-separated list of SE names to restrict the checking to. Default is to check all replicas."));
		commander.printOutln(helpOption("-c", "Print the full command line in case of errors."));
		commander.printOutln(helpOption("-v", "More details on the status."));
		commander.printOutln(helpOption("-p", "Comma-separated list of job IDs to check the input data of"));
		commander.printOutln();
	}

	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * Constructor needed for the command factory in JAliEnCOMMander
	 *
	 * @param commander
	 *
	 * @param alArguments
	 *            the arguments of the command
	 * @throws OptionException
	 */
	public JAliEnCommandxrdstat(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);
		try {
			final OptionParser parser = new OptionParser();
			parser.accepts("d");
			parser.accepts("s").withRequiredArg().describedAs("Comma-separated list of SE names to restrict the checking to.");
			parser.accepts("c");
			parser.accepts("i");
			parser.accepts("v");
			parser.accepts("p").withRequiredArg().withValuesSeparatedBy(",").describedAs("Comma-separated list of job IDs to check the input data of");

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));
			bDownload = options.has("d");
			printCommand = options.has("c");
			ignoreStat = options.has("i");
			verbose = options.has("v");

			if (options.has("s")) {
				final StringTokenizer st = new StringTokenizer(options.valueOf("s").toString(), ",;");

				while (st.hasMoreTokens()) {
					final String tok = st.nextToken();

					try {
						final SE se = SEUtils.getSE(tok);

						if (se != null)
							ses.add(se);
						else
							commander.printOutln("The SE you have indicated doesn't exist: " + tok);
					}
					catch (final Throwable t) {
						commander.printOutln("What's this? " + tok + " : " + t.getMessage());
					}
				}
			}

			if (options.has("p")) {
				jobIDs = new ArrayList<>();

				for (final Object o : options.valuesOf("p")) {
					try {
						jobIDs.add(Long.valueOf(o.toString()));
					}
					catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
						commander.printOutln("Invalid job ID: " + o);
					}
				}
			}

			alPaths = new LinkedHashSet<>(options.nonOptionArguments().size());
			alPaths.addAll(optionToString(options.nonOptionArguments()));
		}
		catch (final OptionException e) {
			printHelp();
			throw e;
		}
	}

}
