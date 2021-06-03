/**
 *
 */
package alien.io.protocols;

import static alien.io.protocols.SourceExceptionCode.CANNOT_START_PROCESS;
import static alien.io.protocols.SourceExceptionCode.INTERNAL_ERROR;
import static alien.io.protocols.SourceExceptionCode.INTERRUPTED_WHILE_WAITING_FOR_COMMAND;
import static alien.io.protocols.SourceExceptionCode.LOCAL_FILE_ALREADY_EXISTS;
import static alien.io.protocols.SourceExceptionCode.LOCAL_FILE_CANNOT_BE_CREATED;
import static alien.io.protocols.SourceExceptionCode.LOCAL_FILE_SIZE_DIFFERENT;
import static alien.io.protocols.SourceExceptionCode.MD5_CHECKSUMS_DIFFER;
import static alien.io.protocols.SourceExceptionCode.NO_SERVERS_HAVE_THE_FILE;
import static alien.io.protocols.SourceExceptionCode.NO_SUCH_FILE_OR_DIRECTORY;
import static alien.io.protocols.SourceExceptionCode.SE_DOES_NOT_EXIST;
import static alien.io.protocols.SourceExceptionCode.XRDCP_NOT_FOUND_IN_PATH;
import static alien.io.protocols.SourceExceptionCode.XRDFS_CANNOT_CONFIRM_UPLOAD;
import static alien.io.protocols.SourceExceptionCode.XROOTD_EXITED_WITH_CODE;
import static alien.io.protocols.SourceExceptionCode.XROOTD_TIMED_OUT;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.catalogue.GUID;
import alien.catalogue.PFN;
import alien.catalogue.access.AccessType;
import alien.config.ConfigUtils;
import alien.io.IOUtils;
import alien.se.SE;
import alien.user.UserFactory;
import lia.util.process.ExternalProcess.ExitStatus;
import utils.ExternalCalls;
import utils.ProcessWithTimeout;

/**
 * @author costing
 * @since Dec 8, 2010
 */
public class Xrootd extends Protocol {
	/**
	 *
	 */
	private static final long serialVersionUID = 7860814883144320429L;

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(Xrootd.class.getCanonicalName());

	/**
	 * Debug level to pass to xrdcp commands
	 */
	protected int xrdcpdebuglevel = 0;

	/**
	 * Path to the Xrootd command line binaries
	 */
	private static final String xrootd_default_path;

	private static String xrdcpPath = null;

	/**
	 * Statically filled variable, <code>true</code> when
	 */
	protected static final boolean xrootdNewerThan4;

	private Map<String, String> extraEnvVariables = new HashMap<>();

	private long rateLimit = 0;
	private char rateLimitUnit = 'm';

	static {
		try {
			org.apache.catalina.webresources.TomcatURLStreamHandlerFactory.getInstance().addUserFactory(new ROOTURLStreamHandlerFactory());
		}
		catch (final Throwable t) {
			logger.log(Level.WARNING, "Tomcat URL handler is not available", t);

			try {
				URL.setURLStreamHandlerFactory(new ROOTURLStreamHandlerFactory());
			}
			catch (final Throwable t2) {
				logger.log(Level.WARNING, "Cannot set ROOT URL stream handler factory", t2);
			}
		}

		String defaultPath = null;

		if (ConfigUtils.getConfig() != null) {
			defaultPath = ConfigUtils.getConfig().gets("xrootd.location", null);

			if (defaultPath != null)
				for (final String command : new String[] { "xrdcpapmon", "xrdcp" }) {
					xrdcpPath = ExternalCalls.programExistsInFolders(command, defaultPath, defaultPath + "/bin");

					if (xrdcpPath != null)
						break;
				}
		}

		if (xrdcpPath == null)
			for (final String command : new String[] { "xrdcpapmon", "xrdcp" }) {
				xrdcpPath = ExternalCalls.programExistsInPath(command);

				if (xrdcpPath != null)
					break;
			}

		if (xrdcpPath == null)
			for (final String command : new String[] { "xrdcpapmon", "xrdcp" }) {
				xrdcpPath = ExternalCalls.programExistsInFolders(command, UserFactory.getUserHome() + "/bin", UserFactory.getUserHome() + "/xrootd/bin", "/opt/xrootd/bin");

				if (xrdcpPath != null)
					break;
			}

		boolean newerThan4 = false;

		if (xrdcpPath != null) {
			int idx = xrdcpPath.lastIndexOf('/');

			if (idx > 0) {
				idx = xrdcpPath.lastIndexOf('/', idx - 1);

				if (idx >= 0)
					defaultPath = xrdcpPath.substring(0, idx);
			}

			final ProcessBuilder pBuilder = new ProcessBuilder(Arrays.asList(xrdcpPath, "--version"));

			checkLibraryPath(pBuilder, defaultPath);

			pBuilder.redirectErrorStream(true);

			Process p = null;

			try {
				p = pBuilder.start();

				final ProcessWithTimeout timeout = new ProcessWithTimeout(p, pBuilder);

				if (timeout.waitFor(15, TimeUnit.SECONDS) && timeout.exitValue() == 0) {
					final String version = timeout.getStdout().toString();

					logger.log(Level.FINE, "Local Xrootd version is " + version);

					if (version.indexOf('.') > 0)
						newerThan4 = version.substring(0, version.indexOf('.')).compareTo("v4") >= 0;
				}
			}
			catch (final IOException | InterruptedException ie) {
				if (p != null)
					p.destroy();

				logger.log(Level.WARNING, "Interrupted while waiting for `" + xrdcpPath + " --version` to finish", ie);
			}
		}

		xrootd_default_path = defaultPath;
		xrootdNewerThan4 = newerThan4;
	}

	private static String DIConnectionWindow = "3"; // Xrootd default is 2 minutes, which is too long to give up on non-working IPv6 sockets (* number of IPs in the alias)

	private int timeout = 60;

	// last value must be 0 for a clean exit
	private static final int statRetryTimesXrootd[] = { 1, 5, 10, 0 };
	private static final int statRetryTimesDCache[] = { 5, 10, 15, 20, 30, 0 };

	/**
	 * package protected
	 */
	public Xrootd() {
		// package protected
	}

	/**
	 * Set the LD_LIBRARY_PATH of this process to default Xrootd's lib/ dir
	 *
	 * @param p
	 */
	public static void checkLibraryPath(final ProcessBuilder p) {
		checkLibraryPath(p, xrootd_default_path);
	}

	/**
	 * Set the LD_LIBRARY_PATH of this process to the lib directory of the given path
	 *
	 * @param p
	 * @param path
	 */
	public static void checkLibraryPath(final ProcessBuilder p, final String path) {
		checkLibraryPath(p, path, true);
	}

	/**
	 * Set the LD_LIBRARY_PATH of this process to the lib directory of the given path
	 *
	 * @param p
	 * @param path
	 * @param append
	 *            whether to append to the existing value (<code>true</code>) or replace it (<code>false</code>)
	 */
	public static void checkLibraryPath(final ProcessBuilder p, final String path, final boolean append) {
		if (path != null) {
			final String libPath = path + "/lib";

			if (!append) {
				p.environment().put("LD_LIBRARY_PATH", libPath);
				p.environment().put("DYLD_LIBRARY_PATH", libPath);
			}
			else
				for (final String key : new String[] { "LD_LIBRARY_PATH", "DYLD_LIBRARY_PATH" }) {
					final String old = p.environment().get(key);

					if (old == null || old.length() == 0)
						p.environment().put(key, libPath);
					else {
						// check first that the path is not already included
						final StringTokenizer st = new StringTokenizer(old, ":");

						boolean found = false;

						while (st.hasMoreTokens())
							if (st.nextToken().equals(libPath)) {
								found = true;
								break;
							}

						if (!found)
							p.environment().put(key, old + ":" + libPath);
					}
				}
		}
	}

	/**
	 * @param level
	 *            xrdcp debug level
	 */
	public void setDebugLevel(final int level) {
		xrdcpdebuglevel = level;
	}

	/**
	 * Set the xrdcp timeout
	 *
	 * @param seconds
	 */
	public void setTimeout(final int seconds) {
		timeout = seconds;
	}

	/**
	 * Extract the most relevant failure reason from an xrdcp / xrd3cp output
	 *
	 * @param message
	 * @return relevant portion of the output
	 */
	public static final String parseXrootdError(final String message) {
		if (message == null || message.length() == 0)
			return null;

		int idx = message.indexOf("Last server error");

		if (idx >= 0) {
			idx = message.indexOf("('", idx);

			if (idx > 0) {
				idx += 2;

				final int idx2 = message.indexOf("')", idx);

				if (idx2 > idx)
					return message.substring(idx, idx2);
			}
		}

		idx = message.lastIndexOf("\tretc=");

		if (idx >= 0) {
			int idx2 = message.indexOf('\n', idx);

			if (idx2 < 0)
				idx2 = message.length();

			return message.substring(idx + 1, idx2);
		}

		idx = message.lastIndexOf("Run: ");

		if (idx >= 0) {
			int idx2 = message.indexOf('\n', idx);

			if (idx2 < 0)
				idx2 = message.length();

			return message.substring(idx + 5, idx2);
		}

		idx = message.lastIndexOf("Server responded with an error: ");

		if (idx >= 0) {
			int idx2 = message.indexOf('\n', idx);

			if (idx2 < 0)
				idx2 = message.length();

			return message.substring(idx + 32, idx2);
		}

		return null;
	}

	@Override
	public boolean delete(final PFN pfn) throws IOException {
		return delete(pfn, true);
	}

	/**
	 * @param pfn
	 * @param enforceTicket optionally enforce a delete token
	 * @return <code>true</code> if everything went ok and the file was deleted
	 * @throws IOException
	 */
	public boolean delete(final PFN pfn, final boolean enforceTicket) throws IOException {
		if (enforceTicket && (pfn == null || pfn.ticket == null || pfn.ticket.type != AccessType.DELETE))
			throw new IOException("You didn't get the rights to delete this PFN");

		try {
			final List<String> command = new LinkedList<>();

			// command.addAll(getCommonArguments());

			String envelope = null;

			boolean encryptedEnvelope = false;

			if (pfn.ticket != null && pfn.ticket.envelope != null) {
				envelope = pfn.ticket.envelope.getEncryptedEnvelope();

				if (envelope == null) {
					envelope = pfn.ticket.envelope.getSignedEnvelope();
					encryptedEnvelope = false;
				}
				else
					encryptedEnvelope = true;
			}

			File fAuthz = null;

			String transactionURL = pfn.pfn;

			if (pfn.ticket != null && pfn.ticket.envelope != null)
				transactionURL = pfn.ticket.envelope.getTransactionURL();

			if (xrootdNewerThan4) {
				final URL url = new URL(transactionURL);

				final String host = url.getHost();
				final int port = url.getPort() > 0 ? url.getPort() : 1094;

				String path = url.getPath();

				if (path.startsWith("/"))
					path = path.substring(1);

				command.add(xrootd_default_path + "/bin/xrdfs");
				command.add(host + ":" + port);
				command.add("rm");
				command.add(path + (envelope != null ? "?" + (encryptedEnvelope ? "authz=" : "") + envelope : ""));
			}
			else {
				command.add(xrootd_default_path + "/bin/xrdrm");
				command.add("-v");

				if (envelope != null) {
					fAuthz = File.createTempFile("xrdrm-", ".authz", IOUtils.getTemporaryDirectory());

					try (FileWriter fw = new FileWriter(fAuthz)) {
						fw.write(envelope);
					}

					command.add("-authz");
					command.add(fAuthz.getCanonicalPath());
				}

				command.add(transactionURL);
			}

			if (logger.isLoggable(Level.FINEST))
				logger.log(Level.FINEST, "Executing rm command: " + command);

			setLastCommand(command);

			final ProcessBuilder pBuilder = new ProcessBuilder(command);

			checkLibraryPath(pBuilder);
			setCommonEnv(pBuilder, null);

			pBuilder.redirectErrorStream(true);

			final ExitStatus exitStatus;

			try {
				final Process p = pBuilder.start();
				final ProcessWithTimeout ptimeout = new ProcessWithTimeout(p, pBuilder);
				ptimeout.waitFor(1, TimeUnit.MINUTES);

				exitStatus = ptimeout.getExitStatus();

				setLastExitStatus(exitStatus);
			}
			catch (final InterruptedException ie) {
				setLastExitStatus(null);
				throw new IOException("Interrupted while waiting for the following command to finish:" + getFormattedLastCommand(), ie);
			}
			finally {
				if (fAuthz != null)
					if (!fAuthz.delete())
						logger.log(Level.WARNING, "Could not delete temporary auth token file: " + fAuthz.getAbsolutePath());
			}

			if (exitStatus.getExtProcExitStatus() != 0) {
				String sMessage = parseXrootdError(exitStatus.getStdOut());

				if (logger.isLoggable(Level.WARNING))
					logger.log(Level.WARNING, "RM of " + pfn.pfn + " failed with exit code: " + exitStatus.getExtProcExitStatus() + ", stdout: " + exitStatus.getStdOut());

				if (sMessage != null) {
					if (exitStatus.getExtProcExitStatus() < 0)
						sMessage = "rm timed out and was killed after 1m: " + sMessage;
					else
						sMessage = "rm exited with exit code " + exitStatus.getExtProcExitStatus() + ": " + sMessage;
				}
				else if (exitStatus.getExtProcExitStatus() < 0)
					sMessage = "The following command has timed out and was killed after 1m: " + getFormattedLastCommand();
				else
					sMessage = "Exit code was " + exitStatus.getExtProcExitStatus() + " for command : " + getFormattedLastCommand();

				throw new TargetException(sMessage);
			}

			if (logger.isLoggable(Level.FINEST))
				logger.log(Level.FINEST, "Exit code was zero and the output was:\n" + exitStatus.getStdOut());

			return true;
		}
		catch (final IOException ioe) {
			throw ioe;
		}
		catch (final Throwable t) {
			logger.log(Level.WARNING, "Caught exception", t);

			throw new IOException("delete aborted due to an unexpected exception", t);
		}
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see alien.io.protocols.Protocol#get(alien.catalogue.PFN, alien.catalogue.access.CatalogueReadAccess, java.lang.String)
	 */
	@Override
	public File get(final PFN pfn, final File localFile) throws IOException {
		return get(pfn, localFile, null);
	}

	private File get(final PFN pfn, final File localFile, final String applicationName) throws IOException {
		File target = null;

		if (localFile != null) {
			if (localFile.exists())
				throw new SourceException(LOCAL_FILE_ALREADY_EXISTS, "Local file " + localFile.getCanonicalPath() + " exists already. Xrdcp would fail.");
			target = localFile;
		}

		final GUID guid = pfn.getGuid();

		if (target == null) {
			// we are free to use any cached value
			target = TempFileManager.getAny(guid);

			if (target != null) {
				logger.log(Level.FINE, "Reusing cached file: " + target.getCanonicalPath());

				return target;
			}

			target = File.createTempFile("xrootd-get", null, IOUtils.getTemporaryDirectory());

			if (!target.delete()) {
				logger.log(Level.WARNING, "Could not delete the just created temporary file: " + target);
				return null;
			}
		}
		else {
			File existingFile = TempFileManager.getTemp(guid);

			final boolean wasTempFile = existingFile != null;

			if (existingFile == null)
				existingFile = TempFileManager.getPersistent(guid);

			if (existingFile != null) {
				if (wasTempFile)
					try {
						if (existingFile.renameTo(target)) {
							TempFileManager.putPersistent(guid, target);
							return target;
						}

						logger.log(Level.WARNING, "Could not rename " + existingFile.getAbsolutePath() + " to " + target.getAbsolutePath());
					}
					catch (final Throwable t) {
						logger.log(Level.WARNING, "Exception renaming " + existingFile.getAbsolutePath() + " to " + target.getAbsolutePath(), t);
					}
					finally {
						TempFileManager.release(existingFile);
					}

				// if the file existed with a persistent copy, or the temporary file could not be renamed, try to simply copy it to the target
				try {
					if (Files.copy(Paths.get(existingFile.toURI()), Paths.get(target.toURI())) == null)
						logger.log(Level.WARNING, "Could not copy " + existingFile.getAbsolutePath() + " to " + target.getAbsolutePath());
					else
						return target;
				}
				catch (final Throwable t) {
					logger.log(Level.WARNING, "Exception copying " + existingFile.getAbsolutePath() + " to " + target.getAbsolutePath(), t);
				}
			}
		}

		if (pfn.ticket == null || pfn.ticket.type != AccessType.READ)
			if (logger.isLoggable(Level.FINE))
				logger.log(Level.FINE, "The envelope for PFN " + pfn.toString() + (pfn.ticket == null ? " could not be found" : " is not a READ one"));

		try {
			final List<String> command = new LinkedList<>();

			if (xrdcpPath == null) {
				logger.log(Level.SEVERE, "Could not find xrdcp in path.");
				throw new SourceException(XRDCP_NOT_FOUND_IN_PATH, "Could not find xrdcp in path.");
			}

			command.add(xrdcpPath);

			/*
			 * TODO: enable when servers support checksum queries, at the moment most don't if (xrootdNewerThan4 && guid.md5 != null && guid.md5.length() > 0) { command.add("-C"); command.add("md5:" +
			 * guid.md5); }
			 */

			String transactionURL = pfn.pfn;

			if (pfn.ticket != null && pfn.ticket.envelope != null)
				transactionURL = pfn.ticket.envelope.getTransactionURL();

			String opaqueParams = "-OS";

			if (pfn.ticket != null && pfn.ticket.envelope != null)
				if (pfn.ticket.envelope.getEncryptedEnvelope() != null)
					opaqueParams += "authz=" + pfn.ticket.envelope.getEncryptedEnvelope();
				else if (pfn.ticket.envelope.getSignedEnvelope() != null)
					opaqueParams += pfn.ticket.envelope.getSignedEnvelope();

			opaqueParams = decorateOpaqueParams(opaqueParams, applicationName);

			if (opaqueParams.length() > 4)
				command.add(opaqueParams);

			command.add(transactionURL);
			command.add(target.getCanonicalPath());

			setRateLimit(command);

			setLastCommand(command);

			final ProcessBuilder pBuilder = new ProcessBuilder(command);

			checkLibraryPath(pBuilder);
			setCommonEnv(pBuilder, applicationName);

			// 20KB/s should be available to anybody
			long maxTime = guid.size / 20000;

			maxTime += timeout;

			pBuilder.redirectErrorStream(true);

			final ExitStatus exitStatus;

			Process p = null;

			try {
				p = pBuilder.start();

				if (p != null) {
					final ProcessWithTimeout ptimeout = new ProcessWithTimeout(p, pBuilder);
					ptimeout.waitFor(maxTime, TimeUnit.SECONDS);
					exitStatus = ptimeout.getExitStatus();
					setLastExitStatus(exitStatus);
				}
				else
					throw new SourceException(CANNOT_START_PROCESS, "Cannot start the process");
			}
			catch (final InterruptedException ie) {
				setLastExitStatus(null);

				p.destroy();

				throw new SourceException(INTERRUPTED_WHILE_WAITING_FOR_COMMAND, "Interrupted while waiting for the following command to finish:\n" + getFormattedLastCommand(), ie);
			}

			if (exitStatus.getExtProcExitStatus() != 0) {
				String sMessage = parseXrootdError(exitStatus.getStdOut());
				SourceExceptionCode errCode;

				logger.log(Level.WARNING, "GET of " + pfn.pfn + " failed with " + exitStatus.getStdOut());

				if (sMessage != null) {
					if (exitStatus.getExtProcExitStatus() < 0) {
						errCode = XROOTD_TIMED_OUT;
						sMessage = xrdcpPath + " timed out and was killed after " + maxTime + "s: " + sMessage;
					}
					else {
						errCode = XROOTD_EXITED_WITH_CODE;
						sMessage = xrdcpPath + " exited with exit code " + exitStatus.getExtProcExitStatus() + ": " + sMessage;
					}
				}
				else if (exitStatus.getExtProcExitStatus() < 0) {
					errCode = XROOTD_TIMED_OUT;
					sMessage = "The following command has timed out and was killed after " + maxTime + "s:\n" + getFormattedLastCommand();
				}
				else {
					errCode = XROOTD_EXITED_WITH_CODE;
					sMessage = "Exit code was " + exitStatus.getExtProcExitStatus() + " for command:\n" + getFormattedLastCommand();
				}

				throw new SourceException(errCode, sMessage);
			}

			if (!checkDownloadedFile(target, pfn)) {
				String message = "Local file doesn't match catalogue details";
				SourceExceptionCode errCode;

				if (target.exists()) {
					if (target.length() != guid.size) {
						errCode = LOCAL_FILE_SIZE_DIFFERENT;
						message += ", local file size is different from the expected value (" + target.length() + " vs " + guid.size + ")";
					}
					else {
						errCode = MD5_CHECKSUMS_DIFFER;
						message += ", MD5 checksums differ";
					}
				}
				else {
					errCode = LOCAL_FILE_CANNOT_BE_CREATED;
					message += ", local file could not be created";
				}
				throw new SourceException(errCode, message);
			}
		}
		catch (final SourceException ioe) {
			if (target.exists() && !target.delete())
				logger.log(Level.WARNING, "Could not delete temporary file on IO exception: " + target);
			else {
				// make sure it doesn't pop up later after an interrupt
				TempFileManager.putTemp(alien.catalogue.GUIDUtils.createGuid(), target);
				TempFileManager.release(target);
			}

			throw ioe;
		}
		catch (final Throwable t) {
			if (target.exists() && !target.delete())
				logger.log(Level.WARNING, "Could not delete temporary file on throwable: " + target);
			else {
				// make sure it doesn't pop up later after an interrupt
				TempFileManager.putTemp(alien.catalogue.GUIDUtils.createGuid(), target);
				TempFileManager.release(target);
			}

			logger.log(Level.WARNING, "Caught exception", t);

			throw new SourceException(INTERNAL_ERROR, "Get aborted because " + t);
		}

		if (localFile == null)
			TempFileManager.putTemp(guid, target);
		else
			TempFileManager.putPersistent(guid, target);

		return target;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see alien.io.protocols.Protocol#put(alien.catalogue.PFN, alien.catalogue.access.CatalogueWriteAccess, java.lang.String)
	 */
	@Override
	public String put(final PFN pfn, final File localFile) throws IOException {
		return put(pfn, localFile, true);
	}

	/**
	 * @param pfn target location
	 * @param localFile local file to upload
	 * @param enforceTicket optional ticket enforcing, should only be set to <code>false</code> for tests but all SEs should enforce it
	 * @return the xrdstat of the newly uploaded file
	 * @throws IOException in case of upload problems
	 */
	public String put(final PFN pfn, final File localFile, final boolean enforceTicket) throws IOException {
		return put(pfn, localFile, null, enforceTicket);
	}

	private String put(final PFN pfn, final File localFile, final String applicationName, final boolean enforceTicket) throws IOException {
		if (localFile == null || !localFile.exists() || !localFile.isFile() || !localFile.canRead())
			throw new TargetException("Local file " + localFile + " cannot be read");

		if (enforceTicket && (pfn.ticket == null || pfn.ticket.type != AccessType.WRITE))
			throw new TargetException("No access to this PFN");

		final GUID guid = pfn.getGuid();

		if (localFile.length() != guid.size)
			throw new TargetException("Difference in sizes: local=" + localFile.length() + " / pfn=" + guid.size);

		try {
			final List<String> command = new LinkedList<>();

			if (xrdcpPath == null) {
				logger.log(Level.SEVERE, "Could not find xrdcp in path.");
				throw new TargetException("Could not find xrdcp in path.");
			}

			command.add(xrdcpPath);

			// no progress bar
			if (xrootdNewerThan4)
				command.add("--nopbar");
			else
				command.add("-np");

			/**
			 * // explicitly ask to create intermediate paths
			 * if (xrootdNewerThan4)
			 * command.add("--path");
			 */

			command.add("--verbose"); // display summary output
			command.add("--force"); // re-create a file if already present
			command.add("--posc"); // request POSC (persist-on-successful-close) processing to create a new file

			/*
			 * TODO: enable when storages support checksum queries, at the moment most don't if (xrootdNewerThan4 && guid.md5!=null && guid.md5.length()>0){ command.add("-C");
			 * command.add("md5:"+guid.md5); }
			 */

			setRateLimit(command);

			command.add(localFile.getCanonicalPath());

			String transactionURL = pfn.pfn;

			if (pfn.ticket != null && pfn.ticket.envelope != null) {
				transactionURL = pfn.ticket.envelope.getTransactionURL();

				String opaqueParams = "-OD";

				if (pfn.ticket.envelope.getEncryptedEnvelope() != null) {
					if (!xrootdNewerThan4)
						opaqueParams += "eos.bookingsize=" + guid.size + "&";

					opaqueParams += "authz=" + pfn.ticket.envelope.getEncryptedEnvelope();

				}
				else if (pfn.ticket.envelope.getSignedEnvelope() != null)
					opaqueParams = pfn.ticket.envelope.getSignedEnvelope();

				command.add(decorateOpaqueParams(opaqueParams, applicationName));
			}

			command.add(transactionURL);

			setLastCommand(command);

			final ProcessBuilder pBuilder = new ProcessBuilder(command);

			checkLibraryPath(pBuilder);
			setCommonEnv(pBuilder, applicationName);

			// 20KB/s should be available to anybody
			final long maxTime = timeout + guid.size / 20000;

			pBuilder.redirectErrorStream(true);

			final ExitStatus exitStatus;

			try {
				final Process p = pBuilder.start();

				if (p != null) {
					final ProcessWithTimeout pTimeout = new ProcessWithTimeout(p, pBuilder);
					pTimeout.waitFor(maxTime, TimeUnit.SECONDS);
					exitStatus = pTimeout.getExitStatus();
					setLastExitStatus(exitStatus);
				}
				else
					throw new TargetException("Cannot start the process");
			}
			catch (final InterruptedException ie) {
				setLastExitStatus(null);
				throw new TargetException("Interrupted while waiting for the following command to finish:\n" + getFormattedLastCommand(), ie);
			}

			if (exitStatus.getExtProcExitStatus() != 0) {
				String sMessage = parseXrootdError(exitStatus.getStdOut());

				if (logger.isLoggable(Level.WARNING))
					logger.log(Level.WARNING, "PUT of " + pfn.pfn + " failed with " + exitStatus.getStdOut());

				if (sMessage != null) {
					if (exitStatus.getExtProcExitStatus() < 0)
						sMessage = xrdcpPath + " timed out and was killed after " + maxTime + "s: " + sMessage;
					else
						sMessage = xrdcpPath + " exited with exit code " + exitStatus.getExtProcExitStatus() + ": " + sMessage;
				}
				else if (exitStatus.getExtProcExitStatus() < 0)
					sMessage = "The following command had timed out and was killed after " + maxTime + "s:\n" + getFormattedLastCommand();
				else
					sMessage = "Exit code was " + exitStatus.getExtProcExitStatus() + " for command:\n" + getFormattedLastCommand();

				throw new TargetException(sMessage);
			}

			if (pfn.ticket != null && pfn.ticket.envelope.getEncryptedEnvelope() != null)
				return xrdstat(pfn, false);

			return xrdstat(pfn, true);
		}
		catch (final TargetException ioe) {
			throw ioe;
		}
		catch (final IOException ioe) {
			throw new TargetException(ioe.getMessage());
		}
		catch (final Throwable t) {
			logger.log(Level.WARNING, "Caught exception", t);

			throw new TargetException("Put aborted because " + t);
		}
	}

	/**
	 * @param command
	 */
	private void setRateLimit(List<String> command) {
		if (rateLimit > 0) {
			command.add("-X");

			String value = String.valueOf(rateLimit);

			if (rateLimitUnit == 'k' || rateLimitUnit == 'm' || rateLimitUnit == 'g')
				value += rateLimitUnit;

			command.add(value);
		}
	}

	private static final String[] XRD_LOGLEVEL = { "Error", "Warning", "Info", "Debug", "Dump" };

	/**
	 * Set some extra environment variable. See the xrdcp manual for all the options.
	 *
	 * @param key environment variable name
	 * @param value value to set, can be <code>null</code> to remove any existing value
	 * @return the previously set value for this key
	 */
	public String setEnvVariable(final String key, final String value) {
		return extraEnvVariables.put(key, value);
	}

	private void setCommonEnv(final ProcessBuilder pBuilder, final String defaultApplicationName) {
		final Map<String, String> env = new LinkedHashMap<>();

		env.put("XRD_CONNECTIONWINDOW", DIConnectionWindow);
		env.put("XRD_CONNECTIONRETRY", "1");
		env.put("XRD_TIMEOUTRESOLUTION", "1");
		env.put("XRD_PREFERIPV4", "1");

		if (xrdcpdebuglevel > 0)
			env.put("XRD_LOGLEVEL", XRD_LOGLEVEL[Math.min(xrdcpdebuglevel, XRD_LOGLEVEL.length) - 1]);

		final String appName = ConfigUtils.getApplicationName(defaultApplicationName);

		if (appName != null)
			env.put("XRD_APPNAME", appName);

		if (timeout > 0)
			env.put("XRD_REQUESTTIMEOUT", String.valueOf(timeout));

		for (final Map.Entry<String, String> entry : extraEnvVariables.entrySet()) {
			if (entry.getValue() != null)
				env.put(entry.getKey(), entry.getValue());
			else
				env.remove(entry.getKey());
		}

		setLastCommandEnv(env);
		pBuilder.environment().putAll(env);
	}

	private static String decorateOpaqueParams(final String params, final String defaultApplicationName) {
		final String appName = ConfigUtils.getApplicationName(defaultApplicationName);

		if (appName != null) {
			String ret = params;

			if (ret.startsWith("-O")) {
				if (ret.contains("="))
					ret += "&";
			}
			else if (ret.contains("?")) {
				if (!ret.endsWith("&"))
					ret += "&";
			}
			else
				ret += "?";

			ret += "eos.app=" + appName;

			return ret;
		}

		return params;
	}

	/**
	 * Check if the PFN has the correct properties, such as described in the access envelope
	 *
	 * @param pfn
	 * @param returnEnvelope
	 * @return the signed envelope from the storage, if it knows how to generate one
	 * @throws IOException
	 *             if the remote file properties are not what is expected
	 */
	public String xrdstat(final PFN pfn, final boolean returnEnvelope) throws IOException {
		return xrdstat(pfn, returnEnvelope, true, false);
	}

	/**
	 * @param output
	 * @return the command output less some of the irrelevant messages
	 */
	static String cleanupXrdOutput(final String output) {
		final StringBuilder sb = new StringBuilder(output.length());

		final BufferedReader br = new BufferedReader(new StringReader(output));

		String line;

		try {
			while ((line = br.readLine()) != null)
				if (!line.startsWith("Overriding '"))
					sb.append(line).append('\n');
		}
		catch (@SuppressWarnings("unused") final IOException ioe) {
			// ignore, cannot happen
		}

		return sb.toString().replaceAll("[\\n\\r\\s]+$", "");
	}

	/**
	 * Check if a file is online or on tape / MSS
	 *
	 * @param pfn
	 * @return <code>true</code> if the file is online, <code>false</code> if offline
	 * @throws IOException
	 *             in case a problem executing this request
	 */
	public boolean isOnline(final PFN pfn) throws IOException {
		if (!xrootdNewerThan4)
			throw new IOException("`prepare` command only supported by Xrootd 4+ clients");

		final String stat = xrdstat(pfn, false, false, false);

		if (stat == null)
			throw new IOException("No stat info on this pfn: " + pfn.getPFN());

		final int idx = stat.indexOf("Flags");

		if (idx < 0)
			throw new IOException("No flags info found in this output:\n" + stat);

		if (stat.indexOf("Offline", idx) > 0)
			return false;

		return true;
	}

	/**
	 * Check if the file is online or offline, and if offline request it to be prepared (staged on disk)
	 *
	 * @param pfn
	 * @return <code>true</code> if the request was queued, <code>false</code> if the file was already online
	 * @throws IOException
	 *             if any problem in performing the request
	 */
	public boolean prepareCond(final PFN pfn) throws IOException {
		if (!isOnline(pfn)) {
			prepare(pfn);
			return true;
		}

		return false;
	}

	/**
	 * Stage the file on a mass storage system (TAPE SE)
	 *
	 * @param pfn
	 * @throws IOException
	 *             if any problem in performing the request
	 */
	public void prepare(final PFN pfn) throws IOException {
		if (!xrootdNewerThan4)
			throw new IOException("`prepare` command only supported by Xrootd 4+ clients");

		final List<String> command = new LinkedList<>();

		final URL url;

		String envelope = null;

		boolean encryptedEnvelope = true;

		if (pfn.ticket != null && pfn.ticket.envelope != null) {
			url = new URL(pfn.ticket.envelope.getTransactionURL());

			envelope = pfn.ticket.envelope.getEncryptedEnvelope();

			if (envelope == null) {
				envelope = pfn.ticket.envelope.getSignedEnvelope();
				encryptedEnvelope = false;
			}
		}
		else
			url = new URL(pfn.getPFN());

		final String host = url.getHost();
		final int port = url.getPort() > 0 ? url.getPort() : 1094;

		String path = url.getPath();

		command.add(xrootd_default_path + "/bin/xrdfs");
		command.add(host + ":" + port);
		command.add("prepare");
		command.add("-s");

		if (path.startsWith("//"))
			path = path.substring(1);

		if (envelope != null)
			path += "?" + (encryptedEnvelope ? "authz=" : "") + envelope;

		command.add(path);

		final ProcessBuilder pBuilder = new ProcessBuilder(command);

		setLastCommand(command);

		checkLibraryPath(pBuilder);
		setCommonEnv(pBuilder, null);

		pBuilder.redirectErrorStream(true);

		ExitStatus exitStatus;

		try {
			final Process p = pBuilder.start();

			final ProcessWithTimeout pTimeout = new ProcessWithTimeout(p, pBuilder);
			pTimeout.waitFor(15, TimeUnit.SECONDS);
			exitStatus = pTimeout.getExitStatus();
			setLastExitStatus(exitStatus);
		}
		catch (final InterruptedException ie) {
			setLastExitStatus(null);
			throw new IOException("Interrupted while waiting for the following command to finish:\n" + getFormattedLastCommand(), ie);
		}

		if (exitStatus.getExtProcExitStatus() < 0)
			throw new IOException("Prepare command has timed out and was killed after 15s:\n" + getFormattedLastCommand() + "\n\nOutput so far was:\n" + exitStatus.getStdOut());

		if (exitStatus.getExtProcExitStatus() > 0)
			throw new IOException("Prepare command exited with exit code: " + exitStatus.getExtProcExitStatus() + ", full command and output is below:\n" + getFormattedLastCommand() + "\n"
					+ exitStatus.getStdOut());
	}

	/**
	 * Check if the PFN has the correct properties, such as described in the access envelope
	 *
	 * @param pfn
	 * @param returnEnvelope
	 * @param retryWithDelay
	 * @param forceRecalcMd5
	 * @return the signed envelope from the storage, if it knows how to generate one
	 * @throws IOException
	 *             if the remote file properties are not what is expected
	 */
	public String xrdstat(final PFN pfn, final boolean returnEnvelope, final boolean retryWithDelay, final boolean forceRecalcMd5) throws IOException {
		final SE se = pfn.getSE();

		if (se == null)
			throw new SourceException(SE_DOES_NOT_EXIST, "SE " + pfn.seNumber + " doesn't exist");

		final int[] statRetryTimes = se.seName.toLowerCase().contains("dcache") ? statRetryTimesDCache : statRetryTimesXrootd;

		for (int statRetryCounter = 0; statRetryCounter < statRetryTimes.length; statRetryCounter++)
			try {
				final List<String> command = new LinkedList<>();

				final String qProt = pfn.getPFN().substring(7);
				final String host = qProt.substring(0, qProt.indexOf(':'));
				final String port = qProt.substring(qProt.indexOf(':') + 1, qProt.indexOf('/'));

				if (xrootdNewerThan4) {
					command.add(xrootd_default_path + "/bin/xrdfs");
					command.add(host + ":" + port);
					command.add("stat");
					command.add(qProt.substring(qProt.indexOf('/') + 1));
				}
				else if (returnEnvelope) {
					// xrd pcaliense01:1095 query 32 /15/63447/e3f01fd2-23e3-11e0-9a96-001f29eb8b98?getrespenv=1\&recomputemd5=1
					command.add(xrootd_default_path + "/bin/xrd");

					command.add(host + ":" + port);
					command.add("query");
					command.add("32");
					String qpfn = qProt.substring(qProt.indexOf('/') + 1) + "?getrespenv=1";

					if (forceRecalcMd5)
						qpfn += "\\&recomputemd5=1";

					command.add(qpfn);
				}
				else {
					command.add(xrootd_default_path + "/bin/xrdstat");
					command.add(pfn.getPFN());
				}

				if (getLastCommand() == null)
					setLastCommand(command);

				final ProcessBuilder pBuilder = new ProcessBuilder(command);

				checkLibraryPath(pBuilder);
				setCommonEnv(pBuilder, null);

				pBuilder.redirectErrorStream(true);

				ExitStatus exitStatus;

				final int sleep = statRetryTimes[statRetryCounter];

				final int processTimeout = (sleep == 0 || !retryWithDelay) ? 30 : 15;

				try {
					final Process p = pBuilder.start();

					final ProcessWithTimeout pTimeout = new ProcessWithTimeout(p, pBuilder);
					pTimeout.waitFor(processTimeout, TimeUnit.SECONDS);
					exitStatus = pTimeout.getExitStatus();
					setLastExitStatus(exitStatus);
				}
				catch (final InterruptedException ie) {
					setLastExitStatus(null);
					setLastCommand(command);
					throw new SourceException(INTERRUPTED_WHILE_WAITING_FOR_COMMAND, "Interrupted while waiting for the following command to finish:\n" + getFormattedLastCommand(), ie);
				}

				if (exitStatus.getExtProcExitStatus() != 0) {
					if (sleep == 0 || !retryWithDelay) {
						final String message;
						SourceExceptionCode code;
						if (exitStatus.getExtProcExitStatus() > 0) {
							code = XROOTD_EXITED_WITH_CODE;
							message = "Exit code was " + exitStatus.getExtProcExitStatus();
						}
						else {
							code = XROOTD_TIMED_OUT;
							message = "Command has timed out and was killed after " + processTimeout + "s";
						}

						throw new SourceException(code,
								message + ", retry #" + (statRetryCounter + 1) + ", output was " + cleanupXrdOutput(exitStatus.getStdOut()) + ", " + "for command:\n" + getFormattedLastCommand());
					}

					Thread.sleep(sleep * 1000);
					continue;
				}

				final long filesize = checkOldOutputOnSize(exitStatus.getStdOut());

				if (pfn.getGuid().size <= 0 || pfn.getGuid().size == filesize)
					return cleanupXrdOutput(exitStatus.getStdOut());

				if (sleep == 0 || !retryWithDelay) {
					setLastCommand(command);
					throw new SourceException(XRDFS_CANNOT_CONFIRM_UPLOAD,
							command.toString() + ": could not confirm the upload after " + (statRetryCounter + 1) + " retries: " + cleanupXrdOutput(exitStatus.getStdOut()));
				}

				Thread.sleep(sleep * 1000);
				continue;
			}
			catch (final IOException ioe) {
				throw ioe;
			}
			catch (final Throwable t) {
				logger.log(Level.WARNING, "Caught exception", t);

				final SourceException ioe = new SourceException(INTERNAL_ERROR, "xrdstat internal failure " + t);

				ioe.setStackTrace(t.getStackTrace());

				throw ioe;
			}

		return null;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see alien.io.protocols.Protocol#transfer(alien.catalogue.PFN, alien.catalogue.access.CatalogueReadAccess, alien.catalogue.PFN, alien.catalogue.access.CatalogueWriteAccess)
	 */
	@Override
	public String transfer(final PFN source, final PFN target) throws IOException {
		if (xrootdNewerThan4)
			return transferv4(source, target, TPC_DEFAULT);

		final File temp = get(source, null, "transfer");

		try {
			return put(target, temp, "transfer", true);
		}
		finally {
			TempFileManager.release(temp);
		}
	}

	/**
	 * Do not force any TPC mode
	 */
	public static final int TPC_DEFAULT = 0;

	/**
	 * Force TPC-only transfers
	 */
	public static final int TPC_ONLY = 1;

	/**
	 * Try TPC first
	 */
	public static final int TPC_FIRST = 2;

	/**
	 * Transfer a file between a source and a target
	 *
	 * @param source
	 *            source PFN
	 * @param target
	 *            target PFN
	 * @param iTPC
	 *            one of the TPC_* variables
	 * @return storage reply envelope
	 * @throws IOException
	 */
	public String transferv4(final PFN source, final PFN target, final int iTPC) throws IOException {
		// direct copying between two storages

		if (!xrootdNewerThan4)
			throw new IOException("Xrootd client v4+ is required for this transfer method");

		try {
			if (source.ticket == null || source.ticket.type != AccessType.READ)
				throw new IOException("The ticket for source PFN " + source.toString() + " " + (source.ticket == null ? "could not be found" : "is not a READ one (" + source.ticket.type + ")"));

			if (target.ticket == null || target.ticket.type != AccessType.WRITE)
				throw new IOException("The ticket for target PFN " + target.toString() + " " + (target.ticket == null ? "could not be found" : "is not a WRITE one (" + target.ticket.type + ")"));

			final List<String> command = new LinkedList<>();
			command.add(xrootd_default_path + "/bin/xrdcp");

			if (iTPC != TPC_DEFAULT) {
				command.add("--tpc");

				if (iTPC == TPC_ONLY) {
					final SE se = target.getSE();

					if (se != null && se.getName().contains("DCACHE"))
						command.add("delegate");

					command.add("only");
				}
				else {
					command.add("first");
				}
			}

			command.add("--force");
			// command.add("--path");
			command.add("--posc");
			command.add("--nopbar");

			final boolean sourceEnvelope = source.ticket.envelope != null;

			final boolean targetEnvelope = target.ticket.envelope != null;

			String sourcePath;

			String targetPath;

			if (sourceEnvelope)
				sourcePath = source.ticket.envelope.getTransactionURL();
			else
				sourcePath = source.pfn;

			if (targetEnvelope)
				targetPath = target.ticket.envelope.getTransactionURL();
			else
				targetPath = target.pfn;

			if (sourceEnvelope)
				if (source.ticket.envelope.getEncryptedEnvelope() != null)
					sourcePath += "?authz=" + source.ticket.envelope.getEncryptedEnvelope();
				else if (source.ticket.envelope.getSignedEnvelope() != null)
					sourcePath += "?" + source.ticket.envelope.getSignedEnvelope();

			if (targetEnvelope)
				if (target.ticket.envelope.getEncryptedEnvelope() != null)
					targetPath += "?authz=" + target.ticket.envelope.getEncryptedEnvelope();
				else if (target.ticket.envelope.getSignedEnvelope() != null)
					targetPath += "?" + target.ticket.envelope.getSignedEnvelope();

			sourcePath = decorateOpaqueParams(sourcePath, "transfer-3rd");
			targetPath = decorateOpaqueParams(targetPath, "transfer-3rd");

			command.add(sourcePath);
			command.add(targetPath);

			if (logger.isLoggable(Level.FINE))
				logger.log(Level.FINE, "Executing command:\n" + command);

			setLastCommand(command);

			final ProcessBuilder pBuilder = new ProcessBuilder(command);

			checkLibraryPath(pBuilder);
			setCommonEnv(pBuilder, null);

			long seconds = source.getGuid().size / 200000; // average target
			// speed: 200KB/s

			seconds += 5 * 60; // 5 minutes extra time, handshakes and such

			pBuilder.redirectErrorStream(true);

			final ExitStatus exitStatus;

			try {
				final Process p = pBuilder.start();

				final ProcessWithTimeout pTimeout = new ProcessWithTimeout(p, pBuilder);
				pTimeout.waitFor(seconds, TimeUnit.SECONDS);
				exitStatus = pTimeout.getExitStatus();
				setLastExitStatus(exitStatus);
			}
			catch (final InterruptedException ie) {
				setLastExitStatus(null);
				throw new IOException("Interrupted while waiting for the following command to finish:\n" + getFormattedLastCommand(), ie);
			}

			if (exitStatus.getExtProcExitStatus() != 0) {
				String sMessage = parseXrootdError(exitStatus.getStdOut());

				logger.log(Level.WARNING, "TRANSFER failed with " + exitStatus.getStdOut());

				if (sMessage != null) {
					if (exitStatus.getExtProcExitStatus() < 0)
						sMessage = "xrdcp (TPC==" + iTPC + ") timed out and was killed after " + seconds + "s, error message was: " + sMessage;
					else
						sMessage = "xrdcp (TPC==" + iTPC + ") exited with exit code " + exitStatus.getExtProcExitStatus() + ": " + sMessage;
				}
				else if (exitStatus.getExtProcExitStatus() < 0)
					sMessage = "The following command has timed out and was killed after " + seconds + "s:\n" + getFormattedLastCommand();
				else
					sMessage = "Exit code was " + exitStatus.getExtProcExitStatus() + " for command:\n" + getFormattedLastCommand();

				if (exitStatus.getExtProcExitStatus() == 5 && exitStatus.getStdOut().indexOf("source or destination has 0 size") >= 0) {
					logger.log(Level.WARNING, "Retrying xrdstat, maybe the file shows up with the correct size in a few seconds");

					try {
						final String ret = xrdstat(target, (target.ticket.envelope.getSignedEnvelope() == null));

						if (ret != null) {
							logger.log(Level.WARNING, "xrdstat is ok, assuming transfer was successful");

							return ret;
						}
					}
					catch (final IOException ioe) {
						logger.log(Level.WARNING, "xrdstat throwed exception", ioe);
					}
				}

				if (sMessage.indexOf("unable to connect to destination") >= 0 || sMessage.indexOf("No servers are available to write the file.") >= 0 || sMessage.indexOf("Unable to create") >= 0
						|| sMessage.indexOf("dest-size=0 (source or destination has 0 size!)") >= 0)
					throw new TargetException(sMessage);

				if (sMessage.indexOf("No servers have the file") >= 0)
					throw new SourceException(NO_SERVERS_HAVE_THE_FILE, sMessage);
				if (sMessage.indexOf("No such file or directory") >= 0)
					throw new SourceException(NO_SUCH_FILE_OR_DIRECTORY, sMessage);

				throw new IOException(sMessage);
			}

			return xrdstat(target, (target.ticket.envelope.getSignedEnvelope() == null));
		}
		catch (final IOException ioe) {
			throw ioe;
		}
		catch (final Throwable t) {
			logger.log(Level.WARNING, "Caught exception", t);

			throw new IOException("Transfer aborted because " + t);
		}
	}

	private static long checkOldOutputOnSize(final String stdout) {
		long size = 0;
		String line = null;
		final BufferedReader reader = new BufferedReader(new StringReader(stdout));

		try {
			while ((line = reader.readLine()) != null)
				if (xrootdNewerThan4) {
					if (line.startsWith("Size:")) {
						size = Long.parseLong(line.substring(line.lastIndexOf(':') + 1).trim());
						break;
					}
				}
				else if (line.startsWith("xstat:")) {
					final int idx = line.indexOf("size=");

					if (idx > 0) {
						final int idx2 = line.indexOf(" ", idx);

						size = Long.parseLong(line.substring(idx + 5, idx2));

						break;
					}
				}
		}
		catch (final IOException e) {
			e.printStackTrace();
		}

		return size;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "xrootd";
	}

	@Override
	int getPreference() {
		return 10;
	}

	@Override
	public boolean isSupported() {
		return true;
	}

	@Override
	public byte protocolID() {
		return 3;
	}

	/**
	 * @return the path for the default Xrootd version (base directory, append /bin or /lib to it)
	 * @see Xrootd#checkLibraryPath(ProcessBuilder)
	 */
	public static String getXrootdDefaultPath() {
		return xrootd_default_path;
	}

	/**
	 * @return path to the xrdcp command line binary
	 */
	public static String getXrdcpPath() {
		return xrdcpPath;
	}

	/**
	 * @param pfn
	 *            Some path + read access token to get the space information for
	 * @return space information
	 * @throws IOException
	 */
	public SpaceInfo getSpaceInfo(final PFN pfn) throws IOException {
		SpaceInfo spaceInfo = null;

		try {
			spaceInfo = getXrdfsSpaceInfo(pfn);
		}
		catch (@SuppressWarnings("unused") final IOException ioe) {
			// ignore, we'll try the next command
		}

		if (spaceInfo == null || !spaceInfo.spaceInfoSet)
			try {
				final SpaceInfo querySpace = getQuerySpaceInfo(pfn);

				if (spaceInfo == null)
					return querySpace;

				if (querySpace.spaceInfoSet)
					spaceInfo.setSpaceInfo(querySpace.path, querySpace.totalSpace, querySpace.freeSpace, querySpace.usedSpace, querySpace.largestFreeChunk);

				if (querySpace.versionInfoSet && !spaceInfo.versionInfoSet)
					spaceInfo.setVersion(querySpace.vendor, querySpace.version);

			}
			catch (final IOException ioe) {
				if (spaceInfo != null)
					return spaceInfo;

				throw ioe;
			}

		return spaceInfo;
	}

	private SpaceInfo getQuerySpaceInfo(final PFN pfn) throws IOException {
		final List<String> command = new LinkedList<>();

		final URL url;

		boolean encryptedEnvelope = false;

		String envelope = null;

		if (pfn.ticket != null && pfn.ticket.type == AccessType.READ && pfn.ticket.envelope != null) {
			url = new URL(pfn.ticket.envelope.getTransactionURL());

			envelope = pfn.ticket.envelope.getEncryptedEnvelope();

			if (envelope == null)
				envelope = pfn.ticket.envelope.getSignedEnvelope();
			else
				encryptedEnvelope = true;
		}
		else
			url = new URL(pfn.pfn);

		final String host = url.getHost();
		final int port = url.getPort() > 0 ? url.getPort() : 1094;

		String path = url.getPath();

		if (path.startsWith("//"))
			path = path.substring(1);

		final SpaceInfo ret = new SpaceInfo();

		ExitStatus exitStatus;

		ProcessBuilder pBuilder;

		for (int attempt = 0; !ret.spaceInfoSet && attempt <= 1; attempt++) {
			command.clear();

			command.add(xrootd_default_path + "/bin/xrdfs");
			command.add(host + ":" + port);
			command.add("query");
			command.add("space");

			if (attempt == 1)
				if (envelope != null)
					command.add(path + "?" + (encryptedEnvelope ? "authz=" : "") + envelope);
				else
					continue;
			else
				command.add(path);

			if (logger.isLoggable(Level.FINEST))
				logger.log(Level.FINEST, "Executing spaceinfo command: " + command);

			setLastCommand(command);

			pBuilder = new ProcessBuilder(command);

			checkLibraryPath(pBuilder);
			setCommonEnv(pBuilder, null);

			pBuilder.redirectErrorStream(true);

			try {
				final Process p = pBuilder.start();

				final ProcessWithTimeout pTimeout = new ProcessWithTimeout(p, pBuilder);
				pTimeout.waitFor(2, TimeUnit.MINUTES);
				exitStatus = pTimeout.getExitStatus();
				setLastExitStatus(exitStatus);

				try (BufferedReader br = new BufferedReader(new StringReader(exitStatus.getStdOut()))) {
					String line;

					long total = 0;
					long free = 0;
					long used = 0;
					long largest = 0;

					while ((line = br.readLine()) != null) {
						// oss.cgroup=default&oss.space=1279832876908544&oss.free=892794559213568&oss.maxf=68719476736&oss.used=387038317694976&oss.quota=1279832876908544

						final StringTokenizer st = new StringTokenizer(line, "&");

						while (st.hasMoreTokens()) {
							final String tok = st.nextToken();

							final int idx = tok.indexOf('=');

							if (idx > 0) {
								final String key = tok.substring(0, idx);
								final String value = tok.substring(idx + 1).trim();

								switch (key) {
									case "oss.space":
									case "oss.quota":
										total = Long.parseLong(value);
										break;
									case "oss.free":
										free = Long.parseLong(value);
										break;
									case "oss.maxf":
										largest = Long.parseLong(value);
										break;
									case "oss.used":
										used = Long.parseLong(value);
										break;
									default:
										break;
								}
							}
						}
					}

					if (total > 0 && free <= total && free >= 0 && used <= total && used >= 0 && largest <= total && largest >= 0)
						ret.setSpaceInfo(path, total, free, used, largest);
				}
			}
			catch (final InterruptedException ie) {
				setLastExitStatus(null);
				throw new IOException("Interrupted while waiting for the following command to finish:\n" + getFormattedLastCommand(), ie);
			}
		}

		return ret;
	}

	private SpaceInfo getXrdfsSpaceInfo(final PFN pfn) throws IOException {
		final List<String> command = new LinkedList<>();

		final URL url;

		boolean encryptedEnvelope = false;

		String envelope = null;

		if (pfn.ticket != null && pfn.ticket.envelope != null) {
			url = new URL(pfn.ticket.envelope.getTransactionURL());

			envelope = pfn.ticket.envelope.getEncryptedEnvelope();

			if (envelope == null)
				envelope = pfn.ticket.envelope.getSignedEnvelope();
			else
				encryptedEnvelope = true;
		}
		else
			url = new URL(pfn.pfn);

		final String host = url.getHost();
		final int port = url.getPort() > 0 ? url.getPort() : 1094;

		String path = url.getPath();

		if (path.startsWith("//"))
			path = path.substring(1);

		final SpaceInfo ret = new SpaceInfo();

		ExitStatus exitStatus;

		ProcessBuilder pBuilder;

		for (int attempt = 0; !ret.spaceInfoSet && attempt <= 1; attempt++) {
			command.clear();

			command.add(xrootd_default_path + "/bin/xrdfs");
			command.add(host + ":" + port);
			command.add("spaceinfo");

			if (attempt == 1) {
				if (envelope != null)
					command.add(path + "?" + (encryptedEnvelope ? "authz=" : "") + envelope);
				else
					continue;
			}
			else
				command.add(path);

			if (logger.isLoggable(Level.FINEST))
				logger.log(Level.FINEST, "Executing spaceinfo command: " + command);

			setLastCommand(command);

			pBuilder = new ProcessBuilder(command);

			checkLibraryPath(pBuilder);
			setCommonEnv(pBuilder, null);

			pBuilder.redirectErrorStream(true);

			try {
				final Process p = pBuilder.start();

				final ProcessWithTimeout pTimeout = new ProcessWithTimeout(p, pBuilder);
				pTimeout.waitFor(2, TimeUnit.MINUTES);
				exitStatus = pTimeout.getExitStatus();
				setLastExitStatus(exitStatus);

				try (BufferedReader br = new BufferedReader(new StringReader(exitStatus.getStdOut()))) {
					String line;

					long total = 0;
					long free = 0;
					long used = 0;
					long largest = 0;

					while ((line = br.readLine()) != null) {
						final StringTokenizer st = new StringTokenizer(line);

						if (!st.hasMoreTokens())
							continue;

						final String firstToken = st.nextToken();

						if (!st.hasMoreTokens())
							continue;

						String lastToken = st.nextToken();

						while (st.hasMoreTokens())
							lastToken = st.nextToken();

						switch (firstToken) {
							case "Total:":
								total = Long.parseLong(lastToken);
								break;
							case "Free:":
								free = Long.parseLong(lastToken);
								break;
							case "Used:":
								used = Long.parseLong(lastToken);
								break;
							case "Largest":
								largest = Long.parseLong(lastToken);
								break;
							default:
								break;
						}
					}

					if (total > 0 && free <= total && free >= 0 && used <= total && used >= 0 && largest <= total && largest >= 0)
						ret.setSpaceInfo(path, total, free, used, largest);
				}
			}
			catch (final InterruptedException ie) {
				setLastExitStatus(null);
				throw new IOException("Interrupted while waiting for the following command to finish:\n" + getFormattedLastCommand(), ie);
			}
		}

		// Now get the server software version

		command.clear();

		command.add(xrootd_default_path + "/bin/xrdfs");
		command.add(host + ":" + port);
		command.add("query");
		command.add("config");
		command.add("version");

		if (logger.isLoggable(Level.FINEST))
			logger.log(Level.FINEST, "Executing spaceinfo command: " + command);

		setLastCommand(command);

		pBuilder = new ProcessBuilder(command);

		checkLibraryPath(pBuilder);
		setCommonEnv(pBuilder, null);

		pBuilder.redirectErrorStream(true);

		try {
			final Process p = pBuilder.start();

			final ProcessWithTimeout pTimeout = new ProcessWithTimeout(p, pBuilder);
			pTimeout.waitFor(15, TimeUnit.SECONDS);
			exitStatus = pTimeout.getExitStatus();
			setLastExitStatus(exitStatus);

			try (BufferedReader br = new BufferedReader(new StringReader(exitStatus.getStdOut()))) {
				String line = br.readLine();

				if (line != null) {
					line = line.trim();
					if (!"version".equals(line) && !line.startsWith("["))
						if (line.startsWith("v"))
							ret.setVersion("Xrootd", line);
						else if (line.startsWith("dCache "))
							ret.setVersion("dCache", line.substring(line.indexOf(' ') + 1).trim());
						else
							ret.setVersion(null, line);
				}
			}
		}
		catch (final InterruptedException ie) {
			setLastExitStatus(null);
			throw new IOException("Interrupted while waiting for the following command to finish:\n" + getFormattedLastCommand(), ie);
		}

		return ret;
	}

	@Override
	public Object clone() {
		final Xrootd theClone = (Xrootd) super.clone();

		theClone.extraEnvVariables = new HashMap<>(this.extraEnvVariables);

		return theClone;
	}

	/**
	 * Set a read or write (only when the target or respectively source is a local file) to the given rate (per second). See `man xrdcp`, the "-X" option
	 * 
	 * @param rate value
	 * @param unit multiplying unit, can be one of 'k', 'm' or 'g'. Anything else is considered to be bytes/second.
	 */
	public void setRateLimit(final long rate, final char unit) {
		this.rateLimit = rate;
		this.rateLimitUnit = unit;
	}
}
