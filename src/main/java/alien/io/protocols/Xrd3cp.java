/**
 *
 */
package alien.io.protocols;

import static alien.io.protocols.SourceExceptionCode.NO_SERVERS_HAVE_THE_FILE;
import static alien.io.protocols.SourceExceptionCode.NO_SUCH_FILE_OR_DIRECTORY;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import alien.catalogue.PFN;
import alien.catalogue.access.AccessType;
import alien.config.ConfigUtils;
import alien.se.SE;
import alien.user.UserFactory;
import lia.util.process.ExternalProcess.ExitStatus;
import utils.ExternalCalls;
import utils.ProcessWithTimeout;

/**
 * @author costing
 * @since Dec 8, 2010
 */
public class Xrd3cp extends Xrootd {

	/**
	 *
	 */
	private static final long serialVersionUID = 9084272684664087714L;

	/**
	 * package protected
	 */
	Xrd3cp() {
		// package protected
	}

	private static final String QUOTES = ""; //

	private static String xrd3cpPath = null;
	private static boolean cachedPath = false;

	private static synchronized String getXrd3cpPath() {
		if (cachedPath)
			return xrd3cpPath;

		cachedPath = true;

		for (final String path : new String[] { getXrootdDefaultPath(), ConfigUtils.getConfig().gets("xrd3cp.location", null), UserFactory.getUserHome() + "/alien/api", "/opt/alien/api" })
			if (path != null) {
				final File test = new File(path + "/bin/xrd3cp");

				if (test.exists() && test.isFile() && test.canExecute()) {
					xrd3cpPath = path;
					return xrd3cpPath;
				}
			}

		xrd3cpPath = ExternalCalls.programExistsInPath("xrd3cp");

		if (xrd3cpPath != null) {
			int idx = xrd3cpPath.lastIndexOf('/');

			if (idx > 0) {
				idx = xrd3cpPath.lastIndexOf('/', idx - 1);

				if (idx >= 0)
					xrd3cpPath = xrd3cpPath.substring(0, idx);
			}
		}

		return xrd3cpPath;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see alien.io.protocols.Protocol#transfer(alien.catalogue.PFN, alien.catalogue.access.CatalogueReadAccess, alien.catalogue.PFN, alien.catalogue.access.CatalogueWriteAccess)
	 */
	@Override
	public String transfer(final PFN source, final PFN target) throws IOException {
		// direct copying between two storages

		try {
			if (source.ticket == null || source.ticket.type != AccessType.READ)
				throw new IOException("The ticket for source PFN " + source.toString() + " could not be found or is not a READ one.");

			if (target.ticket == null || target.ticket.type != AccessType.WRITE)
				throw new IOException("The ticket for target PFN " + target.toString() + " could not be found or is not a WRITE one.");

			final List<String> command = new LinkedList<>();
			command.add(getXrd3cpPath() + "/bin/xrd3cp");
			command.add("-m");

			final SE targetSE = target.getSE();

			final String targetSEName = targetSE != null ? targetSE.getName().toLowerCase() : "";

			// CERN::EOS, CERN::OCDB, NDGF::DCACHE, NDGF::DCACHE_TAPE,
			// NSC::DCACHE, SARA::DCACHE, SARA::DCACHE_TAPE, SNIC::DCACHE
			// All these SEs need to pass the opaque parameters, native xrootd
			// SEs fail to perform the 3rd party transfer if this is given
			if (targetSEName.indexOf("::eos") >= 0 || targetSEName.indexOf("::dcache") >= 0 || targetSEName.equals("alice::cern::ocdb"))
				command.add("-O");

			command.add("-S");

			final boolean sourceEnvelope = source.ticket.envelope != null;

			final boolean targetEnvelope = target.ticket.envelope != null;

			if (sourceEnvelope)
				command.add(source.ticket.envelope.getTransactionURL());
			else
				command.add(source.pfn);

			String targetPath;

			if (targetEnvelope)
				targetPath = target.ticket.envelope.getTransactionURL();
			else
				targetPath = target.pfn;

			targetPath = addURLParameter(targetPath, "eos.bookingsize=" + source.getGuid().size);

			command.add(targetPath);

			if (sourceEnvelope)
				if (source.ticket.envelope.getEncryptedEnvelope() != null)
					command.add(QUOTES + "authz=" + source.ticket.envelope.getEncryptedEnvelope() + QUOTES);
				else if (source.ticket.envelope.getSignedEnvelope() != null)
					command.add(source.ticket.envelope.getSignedEnvelope());

			if (targetEnvelope)
				if (target.ticket.envelope.getEncryptedEnvelope() != null)
					command.add(QUOTES + "authz=" + target.ticket.envelope.getEncryptedEnvelope() + QUOTES);
				else if (target.ticket.envelope.getSignedEnvelope() != null)
					command.add(target.ticket.envelope.getSignedEnvelope());

			setLastCommand(command);

			final ProcessBuilder pBuilder = new ProcessBuilder(command);

			final String xrd3cpBasePath = getXrd3cpPath();

			checkLibraryPath(pBuilder, xrd3cpBasePath);

			if (xrd3cpBasePath.endsWith("/api"))
				checkLibraryPath(pBuilder, xrd3cpBasePath.substring(0, xrd3cpBasePath.lastIndexOf('/')), true);

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
				throw new IOException("Interrupted while waiting for the following command to finish : " + command.toString(), ie);
			}

			if (exitStatus.getExtProcExitStatus() != 0) {
				String sMessage = parseXrootdError(exitStatus.getStdOut());

				logger.log(Level.WARNING, "TRANSFER failed with " + exitStatus.getStdOut());

				if (sMessage != null)
					sMessage = "xrd3cp exited with " + exitStatus.getExtProcExitStatus() + ": " + sMessage;
				else
					sMessage = "Exit code was " + exitStatus.getExtProcExitStatus() + " for command : " + command.toString();

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

	/*
	 * (non-Javadoc)
	 *
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "xrd3cp";
	}

	@Override
	int getPreference() {
		return 3;
	}

	@Override
	public boolean isSupported() {
		// return getXrd3cpPath() != null;
		return false;
	}

	/**
	 * Testing method for getting the xrd3cp path
	 *
	 * @param args
	 */
	public static void main(final String[] args) {
		System.err.println("xrd3cp location: " + getXrd3cpPath());
	}

	@Override
	public byte protocolID() {
		return 4;
	}
}
