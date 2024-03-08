package alien.io.protocols;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.catalogue.PFN;
import alien.catalogue.access.AccessType;
import alien.config.ConfigUtils;
import alien.io.IOUtils;
import lia.util.process.ExternalProcess;
import lia.util.process.ExternalProcess.ExitStatus;
import lia.util.process.ExternalProcessBuilder;

/**
 * @author ron
 * @since Oct 11, 2011
 */
public class CpForTest extends Protocol {

	/**
	 *
	 */
	private static final long serialVersionUID = 7899348307554604135L;
	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(Xrootd.class.getCanonicalName());

	/**
	 * package protected
	 */
	public CpForTest() {
		// package protected
	}

	private static String getLocalPath(final PFN pfn) {

		return pfn.pfn.substring(pfn.pfn.lastIndexOf("//"));
	}

	@Override
	public boolean delete(final PFN pfn) throws IOException {
		if (pfn == null || pfn.ticket == null || pfn.ticket.type != AccessType.DELETE)
			throw new IOException("You didn't get the rights to delete this PFN");

		try {
			final List<String> command = new LinkedList<>();

			// command.addAll(getCommonArguments());

			// String envelope = null;
			//
			// if (pfn.ticket.envelope != null) {
			// envelope = pfn.ticket.envelope.getEncryptedEnvelope();
			//
			// if (envelope == null)
			// envelope = pfn.ticket.envelope.getSignedEnvelope();
			// }

			command.add("wouldremove");
			command.add(getLocalPath(pfn));

			// System.err.println(command);

			final ExternalProcessBuilder pBuilder = new ExternalProcessBuilder(command);

			pBuilder.returnOutputOnExit(true);

			pBuilder.timeout(1, TimeUnit.HOURS);

			pBuilder.redirectErrorStream(true);

			final ExitStatus exitStatus;

			try {
				exitStatus = pBuilder.start().waitFor();
			}
			catch (final InterruptedException ie) {
				throw new IOException("Interrupted while waiting for the following command to finish : " + command.toString(), ie);
			}

			if (exitStatus.getExtProcExitStatus() != 0) {
				System.err.println("cp remove error\n" + exitStatus.getStdOut());

				throw new IOException("Exit code " + exitStatus.getExtProcExitStatus());
			}

			// System.err.println(exitStatus.getStdOut());

			return true;
		}
		catch (final IOException ioe) {
			throw ioe;
		}
		catch (final Throwable t) {
			logger.log(Level.WARNING, "Caught exception", t);

			throw new IOException("delete aborted because " + t);
		}

	}

	/*
	 * (non-Javadoc)
	 *
	 * @see alien.io.protocols.Protocol#get(alien.catalogue.PFN, alien.catalogue.access.CatalogueReadAccess, java.lang.String)
	 */
	@Override
	public File get(final PFN pfn, final File localFile) throws IOException {
		File target;

		if (localFile != null) {
			if (localFile.exists())
				throw new IOException("Local file " + localFile.getCanonicalPath() + " exists already. Cp would fail.");

			target = localFile;
		}
		else {
			target = File.createTempFile("cp-get", null, IOUtils.getTemporaryDirectory());

			if (!target.delete())
				logger.log(Level.WARNING, "Could not delete the just created temporary file: " + target);
		}

		if (pfn.ticket == null || pfn.ticket.type != AccessType.READ)
			throw new IOException("The envelope for PFN " + pfn.pfn + " could not be found or is not a READ one.");

		try {
			final List<String> command = new LinkedList<>();
			command.add("cp");
			command.add(getLocalPath(pfn));
			command.add(target.getCanonicalPath());

			final ExternalProcessBuilder pBuilder = new ExternalProcessBuilder(command);

			pBuilder.returnOutputOnExit(true);

			pBuilder.timeout(24, TimeUnit.HOURS);

			pBuilder.redirectErrorStream(true);

			final ExitStatus exitStatus;

			try {
				final ExternalProcess p = pBuilder.start();

				if (p != null)
					exitStatus = p.waitFor();
				else
					throw new IOException("Cannot start the process");
			}
			catch (final InterruptedException ie) {
				throw new IOException("Interrupted while waiting for the following command to finish : " + command.toString(), ie);
			}

			if (exitStatus.getExtProcExitStatus() != 0) {
				String sMessage = exitStatus.getStdOut();

				logger.log(Level.WARNING, "GET failed with " + exitStatus.getStdOut() + "\nCommand: " + command.toString());

				if (sMessage != null)
					sMessage = "cp exited with " + exitStatus.getExtProcExitStatus() + ": " + sMessage;
				else
					sMessage = "Exit code was " + exitStatus.getExtProcExitStatus() + " for command : " + command.toString();

				throw new IOException(sMessage);
			}

			if (!checkDownloadedFile(target, pfn))
				throw new IOException("Local file doesn't match catalogue details");
		}
		catch (final IOException ioe) {
			if (!target.delete())
				logger.log(Level.WARNING, "Could not delete temporary file on IO exception: " + target);

			throw ioe;
		}
		catch (final Throwable t) {
			if (!target.delete())
				logger.log(Level.WARNING, "Could not delete temporary file on throwable: " + target);

			logger.log(Level.WARNING, "Caught exception", t);

			throw new IOException("Get aborted because " + t);
		}
		return target;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see alien.io.protocols.Protocol#put(alien.catalogue.PFN, alien.catalogue.access.CatalogueWriteAccess, java.lang.String)
	 */
	@Override
	public String put(final PFN pfn, final File localFile) throws IOException {

		if (localFile == null || !localFile.exists() || !localFile.isFile() || !localFile.canRead())
			throw new IOException("Local file " + localFile + " cannot be read");

		if (pfn.ticket == null || pfn.ticket.type != AccessType.WRITE)
			throw new IOException("No access to this PFN");

		if (localFile.length() != pfn.getGuid().size)
			throw new IOException("Difference in sizes: local=" + localFile.length() + " / pfn=" + pfn.getGuid().size);

		try {

			Runtime.getRuntime().exec("mkdir -p " + getLocalPath(pfn).substring(0, getLocalPath(pfn).lastIndexOf('/')));

			final List<String> command = new LinkedList<>();
			command.add("cp");
			command.add(localFile.getCanonicalPath());
			command.add(getLocalPath(pfn));

			final ExternalProcessBuilder pBuilder = new ExternalProcessBuilder(command);

			pBuilder.returnOutputOnExit(true);

			pBuilder.timeout(24, TimeUnit.HOURS);

			pBuilder.redirectErrorStream(true);

			final ExitStatus exitStatus;

			try {
				exitStatus = pBuilder.start().waitFor();
			}
			catch (final InterruptedException ie) {
				throw new IOException("Interrupted while waiting for the following command to finish : " + command.toString(), ie);
			}

			if (exitStatus.getExtProcExitStatus() != 0) {
				String sMessage = exitStatus.getStdOut();

				logger.log(Level.WARNING, "PUT failed with " + exitStatus.getStdOut());

				if (sMessage != null)
					sMessage = "cp exited with " + exitStatus.getExtProcExitStatus() + ": " + sMessage;
				else
					sMessage = "Exit code was " + exitStatus.getExtProcExitStatus() + " for command : " + command.toString();

				throw new IOException(sMessage);
			}
			return pfn.ticket.envelope.getSecureEnvelope();
		}
		catch (final IOException ioe) {
			throw ioe;
		}
		catch (final Throwable t) {
			logger.log(Level.WARNING, "Caught exception", t);

			throw new IOException("Get aborted because " + t);
		}
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see alien.io.protocols.Protocol#transfer(alien.catalogue.PFN, alien.catalogue.access.CatalogueReadAccess, alien.catalogue.PFN, alien.catalogue.access.CatalogueWriteAccess)
	 */
	@Override
	public String transfer(final PFN source, final PFN target) throws IOException {
		final File temp = get(source, null);

		try {
			return put(target, temp);
		}
		finally {
			TempFileManager.release(temp);
		}
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "cp";
	}

	@Override
	int getPreference() {
		return 20;
	}

	@Override
	public boolean isSupported() {
		return true;
	}

	@Override
	public byte protocolID() {
		return 0;
	}
}
