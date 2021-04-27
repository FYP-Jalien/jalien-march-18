/**
 *
 */
package alien.io.protocols;

import static alien.io.protocols.SourceExceptionCode.NO_SERVERS_HAVE_THE_FILE;
import static alien.io.protocols.SourceExceptionCode.NO_SUCH_FILE_OR_DIRECTORY;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import alien.catalogue.PFN;
import alien.catalogue.access.AccessType;
import alien.config.ConfigUtils;
import lia.util.process.ExternalProcess.ExitStatus;
import utils.ProcessWithTimeout;

/**
 * 3rd party Xrootd transfers via a set of well-connected gateway servers at CERN
 *
 * @author costing
 * @since Jun 25 2015
 */
public class Xrd3cpGW extends Xrootd {

	/**
	 *
	 */
	private static final long serialVersionUID = 9084272684664087714L;

	/**
	 * package protected
	 */
	Xrd3cpGW() {
		// package protected
	}

	private static List<String> transferServers = null;

	private static int serverIdx = 0;

	private static long lastUpdated = 0;

	private static synchronized void updateServerList() {
		if (transferServers == null || (System.currentTimeMillis() - lastUpdated) > 1000 * 60) {
			final String servers = ConfigUtils.getConfig().gets("xrootdgw.servers", "eosalicemover.cern.ch");

			final StringTokenizer stServers = new StringTokenizer(servers, ",; \r\n\t");

			transferServers = new ArrayList<>();

			while (stServers.hasMoreTokens()) {
				final String server = stServers.nextToken();

				try {
					final InetAddress[] addresses = InetAddress.getAllByName(server);

					if (addresses != null)
						for (final InetAddress addr : addresses)
							transferServers.add(addr.getHostAddress());
				}
				catch (final UnknownHostException uhe) {
					logger.log(Level.WARNING, "Cannot resolve address of " + server, uhe);
				}
			}

			Collections.sort(transferServers);
		}
	}

	private static synchronized String getTransferServerInstance() {
		updateServerList();

		serverIdx = (serverIdx + 1) % transferServers.size();

		return ThreadLocalRandom.current().nextInt(100000000) + "@" + transferServers.get(serverIdx);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see alien.io.protocols.Protocol#transfer(alien.catalogue.PFN, alien.catalogue.access.CatalogueReadAccess, alien.catalogue.PFN, alien.catalogue.access.CatalogueWriteAccess)
	 */
	@Override
	public String transfer(final PFN source, final PFN target) throws IOException {
		// copying between two storages through a gateway Xrootd server

		if (!xrootdNewerThan4)
			throw new IOException("Xrootd client v4+ is required for Xrd3cpGW");

		try {
			if (source.ticket == null || source.ticket.type != AccessType.READ)
				throw new IOException("The ticket for source PFN " + source.toString() + " could not be found or is not a READ one.");

			if (target.ticket == null || target.ticket.type != AccessType.WRITE)
				throw new IOException("The ticket for target PFN " + target.toString() + " could not be found or is not a WRITE one.");

			final List<String> command = new LinkedList<>();
			command.add(getXrootdDefaultPath() + "/bin/xrdcp");
			command.add("--tpc");
			command.add("only");
			command.add("--force");
			command.add("--path");
			command.add("--posc");

			final boolean sourceEnvelope = source.ticket.envelope != null;

			final boolean targetEnvelope = target.ticket.envelope != null;

			final String serverInstance = getTransferServerInstance();

			String sourcePath = "root://" + serverInstance + ":" + ConfigUtils.getConfig().geti("xrootdgw.port.source", 20999) + "//";

			String targetPath = "root://" + serverInstance + ":" + ConfigUtils.getConfig().geti("xrootdgw.port.target", 21000) + "//";

			if (sourceEnvelope)
				sourcePath += source.ticket.envelope.getTransactionURL();
			else
				sourcePath += source.pfn;

			if (targetEnvelope)
				targetPath += target.ticket.envelope.getTransactionURL();
			else
				targetPath += target.pfn;

			final String urlParameters = ConfigUtils.getConfig().gets("xrootdgw.urlparameters", "diamond.tpc.blocksize=32M");

			if (urlParameters.length() > 0) {
				sourcePath = addURLParameter(sourcePath, urlParameters);
				targetPath = addURLParameter(targetPath, urlParameters);
			}

			if (sourceEnvelope)
				if (source.ticket.envelope.getEncryptedEnvelope() != null)
					sourcePath = addURLParameter(sourcePath, "authz=" + source.ticket.envelope.getEncryptedEnvelope());
				else if (source.ticket.envelope.getSignedEnvelope() != null)
					sourcePath = addURLParameter(sourcePath, source.ticket.envelope.getSignedEnvelope());

			if (targetEnvelope)
				if (target.ticket.envelope.getEncryptedEnvelope() != null)
					targetPath = addURLParameter(targetPath, "authz=" + target.ticket.envelope.getEncryptedEnvelope());
				else if (target.ticket.envelope.getSignedEnvelope() != null)
					targetPath = addURLParameter(targetPath, target.ticket.envelope.getSignedEnvelope());

			command.add(sourcePath);
			command.add(targetPath);

			setLastCommand(command);

			final ProcessBuilder pBuilder = new ProcessBuilder(command);

			checkLibraryPath(pBuilder);

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

				logger.log(Level.WARNING, "TRANSFER via " + serverInstance + " failed with " + exitStatus.getStdOut());

				if (sMessage != null)
					sMessage = "xrdcp --tpc only exited with " + exitStatus.getExtProcExitStatus() + ": " + sMessage;
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
		return "xrd3cpGW";
	}

	@Override
	int getPreference() {
		return 5;
	}

	@Override
	public boolean isSupported() {
		return xrootdNewerThan4;
	}

	@Override
	public byte protocolID() {
		return 6;
	}

	@Override
	public Protocol clone() {
		final Xrd3cpGW ret = new Xrd3cpGW();

		ret.setDebugLevel(xrdcpdebuglevel);

		return ret;
	}
}
