/**
 *
 */
package alien.io;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.catalogue.GUID;
import alien.catalogue.PFN;
import alien.config.ConfigUtils;
import alien.io.protocols.Factory;
import alien.io.protocols.Protocol;
import alien.io.protocols.SourceException;
import alien.io.protocols.TargetException;
import alien.io.protocols.TempFileManager;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import alien.se.SE;
import alien.se.SEUtils;
import lazyj.DBFunctions;

/**
 * @author costing
 * @since Dec 8, 2010
 */
public class Transfer implements Serializable, Runnable {
	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(Transfer.class.getCanonicalName());

	/**
	 * Monitoring component
	 */
	static transient final Monitor monitor = MonitorFactory.getMonitor(Transfer.class.getCanonicalName());

	/**
	 * Transfer was successful
	 */
	public static final int OK = 0;

	/**
	 * Failed from internal reasons
	 */
	public static final int FAILED_SYSTEM = 1;

	/**
	 * Transfer failed reading the source
	 */
	public static final int FAILED_SOURCE = 2;

	/**
	 * Transfer failed writing the target
	 */
	public static final int FAILED_TARGET = 3;

	/**
	 * It is not clear from the message if the source or the target was a problem
	 */
	public static final int FAILED_UNKNOWN = 4;

	/**
	 * Transfer should be retried later (currently staging from tape for example)
	 */
	public static final int DELAYED = 10;

	/**
	 *
	 */
	private static final long serialVersionUID = -4620016257875988468L;

	private final long transferId;

	/**
	 * Source pfns, package protected
	 */
	final Collection<PFN> sources;

	/**
	 * Target pfn, package protected
	 */
	final Collection<PFN> targets;

	private String targetPFN;

	private String storageReplyEnvelope;

	private GUID referenceGUID = null;

	/**
	 * Last tried source
	 */
	int lastTriedSE = -1;

	/**
	 * Last tried protocol
	 */
	Protocol lastTriedProtocol = null;

	/**
	 * when the transfer was issued
	 */
	final long startedWork = System.currentTimeMillis();

	private final Collection<PFN> successfulTransfers = new ArrayList<>();
	private final Collection<PFN> failedTransfers = new ArrayList<>();

	/**
	 * Which of the replica to remove after a successful transfer
	 */
	public final String onCompleteRemoveReplica;

	/**
	 * @param transferId
	 *            transfer ID
	 * @param sources
	 *            source PFNs (one or more, sorted by preference)
	 * @param targets
	 *            target PFN, can be <code>null</code> if the file is to be copied to the local disk in a temporary file
	 * @param onCompleteRemoveReplica
	 *            replica to remove after a successful transfer
	 */
	public Transfer(final long transferId, final Collection<PFN> sources, final Collection<PFN> targets, final String onCompleteRemoveReplica) {
		this.sources = sources;

		if (this.sources == null || this.sources.size() == 0)
			throw new IllegalArgumentException("No sources for this transfer");

		this.targets = targets;

		this.transferId = transferId;

		this.onCompleteRemoveReplica = onCompleteRemoveReplica;
	}

	/**
	 * Get the list of protocols via which this PFN can be accessed
	 *
	 * @param pfn
	 * @return list of protocols
	 */
	public static List<Protocol> getAccessProtocols(final PFN pfn) {
		return getProtocols(pfn, true);
	}

	/**
	 * Get the protocols supported by this guy
	 *
	 * @param pfn
	 * @return list of protocols
	 */
	public static List<Protocol> getProtocols(final PFN pfn) {
		return getProtocols(pfn, false);
	}

	private static List<Protocol> getProtocols(final PFN pfn, final boolean onlyAccess) {
		final List<Protocol> ret = new LinkedList<>();

		if (pfn == null)
			return ret;

		final String sPFN = pfn.pfn;

		if (sPFN == null || sPFN.length() == 0)
			return ret;

		final int idx = sPFN.indexOf("://");

		if (idx <= 0)
			return ret;

		final String s = sPFN.substring(0, idx).trim().toLowerCase();

		if (s.equals("root")) {
			if (!onlyAccess) {
				if (Factory.xrd3cp.isSupported())
					ret.add((Protocol) Factory.xrd3cp.clone());

				if (Factory.xrd3cp4.isSupported())
					ret.add((Protocol) Factory.xrd3cp4.clone());
			}

			if (Factory.xrootd.isSupported()) {
				if (!pfn.isCsd)
					ret.add((Protocol) Factory.xrootd.clone());
				else
					ret.add((Protocol) Factory.xrootdcsd.clone());
			}
		}
		else
			if (s.equals("http")) {
				if (Factory.http.isSupported())
					ret.add((Protocol) Factory.http.clone());
			}
			else
				if (s.equals("torrent")) {
					if (Factory.torrent.isSupported())
						ret.add((Protocol) Factory.torrent.clone());
				}
				else
					if (s.equals("file"))
						if (Factory.cp.isSupported())
							ret.add((Protocol) Factory.cp.clone());

		return ret;
	}

	/**
	 * Get the protocols that are common between these two PFNs
	 *
	 * @param source
	 * @param target
	 *            target PFN (can be <code>null</code>, meaning a local temporary file)
	 * @return protocols that match both
	 */
	public static List<Protocol> getProtocols(final PFN source, final PFN target) {
		final List<Protocol> ret = getProtocols(source);

		final List<Protocol> targetProtocols = getProtocols(target);

		ret.retainAll(targetProtocols);

		// SE targetSE = target.getSE();
		//
		// if (targetSE!=null && targetSE.getName().toLowerCase().indexOf("::eos")>=0)
		// ret.remove(Factory.xrd3cp);

		return ret;
	}

	private int exitCode = -1;

	private String failureReason = null;

	/*
	 * (non-Javadoc)
	 *
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		for (final PFN target : targets) {
			final Timing timing = new Timing();

			doWork(target);

			if (exitCode == OK)
				successfulTransfers.add(target);
			else
				failedTransfers.add(target);

			timing.endTiming();

			if (monitor != null) {
				monitor.addMeasurement("transfer_time", timing);

				monitor.incrementCounter("transfer_status_" + exitCode);

				if (exitCode == 0 && referenceGUID != null)
					monitor.addMeasurement("transfer_MB", referenceGUID.size / (1024 * 1024d));
			}
		}
	}

	private static Set<Integer> SE_FORCED_3RD = null;

	private static long lastForced3RDRefresh = System.currentTimeMillis();

	private static synchronized Set<Integer> getForced3RDMap() {
		if (SE_FORCED_3RD == null || (System.currentTimeMillis() - lastForced3RDRefresh > 1000 * 60)) {
			SE_FORCED_3RD = new HashSet<>();

			try (DBFunctions db = ConfigUtils.getDB("transfers")) {
				if (db != null) {
					db.query("SELECT sename,protocol FROM PROTOCOLS ORDER BY 1,2;");

					while (db.moveNext()) {
						final SE se = SEUtils.getSE(db.gets(1));

						if (se != null) {
							final Integer key = Integer.valueOf(se.seNumber);

							if (db.gets(2).equals("xrd3cp"))
								SE_FORCED_3RD.add(key);
							else
								if (db.gets(2).equals("xrdcp"))
									SE_FORCED_3RD.remove(key);
						}
					}
				}
			}

			lastForced3RDRefresh = System.currentTimeMillis();
		}

		return SE_FORCED_3RD;
	}

	private static boolean isTPCForced(final PFN pfn) {
		return getForced3RDMap().contains(Integer.valueOf(pfn.seNumber));
	}

	private void doWork(final PFN target) {
		if (target == null)
			return;

		// try the best protocols first
		final Set<Protocol> protocols = new HashSet<>();

		boolean forceTPC = isTPCForced(target);

		for (final PFN source : sources) {
			if (referenceGUID == null)
				referenceGUID = source.getGuid();

			final List<Protocol> common = getProtocols(source, target);

			if (common != null && common.size() > 0)
				protocols.addAll(common);

			forceTPC = forceTPC && isTPCForced(source);
		}

		if (protocols.size() == 0) {
			// no common protocols
			final List<PFN> sortedSources = SEUtils.sortBySite(sources, ConfigUtils.getSite(), false, false);

			for (final PFN source : sortedSources) {
				doWork(source, target);

				lastTriedSE = source.seNumber;

				if (exitCode == OK || exitCode == FAILED_TARGET)
					break;
			}

			return;
		}

		final SE s = target.getSE();

		if (s == null) {
			exitCode = FAILED_TARGET;
			failureReason = "Target is null";

			return;
		}

		String targetSite = s.getName();

		targetSite = targetSite.substring(targetSite.indexOf("::") + 2, targetSite.lastIndexOf("::"));

		if (logger.isLoggable(Level.FINE))
			logger.log(Level.FINE, transferId + " : Target site: " + targetSite);

		// sort protocols by preference
		final List<Protocol> sortedProtocols = new ArrayList<>(TransferUtils.filterProtocols(s, protocols));

		if (forceTPC) {
			// if both Xrootd and Xrd3cp4 are in the list then remove Xrootd

			boolean containsXrd3cp4 = false;
			Protocol xrootd = null;

			for (final Protocol p : sortedProtocols)
				if (p.protocolID() == Factory.xrootd.protocolID())
					xrootd = p;
				else
					if (p.protocolID() == Factory.xrd3cp4.protocolID())
						containsXrd3cp4 = true;

			if (containsXrd3cp4 && xrootd != null)
				sortedProtocols.remove(xrootd);
		}

		Collections.sort(sortedProtocols);

		if (logger.isLoggable(Level.FINE))
			logger.log(Level.FINE, transferId + " : Sorted protocols : " + sortedProtocols + ", force TPC : " + forceTPC);

		for (final Protocol p : sortedProtocols) {
			// sort pfns function of the distance between source, target and
			// ourselves

			final List<PFN> sortedSources = SEUtils.sortBySite(sources, (p == Factory.xrd3cp || p == Factory.xrd3cp4) ? targetSite : ConfigUtils.getSite(), false, false);

			final Set<PFN> brokenSources = new HashSet<>();

			lastTriedProtocol = p;

			for (final PFN source : sortedSources) {
				if (!getProtocols(source).contains(p)) {
					if (logger.isLoggable(Level.FINER))
						logger.log(Level.FINER, transferId + " : Will not apply protocol " + p + " on " + source.getPFN());

					continue;
				}

				if (logger.isLoggable(Level.FINER))
					logger.log(Level.FINER, transferId + " : Trying protocol " + p + " on " + source.getPFN());

				lastTriedSE = source.seNumber;

				doWork(p, source, target);

				// if the target is broken, don't try again the same protocol
				if (exitCode == OK || exitCode == FAILED_TARGET)
					break;

				if (exitCode == FAILED_SOURCE) {
					if (logger.isLoggable(Level.FINE))
						logger.log(Level.FINE, transferId + " : Removing source " + source.getPFN() + " because it is broken");

					brokenSources.add(source);
				}
			}

			if (exitCode == OK)
				break;

			sortedSources.removeAll(brokenSources);
		}

		if (exitCode < 0)
			exitCode = FAILED_SYSTEM;

		if (failureReason == null)
			failureReason = "None of the protocols managed to perform the transfer";
	}

	private void doWork(final PFN source, final PFN target) {
		final List<Protocol> protocolsSource = getProtocols(source);

		if (protocolsSource.size() == 0) {
			exitCode = FAILED_SOURCE;
			failureReason = "No known protocols for source PFN " + source;
			return;
		}

		if (target == null) {
			// file should be written locally

			for (final Protocol p : protocolsSource) {
				lastTriedProtocol = p;

				try {
					targetPFN = p.get(source, null).getCanonicalPath();

					exitCode = OK;
					failureReason = null;
					return;
				} catch (@SuppressWarnings("unused") final UnsupportedOperationException uoe) {
					// ignore
				} catch (final IOException ioe) {
					exitCode = FAILED_SOURCE;
					failureReason = ioe.getMessage();
				}
			}

			return;
		}

		final List<Protocol> protocolsTarget = getProtocols(target);

		if (protocolsTarget.size() == 0) {
			exitCode = FAILED_TARGET;
			failureReason = "No known protocols for target PFN " + target;
			return;
		}

		File temp = null;

		for (final Protocol p : protocolsSource) {
			lastTriedProtocol = p;

			try {
				temp = p.get(source, null);
				break;
			} catch (@SuppressWarnings("unused") final UnsupportedOperationException uoe) {
				// ignore
			} catch (final IOException ioe) {
				exitCode = FAILED_SOURCE;
				failureReason = ioe.getMessage();
			}
		}

		if (temp == null)
			return;

		for (final Protocol p : protocolsTarget) {
			lastTriedProtocol = p;

			try {
				storageReplyEnvelope = p.put(target, temp);
				exitCode = OK;
				failureReason = null;

				targetPFN = target.pfn;

				return;
			} catch (@SuppressWarnings("unused") final UnsupportedOperationException uoe) {
				// ignore
			} catch (final IOException ioe) {
				exitCode = FAILED_TARGET;
				failureReason = ioe.getMessage();
			} finally {
				TempFileManager.release(temp);
			}
		}

		return;
	}

	private void doWork(final Protocol p, final PFN source, final PFN target) {
		try {
			storageReplyEnvelope = p.transfer(source, target);

			exitCode = OK;
			failureReason = "OK: " + p.getClass().getSimpleName() + " (" + source.getPFN() + " -> " + target.getPFN() + ")";

			targetPFN = target.pfn;

			return;
		} catch (@SuppressWarnings("unused") final UnsupportedOperationException uoe) {
			// ignore, move to the next one
		} catch (final SourceException se) {
			exitCode = FAILED_SOURCE;
			failureReason = se.getMessage();

			logger.log(Level.WARNING,
					"Transfer " + transferId + ", " + p.getClass().getSimpleName() + " (" + source.getPFN() + " -> " + target.getPFN() + ") failed with source exception: " + failureReason);
		} catch (final TargetException se) {
			exitCode = FAILED_TARGET;
			failureReason = se.getMessage();

			logger.log(Level.WARNING,
					"Transfer " + transferId + ", " + p.getClass().getSimpleName() + " (" + source.getPFN() + " -> " + target.getPFN() + ") failed with target exception: " + failureReason);
		} catch (final IOException e) {
			exitCode = FAILED_SYSTEM;
			failureReason = e.getMessage();

			logger.log(Level.WARNING,
					"Transfer " + transferId + ", " + p.getClass().getSimpleName() + " (" + source.getPFN() + " -> " + target.getPFN() + ") failed with generic exception: " + failureReason);
		} finally {
			TransferUtils.logAttempt(p, source, target, exitCode, failureReason);
		}
	}

	/**
	 * @return the exit code, if &lt;0 then the operation is ongoing still, if 0 then the transfer was successful, otherwise it failed and the message is in {@link #getFailureReason()}
	 */
	public int getExitCode() {
		return exitCode;
	}

	/**
	 * @return the failure reason, if any
	 */
	public String getFailureReason() {
		return failureReason;
	}

	/**
	 * For a successful operation, get the PFN of the target, either the local (no protocol) or remote (with protocol) file
	 *
	 * @return target PFN
	 */
	public String getTargetPFN() {
		return targetPFN;
	}

	/**
	 * For a successful operation, get the reply envelope for the storage
	 *
	 * @return storage reply envelope
	 */
	public String getStorageReplyEnvelope() {
		return storageReplyEnvelope;
	}

	/**
	 * @return the transfer ID
	 */
	public long getTransferId() {
		return transferId;
	}

	@Override
	public String toString() {
		return "ID: " + transferId + ", exitCode: " + exitCode + ", reason: " + failureReason;
	}

	/**
	 * @return from the list of targets, which were successfully executed
	 */
	public Collection<PFN> getSuccessfulTransfers() {
		return successfulTransfers;
	}

	/**
	 * @return from the list of targets, which failed
	 */
	public Collection<PFN> getFailedTransfers() {
		return failedTransfers;
	}
}
