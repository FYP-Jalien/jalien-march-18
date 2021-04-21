/**
 *
 */
package alien.se;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import alien.api.Dispatcher;
import alien.api.ServerException;
import alien.api.catalogue.SEfromString;
import alien.catalogue.CatalogueUtils;
import alien.catalogue.GUID;
import alien.catalogue.GUIDIndex;
import alien.catalogue.GUIDIndex.SEUsageStats;
import alien.catalogue.GUIDUtils;
import alien.catalogue.Host;
import alien.catalogue.PFN;
import alien.config.ConfigUtils;
import lazyj.DBFunctions;
import lazyj.Format;
import lia.util.ShutdownManager;

/**
 * @author costing
 * @since Nov 4, 2010
 */
public final class SEUtils {

	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(SEUtils.class.getCanonicalName());

	private static Map<Integer, SE> seCache = null;

	private static long seCacheUpdated = 0;

	private static final ReentrantReadWriteLock seCacheRWLock = new ReentrantReadWriteLock();
	private static final ReadLock seCacheReadLock = seCacheRWLock.readLock();
	private static final WriteLock seCacheWriteLock = seCacheRWLock.writeLock();

	private static Map<Integer, SECounterUpdate> seCounterUpdates = new ConcurrentHashMap<>();

	private static Map<String, Map<Integer, Double>> seDistance = null;

	private static long seDistanceUpdated = 0;

	private static final ReentrantReadWriteLock seDistanceRWLock = new ReentrantReadWriteLock();
	private static final ReadLock seDistanceReadLock = seDistanceRWLock.readLock();
	private static final WriteLock seDistanceWriteLock = seDistanceRWLock.writeLock();

	private static final String SEDISTANCE_QUERY;

	private static final int maxAllowedRandomPFNs = 10000;

	static {
		if (ConfigUtils.isCentralService()) {
			try (DBFunctions db = ConfigUtils.getDB("alice_users")) {
				db.setReadOnly(true);

				if (db.query("SELECT sitedistance FROM SEDistance LIMIT 0;", true))
					SEDISTANCE_QUERY = "SELECT SQL_NO_CACHE sitename, senumber, sitedistance FROM SEDistance ORDER BY sitename, sitedistance;";
				else
					SEDISTANCE_QUERY = "SELECT SQL_NO_CACHE sitename, senumber, distance FROM SEDistance ORDER BY sitename, distance;";
			}

			updateSECache();
			updateSEDistanceCache();
		}
		else
			SEDISTANCE_QUERY = null;

		ShutdownManager.getInstance().addModule(() -> flushCounterUpdates());
	}

	private static void updateSECache() {
		if (!ConfigUtils.isCentralService())
			return;

		seCacheReadLock.lock();

		try {
			if (System.currentTimeMillis() - seCacheUpdated > CatalogueUtils.CACHE_TIMEOUT || seCache == null) {
				seCacheReadLock.unlock();

				seCacheWriteLock.lock();

				try {
					if (System.currentTimeMillis() - seCacheUpdated > CatalogueUtils.CACHE_TIMEOUT || seCache == null) {
						if (logger.isLoggable(Level.FINER))
							logger.log(Level.FINER, "Updating SE cache");

						flushCounterUpdates();

						try (DBFunctions db = ConfigUtils.getDB("alice_users")) {
							db.setReadOnly(true);
							db.setQueryTimeout(30);

							if (db.query("SELECT SQL_NO_CACHE * FROM SE WHERE (seioDaemons IS NOT NULL OR seName='no_se');")) {
								final Map<Integer, SE> ses = new HashMap<>();

								while (db.moveNext()) {
									final SE se = new SE(db);

									if (se.size >= 0)
										ses.put(Integer.valueOf(se.seNumber), se);
								}

								if (ses.size() > 0) {
									seCache = ses;
									seCacheUpdated = System.currentTimeMillis();
								}
								else {
									if (seCache == null)
										seCache = ses;

									// try again soon
									seCacheUpdated = System.currentTimeMillis() - CatalogueUtils.CACHE_TIMEOUT + 1000 * 30;
								}
							}
							else
								seCacheUpdated = System.currentTimeMillis() - CatalogueUtils.CACHE_TIMEOUT + 1000 * 10;
						}
					}
				}
				finally {
					seCacheWriteLock.unlock();
					seCacheReadLock.lock();
				}
			}
		}
		finally {
			seCacheReadLock.unlock();
		}
	}

	/**
	 * Get the SE by its number
	 *
	 * @param seNumber
	 * @return the SE, if it exists, or <code>null</code> if it doesn't
	 */
	public static SE getSE(final int seNumber) {
		return getSE(Integer.valueOf(seNumber));
	}

	/**
	 * Get the SE by its number
	 *
	 * @param seNumber
	 * @return the SE, if it exists, or <code>null</code> if it doesn't
	 */
	public static SE getSE(final Integer seNumber) {
		if (!ConfigUtils.isCentralService())
			try {
				final SEfromString request = new SEfromString(null, seNumber.intValue());
				final SEfromString response = Dispatcher.execute(request);
				// System.err.println("Response: " + response);
				return response != null ? response.getSE() : null;
			}
			catch (@SuppressWarnings("unused") final ServerException se) {
				return null;
			}

		updateSECache();

		if (seCache == null)
			return null;

		if (seNumber.intValue() <= 0 && seCache.size() > 0)
			return seCache.values().iterator().next();

		return seCache.get(seNumber);
	}

	/**
	 * Get the SE object that has this name
	 *
	 * @param seName
	 * @return SE, if defined, otherwise <code>null</code>
	 */
	public static SE getSE(final String seName) {
		if (seName == null || seName.length() == 0)
			return null;

		if (!ConfigUtils.isCentralService())
			try {
				return Dispatcher.execute(new SEfromString(null, seName)).getSE();
			}
			catch (@SuppressWarnings("unused") final ServerException se) {
				return null;
			}

		updateSECache();

		if (seCache == null)
			return null;

		final Collection<SE> ses = seCache.values();

		final String name = seName.trim().toUpperCase();

		for (final SE se : ses)
			if (se.seName.equals(name))
				return se;

		return null;
	}

	/**
	 * Get all SE objects that have the given names, exactly or matching the regex if the name is a pattern or a fragment of a valid SE name.
	 *
	 * @param ses
	 *            names to get the objects for, can be <code>null</code> in which case all known SEs are returned
	 * @return SE objects matching one of the names/patterns in the argument list
	 */
	public static List<SE> getSEs(final List<String> ses) {
		updateSECache();

		if (seCache == null)
			return null;

		if (ses == null)
			return new ArrayList<>(seCache.values());

		final List<SE> ret = new ArrayList<>();
		for (final String seName : ses) {
			try {
				// if the request was a number, get the storage by ID number
				final int seNo = Integer.parseInt(seName);

				final SE se = getSE(seNo);

				if (se != null) {
					if (!ret.contains(se))
						ret.add(se);

					// matched the number exactly, return as is
					continue;
				}
			}
			catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
				// ignore, go forward
			}

			final SE maybeSE = SEUtils.getSE(seName);

			if (maybeSE != null) {
				if (!ret.contains(maybeSE))
					ret.add(maybeSE);
			}
			else {
				// could it have been a pattern ?
				if (seName.contains("*") || seName.contains("+") || seName.contains(".") || !seName.contains("::") || seName.indexOf("::") == seName.lastIndexOf("::")) {
					try {
						final Pattern p = Pattern.compile("^.*" + seName + ".*$", Pattern.CASE_INSENSITIVE);

						for (final SE se1 : seCache.values()) {
							final Matcher m = p.matcher(se1.seName);

							if (m.matches() && !ret.contains(se1))
								ret.add(se1);
						}
					}
					catch (@SuppressWarnings("unused") final Throwable t) {
						// ignore wrongly specified pattern
					}
				}
			}
		}

		return ret;
	}

	private static void updateSEDistanceCache() {
		if (!ConfigUtils.isCentralService())
			return;

		seDistanceReadLock.lock();

		try {
			if (System.currentTimeMillis() - seDistanceUpdated > CatalogueUtils.CACHE_TIMEOUT || seDistance == null) {
				seDistanceReadLock.unlock();

				seDistanceWriteLock.lock();

				try {
					if (System.currentTimeMillis() - seDistanceUpdated > CatalogueUtils.CACHE_TIMEOUT || seDistance == null) {
						if (logger.isLoggable(Level.FINER))
							logger.log(Level.FINER, "Updating SE Ranks cache");

						try (DBFunctions db = ConfigUtils.getDB("alice_users")) {
							db.setReadOnly(true);
							db.setQueryTimeout(60);

							if (db.query(SEDISTANCE_QUERY)) {
								final Map<String, Map<Integer, Double>> newDistance = new HashMap<>();

								String sOldSite = null;
								Map<Integer, Double> oldMap = null;

								while (db.moveNext()) {
									final String sitename = db.gets(1).trim().toUpperCase();
									final int seNumber = db.geti(2);
									final double distance = db.getd(3);

									if (!sitename.equals(sOldSite) || oldMap == null) {
										oldMap = newDistance.get(sitename);

										if (oldMap == null) {
											oldMap = new LinkedHashMap<>();
											newDistance.put(sitename, oldMap);
										}

										sOldSite = sitename;
									}

									oldMap.put(Integer.valueOf(seNumber), Double.valueOf(distance));
								}

								if (newDistance.size() > 0) {
									seDistance = newDistance;
									seDistanceUpdated = System.currentTimeMillis();
								}
								else {
									if (seDistance == null)
										seDistance = newDistance;

									// try again soon
									seDistanceUpdated = System.currentTimeMillis() - CatalogueUtils.CACHE_TIMEOUT + 1000 * 30;
								}
							}
							else
								seDistanceUpdated = System.currentTimeMillis() - CatalogueUtils.CACHE_TIMEOUT + 1000 * 10;
						}
					}
				}
				finally {
					seDistanceWriteLock.unlock();
				}

				seDistanceReadLock.lock();
			}
		}
		finally {
			seDistanceReadLock.unlock();
		}
	}

	private static final class PFNComparatorBySite implements Serializable, Comparator<PFN> {
		/**
		 *
		 */
		private static final long serialVersionUID = 3852623282834261566L;

		private final Map<Integer, Double> distance;

		private final boolean write;

		public PFNComparatorBySite(final Map<Integer, Double> distance, final boolean write) {
			this.distance = distance;
			this.write = write;
		}

		@Override
		public int compare(final PFN o1, final PFN o2) {
			final Double distance1 = distance.get(Integer.valueOf(o1.seNumber));
			final Double distance2 = distance.get(Integer.valueOf(o2.seNumber));

			if (distance1 == null && distance2 == null)
				// can't decide which is better, there is no ranking info for
				// either
				return 0;

			if (distance1 != null && distance2 != null) {
				// both ranks known, the smallest rank goes higher
				double diff = distance1.doubleValue() - distance2.doubleValue();

				final SE se1 = o1.getSE();
				final SE se2 = o2.getSE();

				if (se1 != null && se2 != null)
					diff += write ? (se1.demoteWrite - se2.demoteWrite) : (se1.demoteRead - se2.demoteRead);

				if (diff < 0)
					return -1;

				if (diff > 0)
					return 1;

				return 0;
			}

			if (distance1 != null)
				// rank is known only for the first one, then this is better
				return -1;

			// the only case left, second one is best
			return 1;
		}
	}

	/**
	 * Get all the SEs available to one site, sorted by the relative distance to the site, exclude exSEs
	 *
	 * @param site
	 * @param write
	 *            <code>true</code> for write operations, <code>false</code> for read
	 * @return sorted list of SEs based on MonALISA distance metric
	 */
	public static List<SE> getClosestSEs(final String site, final boolean write) {
		return getClosestSEs(site, null, write);
	}

	/**
	 * Get all the SEs available to one site, sorted by the relative distance to the site, exclude exSEs
	 *
	 * @param site
	 * @param exSEs
	 * @param write
	 *            <code>true</code> for write operations, <code>false</code> for read
	 * @return sorted list of SEs based on MonALISA distance metric
	 */
	public static List<SE> getClosestSEs(final String site, final List<SE> exSEs, final boolean write) {
		if (site == null || site.length() == 0)
			return getDefaultSEList(write);

		updateSEDistanceCache();

		if (seDistance == null || seDistance.size() == 0)
			return getDefaultSEList(write);

		final String sitename = site.trim().toUpperCase();

		final Map<Integer, Double> distance = seDistance.get(sitename);

		if (distance == null || distance.size() == 0)
			return getDefaultSEList(write);

		final List<SE> ret = new ArrayList<>(distance.size());

		for (final Map.Entry<Integer, Double> me : distance.entrySet()) {
			final SE se = getSE(me.getKey());

			if (se != null && (exSEs == null || !exSEs.contains(se)))
				ret.add(se);
		}

		Collections.sort(ret, new SEComparator(distance, write));

		return ret;
	}

	private static List<SE> getDefaultSEList(final boolean write) {
		final List<SE> allSEs = getSEs(null);

		if (allSEs == null || allSEs.size() == 0)
			return allSEs;

		final Map<Integer, Double> distance = new HashMap<>();

		final Double zero = Double.valueOf(0);

		for (final SE se : allSEs)
			distance.put(Integer.valueOf(se.seNumber), zero);

		Collections.sort(allSEs, new SEComparator(distance, write));

		return allSEs;
	}

	/**
	 * Get if possible all SEs for a certain site with specs
	 *
	 * @param site
	 *            name of the site the job/client currently is
	 * @param ses
	 *            force writing on these SEs irrespective of the QoS
	 * @param exses
	 *            exclude these SEs, usually because the operation was already tried on them
	 * @param qos
	 *            SE type and number of replicas, for writing
	 * @param write
	 *            <code>true</code> for write operations, <code>false</code> for read
	 * @return the list of SEs
	 */
	public static List<SE> getBestSEsOnSpecs(final String site, final List<String> ses, final List<String> exses, final HashMap<String, Integer> qos, final boolean write) {

		if (logger.isLoggable(Level.FINE)) {
			logger.log(Level.FINE, "got pos: " + ses);
			logger.log(Level.FINE, "got neg: " + exses);
			logger.log(Level.FINE, "got qos: " + qos);
		}

		final List<SE> SEs;

		if (ses != null && ses.size() > 0)
			SEs = SEUtils.getSEs(ses);
		else
			SEs = new ArrayList<>();

		List<SE> exSEs = null;

		if (exses != null && exses.size() > 0) {
			exSEs = SEUtils.getSEs(exses);

			if (exSEs != null) {
				SEs.removeAll(exSEs);
				exSEs.addAll(SEs);
			}
		}

		if (exSEs == null)
			exSEs = new ArrayList<>(SEs);

		for (final Map.Entry<String, Integer> qosDef : qos.entrySet())
			if (qosDef.getValue().intValue() > 0) {
				// TODO: get a number #qos.get(qosType) of qosType SEs
				final List<SE> discoveredSEs = SEUtils.getClosestSEs(site, exSEs, write);

				final Iterator<SE> it = discoveredSEs.iterator();

				int counter = 0;

				while (counter < qosDef.getValue().intValue() && it.hasNext()) {
					final SE se = it.next();

					if (!se.isQosType(qosDef.getKey()) || exSEs.contains(se))
						continue;

					SEs.add(se);
					counter++;
				}
			}

		if (logger.isLoggable(Level.FINE))
			logger.log(Level.FINE, "Returning SE list: " + SEs);

		return SEs;
	}

	/**
	 * Sort a collection of PFNs by their relative distance to a given site (where the job is running for example)
	 *
	 * @param pfns
	 * @param sSite
	 * @param removeBrokenSEs
	 * @param write
	 *            <code>true</code> for write operations, <code>false</code> for read
	 * @return the sorted list of locations
	 */
	public static List<PFN> sortBySite(final Collection<PFN> pfns, final String sSite, final boolean removeBrokenSEs, final boolean write) {
		if (pfns == null)
			return null;

		final List<PFN> ret = new ArrayList<>(pfns);

		if (ret.size() <= 1 || sSite == null || sSite.length() == 0)
			return ret;

		updateSEDistanceCache();

		if (seDistance == null)
			return ret;

		final Map<Integer, Double> ranks = seDistance.get(sSite.trim().toUpperCase());

		if (ranks == null)
			return ret;

		if (removeBrokenSEs) {
			final Iterator<PFN> it = ret.iterator();

			while (it.hasNext()) {
				final PFN pfn = it.next();

				if (!ranks.containsKey(Integer.valueOf(pfn.seNumber)))
					it.remove();
			}
		}

		final Comparator<PFN> c = new PFNComparatorBySite(ranks, write);

		Collections.sort(ret, c);

		return ret;
	}

	/**
	 * Sort a collection of PFNs by their relative distance to a given site (where the job is running for example), priorize SEs, exclude exSEs
	 *
	 * @param pfns
	 * @param sSite
	 * @param removeBrokenSEs
	 * @param SEs
	 * @param exSEs
	 * @param write
	 *            <code>true</code> for write operations, <code>false</code> for read
	 * @return the sorted list of locations
	 */
	public static List<PFN> sortBySiteSpecifySEs(final Collection<PFN> pfns, final String sSite, final boolean removeBrokenSEs, final List<SE> SEs, final List<SE> exSEs, final boolean write) {
		final List<PFN> spfns = sortBySite(pfns, sSite, removeBrokenSEs, write);

		if ((SEs == null || SEs.isEmpty()) && (exSEs == null || exSEs.isEmpty()))
			return spfns;

		final List<PFN> tail = new ArrayList<>(spfns.size());
		final List<PFN> ret = new ArrayList<>(spfns.size());

		for (final PFN pfn : spfns)
			if (SEs != null && SEs.contains(pfn.getSE()))
				ret.add(pfn);
			else if (exSEs == null || !exSEs.contains(pfn.getSE()))
				tail.add(pfn);

		ret.addAll(tail);
		return ret;
	}

	/**
	 * @author costing
	 * @since Nov 14, 2010
	 */
	public static final class SEComparator implements Comparator<SE>, Serializable {
		/**
		 *
		 */
		private static final long serialVersionUID = -5231000693345849547L;

		private final Map<Integer, Double> distance;

		private final boolean write;

		/**
		 * @param distance
		 * @param write
		 *            <code>true</code> for write operations, <code>false</code> for read
		 */
		public SEComparator(final Map<Integer, Double> distance, final boolean write) {
			this.distance = distance;
			this.write = write;
		}

		@Override
		public int compare(final SE o1, final SE o2) {
			final Double d1 = distance.get(Integer.valueOf(o1.seNumber));
			final Double d2 = distance.get(Integer.valueOf(o2.seNumber));

			// broken first SE, move the second one to the front if it's ok
			if (d1 == null)
				return d2 == null ? 0 : 1;

			// broken second SE, move the first one to the front
			if (d2 == null)
				return -1;

			// lower rank to the front

			final double rank1 = d1.doubleValue() + (write ? o1.demoteWrite : o1.demoteRead);
			final double rank2 = d2.doubleValue() + (write ? o2.demoteWrite : o2.demoteRead);

			final double diff = rank1 - rank2;

			if (diff < 0)
				return -1;

			if (diff > 0)
				return 1;

			return 0;
		}
	}

	/**
	 * @param ses
	 * @param sSite
	 * @param removeBrokenSEs
	 * @param write
	 *            <code>true</code> for write operations, <code>false</code> for read
	 * @return the sorted list of SEs
	 */
	public static List<SE> sortSEsBySite(final Collection<SE> ses, final String sSite, final boolean removeBrokenSEs, final boolean write) {
		if (ses == null)
			return null;

		final List<SE> ret = new ArrayList<>(ses);

		if ((ret.size() <= 1 || sSite == null || sSite.length() == 0) && (!removeBrokenSEs))
			return ret;

		updateSEDistanceCache();

		if (seDistance == null)
			return null;

		final Map<Integer, Double> ranks = sSite != null ? seDistance.get(sSite.trim().toUpperCase()) : null;

		if (ranks == null)
			// missing information about this site, leave the storages as they
			// are
			return ret;

		if (removeBrokenSEs) {
			final Iterator<SE> it = ret.iterator();

			while (it.hasNext()) {
				final SE se = it.next();

				if (!ranks.containsKey(Integer.valueOf(se.seNumber)))
					it.remove();
			}
		}

		final Comparator<SE> c = new SEComparator(ranks, write);

		Collections.sort(ret, c);

		return ret;
	}

	/**
	 * Get the distance between a site and a target SE
	 *
	 * @param sSite
	 *            reference site
	 * @param toSE
	 *            target se, can be either a {@link SE} object, a name (as String) or a SE number (Integer), anything else will throw an exception
	 * @param write
	 *            <code>true</code> for writing, <code>false</code> for reading
	 * @return the distance (0 = local, 1 = far away, with negative values being strongly preferred and >1 values highly demoted)
	 */
	public static Double getDistance(final String sSite, final Object toSE, final boolean write) {
		if (toSE == null)
			return null;

		final SE se;

		if (toSE instanceof SE)
			se = (SE) toSE;
		else if (toSE instanceof String)
			se = getSE((String) toSE);
		else if (toSE instanceof Integer)
			se = getSE((Integer) toSE);
		else
			throw new IllegalArgumentException("Invalid object type for the toSE parameter: " + toSE.getClass().getCanonicalName());

		if (se == null)
			return null;

		updateSEDistanceCache();

		if (seDistance == null)
			return null;

		final Map<Integer, Double> ranks = seDistance.get(sSite.trim().toUpperCase());

		if (ranks == null)
			return null;

		final Double distance = ranks.get(Integer.valueOf(se.seNumber));

		if (distance == null)
			return null;

		final double d = distance.doubleValue() + (write ? se.demoteWrite : se.demoteRead);

		return Double.valueOf(d);
	}

	/**
	 * Update the number of files and the total size for each known SE, according to the G*L and G*L_PFN tables
	 */
	public static void updateSEUsageCache() {
		logger.log(Level.INFO, "Updating SE usage cache data");

		final long lStart = System.currentTimeMillis();

		final Map<Integer, SEUsageStats> m = getSEUsage();

		try (DBFunctions db = ConfigUtils.getDB("alice_users")) {
			db.setReadOnly(false);
			db.setQueryTimeout(60);

			for (final Map.Entry<Integer, SEUsageStats> entry : m.entrySet()) {
				db.query("UPDATE SE SET seUsedSpace=?, seNumFiles=? WHERE seNumber=?;", false, Long.valueOf(entry.getValue().usedSpace), Long.valueOf(entry.getValue().fileCount), entry.getKey());

				final SE se = getSE(entry.getKey().intValue());

				if (se != null) {
					se.seUsedSpace = entry.getValue().usedSpace;
					se.seNumFiles = entry.getValue().fileCount;
				}
			}
		}

		logger.log(Level.INFO, "Finished updating SE usage cache data, took " + Format.toInterval(System.currentTimeMillis() - lStart));
	}

	private static Map<Integer, SEUsageStats> getSEUsage() {
		final Map<Integer, SEUsageStats> m = new HashMap<>();

		for (final GUIDIndex index : CatalogueUtils.getAllGUIDIndexes()) {
			logger.log(Level.FINE, "Getting usage from " + index);

			final Map<Integer, SEUsageStats> t = index.getSEUsageStats();

			for (final Map.Entry<Integer, SEUsageStats> entry : t.entrySet()) {
				final SEUsageStats s = m.get(entry.getKey());

				if (s == null)
					m.put(entry.getKey(), entry.getValue());
				else
					s.merge(entry.getValue());
			}
		}

		return m;
	}

	/**
	 * Debug method
	 *
	 * @param args
	 */
	public static void main(final String[] args) {
		updateSEUsageCache();
	}

	/**
	 * @param purge
	 *            if <code>true</code> then all PFNs present on these SEs will be queued for physical deletion from the storage. Set it to <code>false</code> if the content was already deleted, the
	 *            storage was disconnected or there is another reason why you don't care if the storage is cleaned or not.
	 * @param seNames
	 *            list of SE names to remove from the catalogue
	 * @throws IOException
	 */
	public static void purgeSE(final boolean purge, final String... seNames) throws IOException {
		final Set<SE> ses = new HashSet<>();

		for (final String seName : seNames) {
			final SE se = SEUtils.getSE(seName);

			if (se == null) {
				System.err.println("Unknown SE: " + seName);

				return;
			}

			ses.add(se);
		}

		if (ses.size() == 0)
			return;

		purgeSE(purge, ses.toArray(new SE[0]));
	}

	/**
	 * Remove all catalogue entries corresponding to these SEs. To be called when a storage is decomissioned for example.
	 *
	 * @param purge
	 * @param ses
	 * @throws IOException
	 */
	public static void purgeSE(final boolean purge, final SE... ses) throws IOException {
		final StringBuilder sbSE = new StringBuilder();

		for (final SE se : ses) {
			System.err.println("Deleting all replicas from: " + se);

			if (sbSE.length() > 0)
				sbSE.append(',');

			sbSE.append(se.seNumber);
		}

		try (PrintWriter pw = new PrintWriter(new FileWriter("orphaned_guids.txt", true))) {

			final int copies[] = new int[10];

			int cnt = 0;

			for (final GUIDIndex idx : CatalogueUtils.getAllGUIDIndexes()) {
				final Host h = CatalogueUtils.getHost(idx.hostIndex);

				try (DBFunctions gdb = h.getDB()) {
					gdb.setReadOnly(true);

					gdb.query("select binary2string(guid) from G" + idx.tableName + "L inner join G" + idx.tableName + "L_PFN using (guidId) WHERE seNumber IN (" + sbSE + ");");

					while (gdb.moveNext()) {
						if ((++cnt) % 10000 == 0)
							System.err.println(cnt);

						final String sguid = gdb.gets(1);

						final GUID g = GUIDUtils.getGUID(sguid);

						if (g == null) {
							System.err.println("Unexpected: cannot load the GUID content of " + gdb.gets(1));
							continue;
						}

						for (final SE se : ses) {
							if (true) {
								g.removePFN(se, purge);

								if (g.getPFNs().size() == 0) {
									System.err.println("Orphaned GUID " + sguid);

									pw.println(sguid);
								}
							}

							copies[Math.min(g.getPFNs().size(), copies.length - 1)]++;
						}
					}
				}
			}

			for (int i = 0; i < copies.length; i++)
				System.err.println(i + " replicas: " + copies[i]);
		}
	}

	/**
	 * Redirect all files indicated to be on the source SE to point to the destination SE instead. To be used if a temporary SE took in all the files of an older SE by other means than the AliEn data
	 * management tools.
	 *
	 * @param seSource
	 *            source SE, to be freed
	 * @param seDest
	 *            destination SE, that accepted the files already
	 * @param debug
	 *            if <code>true</code> then the operation will NOT be executed, the queries will only be logged on screen for manually checking them. It is recommended to run it once in this more and
	 *            only if everything looks ok to commit to executing it, by passing <code>false</code> as this parameter.
	 */
	public static void moveSEContent(final String seSource, final String seDest, final boolean debug) {
		final SE source = SEUtils.getSE(seSource);
		final SE dest = SEUtils.getSE(seDest);

		if (source == null || dest == null) {
			System.err.println("Invalid SEs");
			return;
		}

		System.err.println("Renumbering " + source.seNumber + " to " + dest.seNumber);

		for (final GUIDIndex idx : CatalogueUtils.getAllGUIDIndexes())
			try (DBFunctions db = CatalogueUtils.getHost(idx.hostIndex).getDB()) {
				if (db == null) {
					System.err.println("Cannot get DB for " + idx);
					continue;
				}

				String q1 = "UPDATE G" + idx.tableName + "L_PFN SET seNumber=" + dest.seNumber;

				if (!source.seStoragePath.equals(dest.seStoragePath))
					q1 += ", pfn=replace(replace(pfn, '" + Format.escSQL(source.seioDaemons) + "', '" + Format.escSQL(dest.seioDaemons) + "'), '"
							+ Format.escSQL(SE.generateProtocol(dest.seioDaemons, source.seStoragePath)) + "', '" + Format.escSQL(SE.generateProtocol(dest.seioDaemons, dest.seStoragePath)) + "')";
				else if (!source.seioDaemons.equals(dest.seioDaemons))
					q1 += ", pfn=replace(pfn, '" + Format.escSQL(source.seioDaemons) + "', '" + Format.escSQL(dest.seioDaemons) + "')";

				q1 += " WHERE seNumber=" + source.seNumber;

				final String q2 = "UPDATE G" + idx.tableName + "L SET seStringlist=replace(sestringlist,'," + source.seNumber + ",','," + dest.seNumber + ",') WHERE seStringlist LIKE '%,"
						+ source.seNumber + ",%';";

				if (debug) {
					System.err.println(q1);
					System.err.println(q2);
				}
				else {
					boolean ok = db.query(q1);
					System.err.println(q1 + " : " + ok + " : " + db.getUpdateCount());

					ok = db.query(q2);

					System.err.println(q2 + " : " + ok + " : " + db.getUpdateCount());
				}
			}
	}

	private static final class SECounterUpdate {
		public AtomicLong aiFiles = new AtomicLong(0);
		public AtomicLong aiBytes = new AtomicLong(0);

		public SECounterUpdate() {
			// nothing to do here
		}

		public void updateCounters(final long deltaFiles, final long deltaBytes) {
			aiFiles.addAndGet(deltaFiles);
			aiBytes.addAndGet(deltaBytes);
		}

		public void flush(final Integer seNumber) {
			final long deltaFiles = aiFiles.getAndSet(0);
			final long deltaBytes = aiBytes.getAndSet(0);

			if (deltaFiles != 0 || deltaBytes != 0)
				try (DBFunctions db = ConfigUtils.getDB("alice_users")) {
					db.setReadOnly(false);
					db.setQueryTimeout(60);

					if (!db.query("UPDATE SE SET seUsedSpace=greatest(seUsedSpace" + (deltaBytes >= 0 ? "+" : "") + "?, 0), seNumFiles=greatest(seNumFiles" + (deltaFiles >= 0 ? "+" : "")
							+ "?, 0) WHERE seNumber=?;", false, Long.valueOf(deltaBytes), Long.valueOf(deltaFiles), seNumber)) {
						aiFiles.addAndGet(deltaFiles);
						aiBytes.addAndGet(deltaBytes);
					}
				}
		}
	}

	/**
	 * Update the storage counters when files are added or removed from them. This does not guarantee counter consistency!
	 *
	 * @param seNumber
	 *            SE number
	 * @param deltaFiles
	 *            how many files were added (positive) or removed (negative)
	 * @param deltaBytes
	 *            how many bytes were added (positive) or removed (negative)
	 */
	public static void incrementStorageCounters(final int seNumber, final long deltaFiles, final long deltaBytes) {
		final Integer seNo = Integer.valueOf(seNumber);
		SECounterUpdate update = seCounterUpdates.get(seNo);

		if (update == null) {
			update = new SECounterUpdate();
			seCounterUpdates.put(seNo, update);
		}

		update.updateCounters(deltaFiles, deltaBytes);
	}

	/**
	 * Flush changes to storage usage counters to disk
	 */
	private static void flushCounterUpdates() {
		for (final Map.Entry<Integer, SECounterUpdate> entry : seCounterUpdates.entrySet())
			entry.getValue().flush(entry.getKey());
	}

	/**
	 * Dump all PFNs present in the given SEs in individual CSV files named "<SE name>.file_list", with the following format:<br>
	 * #PFN,size,MD5
	 *
	 * @param realPFNs
	 *            if <code>true</code> the catalogue PFNs will be written, if <code>false</code> the PFNs will be generated from the code again. It should be set to <code>false</code> if there were
	 *            any reindexing done in the database and the PFN strings still point to the old SE.
	 * @param ses
	 *            SEs to dump the content from
	 * @throws IOException
	 */
	public static void masterSE(final boolean realPFNs, final String... ses) throws IOException {
		final NumberFormat twoDigits = new DecimalFormat("00");
		final NumberFormat fiveDigits = new DecimalFormat("00000");

		for (final String seName : ses) {
			long fileCount = 0;
			long totalSize = 0;

			final SE se = SEUtils.getSE(seName);

			try (PrintWriter pw = new PrintWriter(new FileWriter(seName + ".file_list"))) {
				pw.println("#PFN,size,MD5,ctime");

				for (final GUIDIndex idx : CatalogueUtils.getAllGUIDIndexes()) {
					final Host h = CatalogueUtils.getHost(idx.hostIndex);

					try (DBFunctions gdb = h.getDB()) {
						gdb.setReadOnly(true);

						if (realPFNs) {
							gdb.query("select distinct pfn,size,md5,binary2string(guid) from G" + idx.tableName + "L inner join G" + idx.tableName + "L_PFN using (guidId) WHERE seNumber="
									+ se.seNumber + ";");

							while (gdb.moveNext()) {
								pw.print(gdb.gets(1) + "," + gdb.getl(2) + "," + gdb.gets(3) + ",");

								totalSize += gdb.getl(2);
								fileCount++;

								try {
									final UUID u = UUID.fromString(gdb.gets(4));
									pw.print(GUIDUtils.epochTime(u));
								}
								catch (@SuppressWarnings("unused") final Throwable t) {
									// ignore any errors
								}

								pw.println();
							}
						}
						else {
							gdb.query("select distinct binary2string(guid),size,md5 from G" + idx.tableName + "L INNER JOIN G" + idx.tableName + "L_PFN using(guidId) where seNumber=?", true,
									Integer.valueOf(se.seNumber));

							while (gdb.moveNext()) {
								final String guid = gdb.gets(1);

								pw.print(twoDigits.format(GUID.getCHash(guid)) + "/" + fiveDigits.format(GUID.getHash(guid)) + "/" + guid + "," + gdb.getl(2) + "," + gdb.gets(3) + ",");

								totalSize += gdb.getl(2);
								fileCount++;

								try {
									final UUID u = UUID.fromString(guid);
									pw.print(GUIDUtils.epochTime(u));
								}
								catch (@SuppressWarnings("unused") final Throwable t) {
									// ignore any errors
								}

								pw.println();
							}
						}
					}
				}
			}

			try (DBFunctions db = ConfigUtils.getDB("alice_users")) {
				db.setReadOnly(false);
				db.setQueryTimeout(60);

				db.query("UPDATE SE SET seUsedSpace=?, seNumFiles=? WHERE seNumber=?;", false, Long.valueOf(totalSize), Long.valueOf(fileCount), Integer.valueOf(se.seNumber));

				se.seUsedSpace = totalSize;
				se.seNumFiles = fileCount;
			}
		}
	}

	/**
	 * @param storageNumber
	 * @param fileCount
	 * @return at most <code>fileCount</code> random PFNs associated to this storage ID
	 */
	public static Collection<PFN> getRandomPFNs(final int storageNumber, final int fileCount) {
		final Set<PFN> pfns = new HashSet<>();
		final SE se = getSE(storageNumber);

		if (se == null) {
			logger.log(Level.WARNING, "Cannot find SE with number " + storageNumber);
			return null;
		}

		if (fileCount <= 0) {
			return null;
		}

		final int targetFileCount = Math.min(fileCount, maxAllowedRandomPFNs);

		List<GUIDIndex> guidIndices = CatalogueUtils.getAllGUIDIndexes();

		if (guidIndices != null) {
			guidIndices = new ArrayList<>(guidIndices);
			Collections.shuffle(guidIndices);

			final Iterator<GUIDIndex> it = guidIndices.iterator();

			while (pfns.size() < targetFileCount && it.hasNext()) {
				final GUIDIndex idx = it.next();

				final Host h = CatalogueUtils.getHost(idx.hostIndex);

				try (DBFunctions gdb = h.getDB()) {
					gdb.setReadOnly(true);

					final String q = "select pfn,binary2string(guid),size from G" + idx.tableName + "L inner join G" + idx.tableName + "L_PFN using (guidId) where seNumber=" + se.seNumber
							+ " order by rand() limit " + (targetFileCount - pfns.size());

					gdb.query(q);

					while (gdb.moveNext()) {
						pfns.add(new PFN(Integer.valueOf(se.seNumber), gdb.gets(1), UUID.fromString(gdb.gets(2)), gdb.getl(3)));
					}
				}
				catch (final Exception e) {
					logger.log(Level.WARNING, "Exception occurred when trying to get random files from SE " + storageNumber, e);
				}
			}
		}
		else {
			logger.log(Level.WARNING, "CatalogueUtils.getAllGUIDIndexes returned null");
		}

		return pfns;
	}
}
