package alien.api.catalogue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import alien.api.Cacheable;
import alien.api.Request;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.PFN;
import alien.se.SEUtils;
import alien.user.AliEnPrincipal;

/**
 *
 * @author ron
 * @since Jun 06, 2011
 */
public class FindfromString extends Request implements Cacheable {

	/**
	 *
	 */
	private static final long serialVersionUID = -5938936122293608584L;
	private final String path;
	private final String pattern;
	private final String query;
	private final int flags;
	private Collection<LFN> lfns;
	private final String xmlCollectionName;
	private Long queueid = Long.valueOf(0);
	private long queryLimit = 1000000;
	private String readSiteSorting = null;
	private Collection<String> excludedPatterns;

	/**
	 * @param user
	 * @param path
	 * @param pattern
	 * @param flags
	 */
	public FindfromString(final AliEnPrincipal user, final String path, final String pattern, final int flags) {
		setRequestUser(user);
		this.path = path;
		this.pattern = pattern;
		this.query = null;
		this.flags = flags;
		this.xmlCollectionName = "";
		this.excludedPatterns = null;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(this.path, this.pattern, this.query, String.valueOf(this.flags), this.xmlCollectionName);
	}

	/**
	 * @param user
	 * @param path
	 * @param pattern
	 * @param query
	 * @param flags
	 * @param xmlCollectionName
	 * @param queueid
	 * @param queryLimit number of entries to limit the search to. If strictly positive, a larger set than this would throw an exception
	 */
	public FindfromString(final AliEnPrincipal user, final String path, final String pattern, final String query, final int flags, final String xmlCollectionName, final Long queueid,
			final long queryLimit) {
		setRequestUser(user);
		this.path = path;
		this.pattern = pattern;
		this.query = query;
		this.flags = flags;
		this.xmlCollectionName = xmlCollectionName;
		this.queueid = queueid;

		if (queryLimit > 0)
			this.queryLimit = queryLimit;

		this.excludedPatterns = null;
	}

	/**
	 * @param user
	 * @param path
	 * @param pattern
	 * @param query
	 * @param flags
	 * @param xmlCollectionName
	 * @param queueid
	 * @param queryLimit number of entries to limit the search to. If strictly positive, a larger set than this would throw an exception
	 * @param readSiteSorting
	 */
	public FindfromString(final AliEnPrincipal user, final String path, final String pattern, final String query, final int flags, final String xmlCollectionName, final Long queueid,
			final long queryLimit, final String readSiteSorting) {
		setRequestUser(user);
		this.path = path;
		this.pattern = pattern;
		this.query = query;
		this.flags = flags;
		this.xmlCollectionName = xmlCollectionName;
		this.queueid = queueid;

		if (queryLimit > 0)
			this.queryLimit = queryLimit;

		this.readSiteSorting = readSiteSorting;

		this.excludedPatterns = null;
	}

	/**
	 * @param user
	 * @param path
	 * @param pattern
	 * @param query
	 * @param flags
	 * @param xmlCollectionName
	 * @param queueid
	 * @param queryLimit number of entries to limit the search to. If strictly positive, a larger set than this would throw an exception
	 * @param readSiteSorting
	 * @param excludedPatterns patterns to remove from matching
	 */
	public FindfromString(final AliEnPrincipal user, final String path, final String pattern, final String query, final int flags, final String xmlCollectionName, final Long queueid,
			final long queryLimit, final String readSiteSorting, final Collection<String> excludedPatterns) {
		setRequestUser(user);
		this.path = path;
		this.pattern = pattern;
		this.query = query;
		this.flags = flags;
		this.xmlCollectionName = xmlCollectionName;
		this.queueid = queueid;

		if (queryLimit > 0)
			this.queryLimit = queryLimit;

		this.readSiteSorting = readSiteSorting;

		this.excludedPatterns = excludedPatterns;
	}

	/**
	 * @param user
	 * @param path
	 * @param pattern
	 * @param flags
	 * @param xmlCollectionName
	 */
	public FindfromString(final AliEnPrincipal user, final String path, final String pattern, final int flags, final String xmlCollectionName) {
		setRequestUser(user);
		this.path = path;
		this.pattern = pattern;
		this.query = null;
		this.flags = flags;
		this.xmlCollectionName = xmlCollectionName;
	}

	@Override
	public void run() {
		lfns = LFNUtils.find(path, pattern, query, flags, getEffectiveRequester(), xmlCollectionName, queueid, readSiteSorting != null ? 1000000 : queryLimit, excludedPatterns);

		if (readSiteSorting != null && lfns != null && lfns.size() > 0) {
			final Map<PFN, LFN> resolvedLFNs = new HashMap<>();

			final Map<LFN, Set<PFN>> locations = LFNUtils.getRealPFNs(lfns);

			for (final Map.Entry<LFN, Set<PFN>> entry : locations.entrySet()) {
				final LFN l = entry.getKey();

				final Set<PFN> pfns = entry.getValue();

				if (pfns != null)
					for (final PFN p : pfns)
						resolvedLFNs.put(p, l);
			}

			final List<PFN> pfns = SEUtils.sortBySite(resolvedLFNs.keySet(), readSiteSorting, false, false);

			lfns = new LinkedHashSet<>(lfns.size());

			for (final PFN p : pfns) {
				lfns.add(resolvedLFNs.get(p));

				if (queryLimit > 0 && lfns.size() >= queryLimit)
					return;
			}
		}
	}

	/**
	 * @return the found LFNs
	 */
	public Collection<LFN> getLFNs() {
		return this.lfns;
	}

	@Override
	public String toString() {
		return "Asked for : path (" + this.path + "), pattern (" + this.pattern + "), flags (" + this.flags + ") reply " + (this.lfns != null ? "contains " + this.lfns.size() + " LFNs" : "is null");
	}

	/**
	 * Made by sraje (Shikhar Raje, IIIT Hyderabad) // *
	 *
	 * @return the list of file names (one level down only) that matched the
	 *         find
	 */
	public List<String> getFileNames() {
		if (lfns == null)
			return null;

		final List<String> ret = new ArrayList<>(lfns.size());

		for (final LFN l : lfns)
			ret.add(l.getFileName());

		return ret;
	}

	@Override
	public String getKey() {
		return path + "|" + pattern + "|" + query + "|" + flags + "|" + queueid + "|" + queryLimit + "|" + readSiteSorting + "|" + excludedPatterns;
	}

	@Override
	public long getTimeout() {
		// small find results, typically the result of OCDB queries, can be cached for longer time
		// larger ones, results of job finds that have to be iterated over, can only be cached for a short period

		if (xmlCollectionName != null)
			return 0;

		return (this.lfns != null && this.lfns.size() < 500) ? 300000 : 60000;
	}
}
