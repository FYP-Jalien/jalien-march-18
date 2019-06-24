package alien.api.catalogue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import alien.api.Cacheable;
import alien.api.Request;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
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
	}

	/**
	 * @param user
	 * @param path
	 * @param pattern
	 * @param query
	 * @param flags
	 * @param xmlCollectionName
	 * @param queueid
	 */
	public FindfromString(final AliEnPrincipal user, final String path, final String pattern, final String query, final int flags, final String xmlCollectionName, final Long queueid) {
		setRequestUser(user);
		this.path = path;
		this.pattern = pattern;
		this.query = query;
		this.flags = flags;
		this.xmlCollectionName = xmlCollectionName;
		this.queueid = queueid;
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
		lfns = LFNUtils.find(path, pattern, query, flags, getEffectiveRequester(), xmlCollectionName, queueid);
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
		return path + "|" + pattern + "|" + query + "|" + flags + "|" + queueid;
	}

	@Override
	public long getTimeout() {
		// small find results, typically the result of OCDB queries, can be cached for longer time
		// larger ones, results of job finds that have to be iterated over, can only be cached for a short period

		return (this.lfns != null || this.lfns.size() < 500) ? 300000 : 60000;
	}
}
