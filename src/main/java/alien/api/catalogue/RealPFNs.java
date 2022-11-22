package alien.api.catalogue;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import alien.api.Request;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.PFN;
import alien.user.AliEnPrincipal;

/**
 *
 * @author costing
 * @since 2022-11-07
 */
public class RealPFNs extends Request {

	private static final long serialVersionUID = 9149843284882383315L;

	private final Collection<String> lfnsToResolve;

	private Map<LFN, Set<PFN>> realPFNs;

	/**
	 * Resolve the real LFN of an existing LFN object
	 *
	 * @param user
	 *            user who makes the request
	 * @param lfns
	 *            LFNs to resolve to real locations
	 */
	public RealPFNs(final AliEnPrincipal user, final Collection<String> lfns) {
		setRequestUser(user);
		this.lfnsToResolve = lfns;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(lfnsToResolve.toString());
	}

	/**
	 * Resolve the archive member and the real LFN of it
	 *
	 * @param user
	 *            user who makes the request
	 * @param sLFN
	 *            requested file to resolve to physical locations
	 */
	public RealPFNs(final AliEnPrincipal user, final String sLFN) {
		setRequestUser(user);
		lfnsToResolve = List.of(sLFN);
	}

	@Override
	public void run() {
		final List<LFN> lfns = LFNUtils.getLFNs(true, lfnsToResolve);

		realPFNs = new LinkedHashMap<>();

		for (final LFN l : lfns) {
			final Set<PFN> locations = l.whereisReal();

			if (locations != null)
				realPFNs.put(l, locations);
		}

		lfnsToResolve.clear();
	}

	/**
	 * @return all entries returned by the resolver
	 */
	public Map<LFN, Set<PFN>> getMap() {
		return realPFNs;
	}

	/**
	 * @param lfn
	 * @return the physical locations of a given file name
	 */
	public Set<PFN> getPFNs(final String lfn) {
		for (final Map.Entry<LFN, Set<PFN>> entry : realPFNs.entrySet()) {
			if (entry.getKey().getFileName().equals(lfn))
				return entry.getValue();
		}

		return null;
	}
}
