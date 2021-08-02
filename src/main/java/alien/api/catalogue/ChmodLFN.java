package alien.api.catalogue;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import alien.api.Request;
import alien.catalogue.CatalogEntity;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.user.AliEnPrincipal;
import alien.user.AuthorizationChecker;

/**
 * chmod request
 */
public class ChmodLFN extends Request {
	/**
	 *
	 */
	private static final long serialVersionUID = -4209526023185462132L;
	private final String path;
	private final String newMode;
	private boolean success;
	private final boolean recursive;
	private HashMap<String, Boolean> results;

	/**
	 * @param user user who requested this operation
	 * @param fpath path to alter mode of
	 * @param newMode octal style of access permissions
	 * @param recursive whether to act recursively or not
	 */
	public ChmodLFN(final AliEnPrincipal user, final String fpath, final String newMode, final boolean recursive) {
		setRequestUser(user);
		path = fpath;
		this.newMode = newMode;
		this.recursive = recursive;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(path, newMode, String.valueOf(this.recursive));
	}

	@Override
	public void run() {
		// if( !AliEnPrincipal.roleIsAdmin( getEffectiveRequester().getName() ) )
		// throw new SecurityException( "Only administrators can do it" );

		results = new HashMap<>();

		final CatalogEntity c = LFNUtils.getLFN(path);
		if (!AuthorizationChecker.isOwner(c, getEffectiveRequester()))
			success = false;
		else
			// throw new SecurityException("You do not own this file: " + c +
			// ", requester: " + getEffectiveRequester() );
			success = LFNUtils.chmodLFN(path, newMode);
		results.put(path, Boolean.valueOf(success));

		if (!recursive || !success)
			return;

		final Collection<LFN> lfns = LFNUtils.find(path, "*", LFNUtils.FIND_INCLUDE_DIRS | LFNUtils.FIND_NO_SORT);

		if (lfns == null) {
			success = false;
			return;
		}

		for (final LFN l : lfns) {
			if (!AuthorizationChecker.isOwner(l, getEffectiveRequester()))
				success = false;
			else
				// throw new SecurityException("You do not own this file: " + l +
				// ", requester: " + getEffectiveRequester() );
				success = LFNUtils.chmodLFN(l.getCanonicalName(), newMode);
			results.put(path, Boolean.valueOf(success));
		}
	}

	/**
	 * @return operation status
	 */
	public boolean getSuccess() {
		return success;
	}

	/**
	 * @return operation status per lfn
	 */
	public HashMap<String, Boolean> getResults() {
		return results;
	}
}
