package alien.api.catalogue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import alien.api.Request;
import alien.api.ServerException;
import alien.catalogue.CatalogEntity;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFNUtils;
import alien.user.AliEnPrincipal;
import alien.user.AuthorizationChecker;

/**
 * Mirror an LFN
 */
public class MirrorLFN extends Request {
	private static final long serialVersionUID = -3323349253430911576L;
	private final String path;
	private int success;
	private final List<String> ses;
	private final List<String> exses;
	private final HashMap<String, Integer> qos;
	private final boolean useGUID;
	private final Integer attempts;
	private final String removeReplica;
	private HashMap<String, Long> results;

	/**
	 * @param user
	 * @param lfn_name
	 * @param ses
	 * @param exses
	 * @param qos
	 * @param useLFNasGuid
	 * @param attempts_cnt
	 * @param removeReplica SE to remove after a successful transfer to the destination SE(s)
	 * @throws ServerException
	 */
	public MirrorLFN(final AliEnPrincipal user, final String lfn_name, final List<String> ses, final List<String> exses, final HashMap<String, Integer> qos, final boolean useLFNasGuid,
			final Integer attempts_cnt, final String removeReplica) throws ServerException {
		setRequestUser(user);
		this.path = lfn_name;
		this.useGUID = useLFNasGuid;
		this.attempts = attempts_cnt;
		this.ses = ses;
		this.exses = exses;
		this.qos = qos;
		this.removeReplica = removeReplica;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(this.path, String.valueOf(this.useGUID), String.valueOf(this.attempts), this.ses != null ? this.ses.toString() : null, this.exses != null ? this.exses.toString() : null,
				this.qos != null ? this.qos.toString() : null, removeReplica);
	}

	@Override
	public void run() {
		this.results = new HashMap<>();
		final CatalogEntity c = (this.useGUID ? GUIDUtils.getGUID(UUID.fromString(this.path), false) : LFNUtils.getLFN(this.path));
		if (!AuthorizationChecker.isOwner(c, getEffectiveRequester()))
			throw new SecurityException("You do not own this file: " + c + ", requester: " + getEffectiveRequester());

		this.results = LFNUtils.mirrorLFN(this.path, this.ses, this.exses, this.qos, this.useGUID, this.attempts, this.removeReplica);

		this.success = this.results != null ? this.results.size() : -1;
	}

	/**
	 * @return <code>true</code> if at least one operation was successful
	 */
	public boolean getSuccess() {
		return this.success >= 0;
	}

	/**
	 * @return exit code
	 */
	public int getResult() {
		return this.success;
	}

	/**
	 * @return results
	 */
	public HashMap<String, Long> getResultHashMap() {
		return this.results;
	}
}
