package alien.api.catalogue;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import alien.api.Request;
import alien.catalogue.GUIDUtils;
import alien.user.AliEnPrincipal;

/**
 * @author Adrian-Eduard Negru
 * @since Apr 21, 2021
 */
public class UpdateGUIDMD5 extends Request {
	private static final long serialVersionUID = 8268047243960520437L;

	// outgoing fields
	private final UUID uuid;
	private final String md5;

	// answer
	private boolean updateSuccessful;

	/**
	 * @param user
	 * @param uuid guid to modify
	 * @param md5 new MD5 value
	 */
	public UpdateGUIDMD5(final AliEnPrincipal user, final UUID uuid, final String md5) {
		setRequestUser(user);
		this.uuid = uuid;
		this.md5 = md5;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(uuid.toString(), md5);
	}

	@Override
	public void run() {
		this.updateSuccessful = GUIDUtils.updateMd5(uuid, md5);
	}

	/**
	 * @return <code>true</code> if the GUID was modified
	 */
	public boolean isUpdateSuccessful() {
		return updateSuccessful;
	}
}
