package alien.api.catalogue;

import alien.api.Request;
import alien.catalogue.GUIDUtils;
import alien.user.AliEnPrincipal;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class UpdateGUIDMD5 extends Request {
	private UUID uuid;
	private String md5;
	private boolean updateSuccessful;

	public UpdateGUIDMD5(final AliEnPrincipal user, final UUID uuid, String md5) {
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

	public boolean isUpdateSuccessful() {
		return updateSuccessful;
	}
}
