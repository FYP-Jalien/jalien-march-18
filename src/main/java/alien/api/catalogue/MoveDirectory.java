package alien.api.catalogue;

import java.util.ArrayList;
import java.util.List;

import alien.api.Request;
import alien.catalogue.LFNUtils;
import alien.user.AliEnPrincipal;
import utils.ExpireTime;

/**
 * @author ibrinzoi
 * @since 2021-12-08
 */
public class MoveDirectory extends Request {
	private static final long serialVersionUID = 1950963890853076564L;

	private final String path;

	/**
	 * @param user
	 * @param path
	 */
	public MoveDirectory(final AliEnPrincipal user, final String path) {
		setRequestUser(user);
		this.path = path;
	}

	@Override
	public void run() {
		LFNUtils.moveDirectory(getEffectiveRequester(), path);
	}

	@Override
	public List<String> getArguments() {
		List<String> pathList = new ArrayList<>();
		pathList.add(this.path);

		return pathList;
	}

}