package alien.api.catalogue;

import java.util.Arrays;
import java.util.List;

import alien.api.Request;
import alien.catalogue.LFNUtils;
import alien.user.AliEnPrincipal;

/**
 * @author ibrinzoi
 * @since 2021-12-08
 */
public class MoveDirectory extends Request {
	private static final long serialVersionUID = 1950963890853076564L;

	private final String path;
	private String response;

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
		this.response = LFNUtils.moveDirectory(getEffectiveRequester(), path);
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(this.path);
	}

	/**
	 * @return the message returned by the moveDirectory operation on the server side
	 */
	public String getResponse() {
		return this.response;
	}

}