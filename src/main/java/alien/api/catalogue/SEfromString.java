package alien.api.catalogue;

import java.util.Arrays;
import java.util.List;

import alien.api.Cacheable;
import alien.api.Request;
import alien.se.SE;
import alien.se.SEUtils;
import alien.user.AliEnPrincipal;

/**
 *
 * @author ron
 * @since Jun 03, 2011
 */
public class SEfromString extends Request implements Cacheable {

	private static final long serialVersionUID = 8631851052133487066L;
	private final String sSE;
	private final int seNo;

	private SE se;

	/**
	 * Get SE by name
	 *
	 * @param user
	 * @param se
	 */
	public SEfromString(final AliEnPrincipal user, final String se) {
		setRequestUser(user);
		sSE = se;
		seNo = 0;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(sSE != null ? sSE : String.valueOf(seNo));
	}

	/**
	 * Get SE by number
	 *
	 * @param user
	 * @param seno
	 */
	public SEfromString(final AliEnPrincipal user, final int seno) {
		setRequestUser(user);
		this.seNo = seno;
		sSE = null;
	}

	@Override
	public void run() {
		if (sSE != null)
			this.se = SEUtils.getSE(sSE);
		else
			this.se = SEUtils.getSE(seNo);
	}

	/**
	 * @return the requested SE
	 */
	public SE getSE() {
		return this.se;
	}

	@Override
	public String toString() {
		if (sSE != null)
			return "Asked for : " + this.sSE + ", reply is:\n" + this.se;

		return "Asked for No: " + this.seNo + ", reply is:\n" + this.se;
	}

	@Override
	public String getKey() {
		return this.sSE + "#" + this.seNo;
	}

	@Override
	public long getTimeout() {
		return 1000 * 60 * 60;
	}
}
