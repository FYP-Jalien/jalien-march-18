package alien.api;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import alien.user.LDAPHelper;

/**
 *
 * @author costing
 * @since Jan 9, 2024
 */
public class LDAPInfoRequest extends Request implements Cacheable {

	private static final long serialVersionUID = 4715023480701764443L;
	
	private final String sParam;
	private final String sRootExt;
	private final String sKey;
	private boolean recursive;

	private Set<String> ret;

	/**
	 * Get some LDAP information via the central services
	 * 
	 * @param sParam
	 *            - search query
	 * @param sRootExt
	 *            - subpath
	 * @param sKey
	 *            - key to extract
	 * @see LDAPHelper#checkLdapInformation(String, String, String)
	 */
	public LDAPInfoRequest(final String sParam, final String sRootExt, final String sKey) {
		this(sParam, sRootExt, sKey, true);
	}

	/**
	 * Get some LDAP information via the central services
	 * 
	 * @param sParam
	 *            - search query
	 * @param sRootExt
	 *            - subpath
	 * @param sKey
	 *            - key to extract
	 * @param recursive
	 * @see LDAPHelper#checkLdapInformation(String, String, String, boolean)
	 */
	public LDAPInfoRequest(final String sParam, final String sRootExt, final String sKey, final boolean recursive) {
		this.sParam = sParam;
		this.sRootExt = sRootExt;
		this.sKey = sKey;
		this.recursive = recursive;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(sParam, sRootExt, sKey, String.valueOf(recursive));
	}

	@Override
	public void run() {
		ret = LDAPHelper.checkLdapInformation(sParam, sRootExt, sKey, recursive);
	}

	/**
	 * @return the LDAP information
	 */
	public Set<String> getValues() {
		return ret;
	}

	@Override
	public String toString() {
		return "Asked for LDAP info of `" + sParam + "`, `" + sRootExt + "`, `" + sKey + "`, `" + recursive + "`";
	}

	@Override
	public String getKey() {
		return sParam + "#" + sRootExt + "#" + sKey + "#" + recursive;
	}

	@Override
	public long getTimeout() {
		return 1000 * 60 * 15;
	}
}
