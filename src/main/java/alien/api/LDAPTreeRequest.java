package alien.api;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import alien.user.LDAPHelper;

/**
 *
 * @author costing
 * @since Jan 9, 2024
 */
public class LDAPTreeRequest extends Request implements Cacheable {

	private static final long serialVersionUID = -823796944415605996L;
	private final String sParam;
	private final String sRootExt;
	private final String prependKey;

	private Map<String, Object> ret;

	/**
	 * Get some LDAP information via the central services
	 * 
	 * @param sParam
	 *            - search query
	 * @param sRootExt
	 *            - subpath
	 * @see LDAPHelper#checkLdapTree(String, String)
	 */
	public LDAPTreeRequest(final String sParam, final String sRootExt) {
		this(sParam, sRootExt, null);
	}

	/**
	 * Get some LDAP information via the central services
	 * 
	 * @param sParam
	 *            - search query
	 * @param sRootExt
	 *            - subpath
	 * @param prependKey
	 *            - key to extract
	 * @see LDAPHelper#checkLdapTree(String, String, String)
	 */
	public LDAPTreeRequest(final String sParam, final String sRootExt, final String prependKey) {
		this.sParam = sParam;
		this.sRootExt = sRootExt;
		this.prependKey = prependKey;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(sParam, sRootExt, prependKey);
	}

	@Override
	public void run() {
		ret = LDAPHelper.checkLdapTree(sParam, sRootExt, prependKey);
	}

	/**
	 * @return the LDAP information
	 */
	public Map<String, Object> getValues() {
		return ret;
	}

	@Override
	public String toString() {
		return "Asked for LDAP info of `" + sParam + "`, `" + sRootExt + "`, `" + prependKey + "`";
	}

	@Override
	public String getKey() {
		return sParam + "#" + sRootExt + "#" + prependKey;
	}

	@Override
	public long getTimeout() {
		return 1000 * 60 * 15;
	}
}
