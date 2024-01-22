package alien.user;

import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.api.Dispatcher;
import alien.api.LDAPInfoRequest;
import alien.api.LDAPTreeRequest;
import alien.api.ServerException;

/***
 * operations with LDAP informations, via the central services
 *
 * @author costing
 * @since Jan 9 2024
 */
public class LDAPHelperRemote {
	private static final Logger logger = Logger.getLogger(LDAPHelperRemote.class.getCanonicalName());

	/**
	 * @param sParam
	 *            - search query
	 * @param sRootExt
	 *            - subpath
	 * @param sKey
	 *            - key to extract
	 * @return Set of result from the query
	 */
	public static final Set<String> checkLdapInformation(final String sParam, final String sRootExt, final String sKey) {
		try {
			return Dispatcher.execute(new LDAPInfoRequest(sParam, sRootExt, sKey)).getValues();
		}
		catch (ServerException e) {
			logger.log(Level.SEVERE, "Cannot query LDAP info via CS", e);
		}

		return null;
	}

	/**
	 * @param sParam
	 *            - search query
	 * @param sRootExt
	 *            - subpath
	 * @param sKey
	 *            - key to extract
	 * @param recursive
	 * @return Set of result from the query
	 */
	public static final Set<String> checkLdapInformation(final String sParam, final String sRootExt, final String sKey, final boolean recursive) {
		try {
			return Dispatcher.execute(new LDAPInfoRequest(sParam, sRootExt, sKey, recursive)).getValues();
		}
		catch (ServerException e) {
			logger.log(Level.SEVERE, "Cannot query LDAP info via CS", e);
		}

		return null;
	}

	/**
	 * @param sParam
	 *            - search query
	 * @param sRootExt
	 *            - subpath
	 * @return Map of result from the query, keys are fields-values from the LDAP tree
	 */
	public static final Map<String, Object> checkLdapTree(final String sParam, final String sRootExt) {
		try {
			return Dispatcher.execute(new LDAPTreeRequest(sParam, sRootExt)).getValues();
		}
		catch (ServerException e) {
			logger.log(Level.SEVERE, "Cannot query LDAP tree via CS", e);
		}

		return null;
	}

	/**
	 * @param sParam
	 *            - search query
	 * @param sRootExt
	 *            - subpath
	 * @param prependKey
	 * @return Map of result from the query, keys are fields-values from the LDAP tree
	 */
	public static final Map<String, Object> checkLdapTree(final String sParam, final String sRootExt, final String prependKey) {
		try {
			return Dispatcher.execute(new LDAPTreeRequest(sParam, sRootExt, prependKey)).getValues();
		}
		catch (ServerException e) {
			logger.log(Level.SEVERE, "Cannot query LDAP tree via CS", e);
		}

		return null;
	}

	/**
	 * @param account
	 * @return the set of emails associated to the given account
	 */
	public static Set<String> getEmails(final String account) {
		if (account == null || account.length() == 0)
			return null;

		return LDAPHelperRemote.checkLdapInformation("uid=" + account, "ou=People,", "email");
	}

	/**
	 * Debug method
	 *
	 * @param args
	 */
	public static void main(final String[] args) {
		System.out.println(checkLdapInformation("uid=gconesab", "ou=People,", "email"));

		System.out.println(checkLdapInformation("subject=/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=agrigora/CN=659434/CN=Alina Gabriela Grigoras", "ou=People,", "uid"));

		System.out.println(" 1 " + checkLdapInformation("users=peters", "ou=Roles,", "uid"));

		try {
			Thread.sleep(1000);
		}
		catch (@SuppressWarnings("unused") final Exception e) { /* nothing */
		}

		System.out.println(" 2 " + checkLdapInformation("users=peters", "ou=Roles,", "uid"));

		try {
			Thread.sleep(1000);
		}
		catch (@SuppressWarnings("unused") final Exception e) { /* nothing */
		}

		System.out.println(" 3 " + checkLdapInformation("users=peters", "ou=Roles,", "uid"));

		try {
			Thread.sleep(1000);
		}
		catch (@SuppressWarnings("unused") final Exception e) { /* nothing */
		}

		System.out.println(" 4 " + checkLdapInformation("users=peters", "ou=Roles,", "uid"));
	}

	/**
	 * @param domain
	 * @return the tree for this domain
	 */
	public static Map<String, Object> getInfoDomain(final String domain) {
		// Get the root site config based on domain
		return LDAPHelperRemote.checkLdapTree("(&(domain=" + domain + ")(objectClass=AliEnSite))", "ou=Sites,");
	}

	/**
	 * @return the tree for the VO
	 */
	public static Map<String, Object> getVOConfig() {
		return LDAPHelperRemote.checkLdapTree("(&(objectClass=AliEnVOConfig))", "ou=Config,");
	}
}
