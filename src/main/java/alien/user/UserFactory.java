package alien.user;

import java.security.cert.X509Certificate;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.ServletRequest;

import alien.config.ConfigUtils;
import lazyj.commands.CommandOutput;
import lazyj.commands.SystemCommand;

/**
 * @author costing
 * @since 2010-11-11
 */
public final class UserFactory {
	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(UserFactory.class.getCanonicalName());

	private UserFactory() {
		// factory
	}

	/**
	 * Get the account for the given username
	 *
	 * @param username
	 * @return the account, or <code>null</code> if it is not a valid username
	 */
	public static AliEnPrincipal getByUsername(final String username) {
		if (username == null)
			return null;

		if (username.equals("admin"))
			return new AliEnPrincipal(username);

		final Set<String> check = LDAPHelper.checkLdapInformation("uid=" + username, "ou=People,", "uid");

		AliEnPrincipal ret = null;

		if (check != null && check.size() >= 1) {
			ret = new AliEnPrincipal(username);

			if (username.equals("jobagent"))
				ret.setJobAgent();
		}

		return ret;
	}

	/**
	 * Get the account for the given role
	 *
	 * @param role
	 * @return the account, or <code>null</code> if it is not a valid role
	 */
	public static AliEnPrincipal getByRole(final String role) {
		if (logger.isLoggable(Level.FINER))
			logger.log(Level.FINER, "Checking for role " + role);
		if (role == null)
			return null;

		if (role.equals("admin"))
			return new AliEnPrincipal(role);

		final Set<String> check = LDAPHelper.checkLdapInformation("uid=" + role, "ou=Roles,", "uid");

		if (check != null && check.size() >= 1)
			return new AliEnPrincipal(role);

		return null;
	}

	/**
	 * Get the account corresponding to this certificate chain
	 *
	 * @param certChain
	 * @return account, or <code>null</code> if no account has this certificate
	 *         associated to it
	 */
	public static AliEnPrincipal getByCertificate(final X509Certificate[] certChain) {
		for (int i = 0; i < certChain.length; i++) {
			final String sDN = certChain[i].getSubjectX500Principal().getName();
			final String sDNTransformed = transformDN(sDN);

			if (logger.isLoggable(Level.FINER))
				logger.log(Level.FINER, "Checking for chain " + i + ": " + sDNTransformed);

			final AliEnPrincipal p = getByDN(sDNTransformed);

			if (p != null) {
				if (logger.isLoggable(Level.FINER))
					logger.log(Level.FINER, "Account for " + i + " (" + sDNTransformed + ") is: " + p);

				p.setUserCert(certChain);
				return p;
			}

			final int idx = sDNTransformed.lastIndexOf('=');

			if (idx < 0 || idx == sDNTransformed.length() - 1)
				return null;

			try {
				Long.parseLong(sDNTransformed.substring(idx + 1));
			}
			catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
				// try the next certificate in chain only if the last item is a
				// number, so it might be a proxy
				// certificate in fact
				return null;
			}
		}

		return null;
	}

	/**
	 * Try to extract the Principal from the HTTPS request
	 *
	 * @param request
	 * @return account for a given request
	 */
	public static AliEnPrincipal get(final ServletRequest request) {
		if (request.isSecure()) {
			final X509Certificate cert[] = (X509Certificate[]) request.getAttribute("javax.servlet.request.X509Certificate");

			if (cert != null && cert.length > 0)
				return getByCertificate(cert);
		}

		return null;
	}

	/**
	 * Get the account corresponding to this certificate DN
	 *
	 * @param dn
	 * @return account, or <code>null</code> if no account has this certificate
	 *         associated to it
	 */
	public static AliEnPrincipal getByDN(final String dn) {
		final Set<AliEnPrincipal> allPrincipal = getAllByDN(dn);

		if (allPrincipal != null && allPrincipal.size() > 0)
			return allPrincipal.iterator().next();

		return null;
	}

	/**
	 * Transform a DN from the comma notation to slash notation, in reverse order.
	 * Example:
	 *
	 * Input: CN=Alina Gabriela Grigoras,CN=659434,CN=agrigora,OU=Users,OU=Organic
	 * Units,DC=cern,DC=ch Output: /DC=ch/DC=cern/OU=Organic
	 * Units/OU=Users/CN=agrigora/CN=659434/CN=Alina Gabriela Grigoras
	 *
	 * @param subject
	 * @return AliEn-style subject
	 */
	public static String transformDN(final String subject) {
		final StringTokenizer st = new StringTokenizer(subject, ",");
		String sNewDn = "";

		while (st.hasMoreTokens()) {
			final String sToken = st.nextToken();

			sNewDn = sToken.trim() + (sNewDn.length() == 0 ? "" : "/") + sNewDn;
		}

		if (!sNewDn.startsWith("/"))
			sNewDn = "/" + sNewDn;

		return sNewDn;
	}

	/**
	 * Get all accounts to which this certificate subject is associated
	 *
	 * @param dn
	 *            subject, in slash notation
	 * @return all accounts, or <code>null</code> if none matches in fact
	 * @see #transformDN(String)
	 */
	public static Set<AliEnPrincipal> getAllByDN(final String dn) {
		if (logger.isLoggable(Level.FINER))
			logger.log(Level.FINER, "Checking for chain: " + dn);
		// If it is a token cert
		if (dn.startsWith("/C=ch/O=AliEn")) {
			AliEnPrincipal p = null;

			if (dn.contains("/CN=JobAgent")) {
				p = getByUsername("jobagent");
			}
			else if (dn.contains("/CN=Job")) {
				// Assuming we have user or job token, parse role to switch identity to that
				// user
				// /C=ch/O=AliEn/CN=Job/CN=username/OU=role/OU=extension

				final int roleOU = dn.indexOf("/OU=") != -1 ? dn.indexOf("/OU=") : dn.length();
				final int jobOU = dn.lastIndexOf("/OU=") != roleOU ? dn.lastIndexOf("/OU=") : dn.length();
				final int nameCN = dn.lastIndexOf("/CN=");

				if (roleOU != dn.length()) // if OU present in DN try to extract role
					p = getByRole(dn.substring(roleOU + 4, jobOU));

				if (nameCN != -1) { // if CN present in DN
					if (p == null) // if getByRole didn't find anything
						p = getByUsername(dn.substring(nameCN + 4, roleOU));

					if (p != null) { // if getByUsername or getByRole found credentials
						if (jobOU != dn.length()) { // if second OU is present in DN
							final String extensions = dn.substring(jobOU + 4);
							final StringTokenizer st = new StringTokenizer(extensions, "/");
							while (st.hasMoreElements()) {
								final String spec = st.nextToken();

								final int idx = spec.indexOf('=');

								if (idx > 0)
									p.setExtension(spec.substring(0, spec.charAt(idx - 1) == '\\' ? idx - 1 : idx), spec.substring(idx + 1));
							}
						}

						p.setDefaultUser(dn.substring(nameCN + 4, roleOU));
					}
				}
			}
			else if (dn.contains("/CN=Users")) {
				// /C=ch/O=AliEn/CN=Users/CN=username/OU=role

				final int roleOU = dn.indexOf("/OU=") != -1 ? dn.indexOf("/OU=") : dn.length();
				final int nameCN = dn.lastIndexOf("/CN=");

				if (roleOU != dn.length()) // if OU present in DN try to extract role
					p = getByRole(dn.substring(roleOU + 4));

				if (nameCN != -1) { // if CN present in DN
					if (p == null) // if getByRole didn't find anything
						p = getByUsername(dn.substring(nameCN + 4, roleOU));

					if (p != null) // if getByUsername or getByRole found credentials
						p.setDefaultUser(dn.substring(nameCN + 4, roleOU));
				}
			}

			if (p != null) {
				final Set<AliEnPrincipal> ret = new LinkedHashSet<>();
				ret.add(p);
				return ret;
			}
		}

		final Set<String> check = LDAPHelper.checkLdapInformation("subject=" + dn, "ou=People,", "uid");

		if (check != null && check.size() > 0) {
			final Set<AliEnPrincipal> ret = new LinkedHashSet<>();

			for (final String username : check) {
				final AliEnPrincipal p = new AliEnPrincipal(username);

				p.setNames(check);

				ret.add(p);
			}

			return ret;
		}

		return null;
	}

	private static boolean isUserKeyOk(final String userString) {
		return userString != null && userString.length() > 0 && !userString.equals("?");
	}

	/**
	 * @return current user's ID, if it can be retrieved from the system
	 */
	public static String getUserID() {
		String sUserId = System.getProperty("userid");

		if (isUserKeyOk(sUserId)) {
			return sUserId;
		}

		final CommandOutput idOutput = SystemCommand.bash("id -u");

		if (idOutput != null)
			sUserId = idOutput.stdout;

		if (isUserKeyOk(sUserId)) {
			System.setProperty("userid", sUserId);
			return sUserId;
		}

		return null;
	}

	/**
	 * @return current user's account name
	 */
	public static String getUserName() {
		String sUserName = System.getProperty("jalien.username");

		if (isUserKeyOk(sUserName)) {
			return sUserName;
		}

		sUserName = System.getProperty("user.name");

		if (!isUserKeyOk(sUserName))
			sUserName = SystemCommand.bash("id -u -n").stdout;

		if (!isUserKeyOk(sUserName))
			sUserName = System.getenv("USER");

		if (isUserKeyOk(sUserName)) {
			System.setProperty("jalien.username", sUserName);
			return sUserName;
		}

		return null;
	}

	/**
	 * @return user home directory, if it can be inferred from the environment
	 */
	public static String getUserHome() {
		String sUserHome = System.getProperty("jalien.userhome");

		if (isUserKeyOk(sUserHome)) {
			return sUserHome;
		}

		sUserHome = System.getProperty("user.home");

		if (!isUserKeyOk(sUserHome))
			sUserHome = System.getenv("HOME");

		if (isUserKeyOk(sUserHome)) {
			System.setProperty("jalien.userhome", sUserHome);
			return sUserHome;
		}

		return null;
	}
}
