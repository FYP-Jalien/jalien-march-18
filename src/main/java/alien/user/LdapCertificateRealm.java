package alien.user;

import java.security.Principal;
import java.security.cert.X509Certificate;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.catalina.Wrapper;
import org.apache.catalina.realm.RealmBase;

/**
 * Overrides X509Certificate Authentication to check the users in LDAP<br>
 * See here for more details:
 * <a href="http://monalisa.cern.ch/blog/2007/04/02/enabling-ssl-in-tomcat/">
 * http://monalisa.cern.ch/blog/2007/04/02/enabling-ssl-in-tomcat/</a><br>
 * <br>
 * <br>
 * Usage: <i>tomcat/conf/server.xml</i><br>
 * <code>
 * &lt;Server&gt;...<br>
 * &lt;Service&gt;<br>
 *     &lt;Connector port=&quot;8889&quot;<br>
            redirectPort=&quot;8443&quot;<br>
            ...
            <br>/&gt;<br>
	   <br>
 *     &lt;Connector port=&quot;8443&quot; scheme=&quot;https&quot; secure=&quot;true&quot;<br>
        protocol=&quot;org.apache.coyote.http11.Http11Protocol&quot;<br>
        SSLEnabled=&quot;true&quot;<br>
        maxThreads=&quot;50&quot; minSpareThreads=&quot;2&quot; maxSpareThreads=&quot;5&quot;<br>
        enableLookups=&quot;false&quot; disableUploadTimeout=&quot;true&quot;<br>
        acceptCount=&quot;100&quot;<br>
        clientAuth=&quot;true&quot;<br>
        sslProtocol=&quot;TLS&quot;<br>
        keystoreFile=&quot;/path/to/keystore.jks&quot;<br>
        keystorePass=&quot;keypass&quot;<br>
        keystoreType=&quot;JKS&quot;<br>
        truststoreFile=&quot;/path/to/truststore.jks&quot;<br>
        truststorePass=&quot;trustpass&quot;<br>
        truststoreType=&quot;JKS&quot;<br>
        allowLinking=&quot;true&quot;<br>
        compression=&quot;on&quot;<br>
        compressionMinSize=&quot;2048&quot;<br>
        compressableMimeType=&quot;text/html,text/xml,text/plain&quot;<br>
    /&gt;<br>
 * 
 * &lt;Engine&gt;...<br>
 * &lt;Realm className=&quot;alien.user.LdapCertificateRealm&quot; debug=&quot;0&quot;/&gt;<br>
 * &lt;/Engine&gt;<br>
 * &lt;/Service&gt;<br>
 * &lt;/Server&gt;<br>
 * </code> <br>
 * Then for the resource that you want to force authentication, in web.xml:<br>
 * <code>
 *   &lt;security-constraint&gt;<br>
    &lt;web-resource-collection&gt;<br>
      &lt;web-resource-name&gt;Certificate required&lt;/web-resource-name&gt;<br>
      &lt;url-pattern&gt;/users/*&lt;/url-pattern&gt;<br>
      &lt;http-method&gt;GET&lt;/http-method&gt;<br>
      &lt;http-method&gt;POST&lt;/http-method&gt;<br>
    &lt;/web-resource-collection&gt;<br>
<br>
    &lt;auth-constraint&gt;<br>
      &lt;role-name&gt;users&lt;/role-name&gt;<br>
    &lt;/auth-constraint&gt;<br>
<br>
    &lt;user-data-constraint&gt;<br>
      &lt;transport-guarantee&gt;CONFIDENTIAL&lt;/transport-guarantee&gt;<br>
    &lt;/user-data-constraint&gt;<br>
  &lt;/security-constraint&gt;<br>
</code>
 * 
 * @author Alina Grigoras
 * @since 02-04-2007
 */
public class LdapCertificateRealm extends RealmBase {
	private static final Logger logger = Logger.getLogger(LdapCertificateRealm.class.getCanonicalName());

	/**
	 * @param certChain
	 *            Certificate chain
	 * @return AlicePrincipal which contains the LDAP username that has the
	 *         given certificate associated
	 */
	@Override
	public Principal authenticate(final X509Certificate[] certChain) {
		return UserFactory.getByCertificate(certChain);
	}

	/**
	 * @param principal
	 *            - the principal which will be checked for the permissions
	 * @param role
	 *            - the role
	 * @return true/false if the user is in role
	 */
	@Override
	public boolean hasRole(final Wrapper wrapper, final Principal principal, final String role) {
		if (principal == null)
			return false;

		Set<String> sUsernames = null;

		if (principal instanceof AliEnPrincipal)
			sUsernames = ((AliEnPrincipal) principal).getNames();
		else {
			sUsernames = new HashSet<>(1);
			sUsernames.add(principal.getName());
		}

		if (logger.isLoggable(Level.FINE))
			logger.fine("hasRole('" + sUsernames + "', '" + role + "')");

		if (sUsernames == null || sUsernames.size() == 0)
			return false;

		if ("users".equals(role))
			return true;

		for (final String sUsername : sUsernames) {
			final Set<String> sRoles = LDAPHelper.checkLdapInformation("users=" + sUsername, "ou=Roles,", "uid");

			if (logger.isLoggable(Level.FINER))
				logger.finer("Roles for '" + sUsername + "' : " + sRoles);

			if (sRoles.contains(role)) {
				if (logger.isLoggable(Level.FINER))
					logger.finer("Returning true because this username has the desired role");

				return true;
			}
		}

		if (logger.isLoggable(Level.FINER))
			logger.finer("Returning false because no username had the desired role");

		return false;
	}

	@Override
	protected String getPassword(final String arg0) {
		return null;
	}

	@Override
	protected Principal getPrincipal(final String arg0) {
		return null;
	}

}
