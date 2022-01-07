package alien.user;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.Principal;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import lazyj.StringFactory;

/**
 * Implements java.security.Principal
 *
 * @author Alina Grigoras
 * @since 02-04-2007
 */
public class AliEnPrincipal implements Principal, Serializable {
	private final static String userRole = "users";

	private final static List<String> admins = Arrays.asList("admin");

	/**
	 *
	 */
	private static final long serialVersionUID = -5393260803758989309L;

	private final String username;
	private String defaultUser;
	private X509Certificate[] usercert;

	/**
	 * Set of account names from LDAP that have the given DN
	 */
	private Set<String> sUsernames = null;

	/**
	 * Cache the roles
	 */
	private Set<String> roles = null;

	private String defaultRole = null;

	/**
	 * When were the roles generated
	 */
	private long lRolesChecked = 0;

	private transient InetAddress remoteEndpoint = null;

	private transient int remotePort = 0;

	/**
	 * building a Principal for ALICE user
	 *
	 * @param username
	 */
	AliEnPrincipal(final String username) {
		this.username = StringFactory.get(username);
		this.defaultUser = StringFactory.get(username);
		this.usercert = null;
	}

	/**
	 * Get one of the accounts that match the given DN (first in the set that we got
	 * from LDAP).
	 *
	 * @return one account name from LDAP that has the DN
	 */
	@Override
	public String getName() {
		return username;
	}

	/**
	 * Get user, who initialized connection
	 *
	 * @return initial user
	 */
	public String getDefaultUser() {
		return defaultUser;
	}

	/**
	 * Retrieve user certificate
	 *
	 * @return user certificate
	 */
	public X509Certificate[] getUserCert() {
		return usercert;
	}

	/**
	 * Store user certificate in principal
	 *
	 * @param cert
	 */
	public void setUserCert(final X509Certificate[] cert) {
		usercert = cert;
	}

	/**
	 * If known, all usernames associated to a DN
	 *
	 * @param names
	 */
	void setNames(final Set<String> names) {
		sUsernames = new LinkedHashSet<>(names);

		if (!sUsernames.contains(username))
			sUsernames.add(StringFactory.get(username));
	}

	/**
	 * Set the initial user name
	 *
	 * @param name
	 */
	public void setDefaultUser(final String name) {
		defaultUser = name;
	}

	/**
	 * Get all the accounts that match the given DN
	 *
	 * @return set of account names that have the DN
	 */
	public Set<String> getNames() {
		if (sUsernames == null) {
			sUsernames = new LinkedHashSet<>();
			sUsernames.add(username);
		}

		return sUsernames;
	}

	@Override
	public String toString() {
		return getNames().toString();
	}

	/**
	 * Check if two principals are the same user
	 *
	 * @param user
	 *            to compare
	 * @return outcome of equals
	 */
	@Override
	public boolean equals(final Object user) {
		if (user == null)
			return false;

		if (this == user)
			return true;

		if (!(user instanceof AliEnPrincipal))
			return false;

		return getName().equals(((AliEnPrincipal) user).getName());
	}

	@Override
	public int hashCode() {
		return getName().hashCode();
	}

	/**
	 * Get all the roles associated with this principal
	 *
	 * @return all roles defined in LDAP
	 */
	public Set<String> getRoles() {
		if (roles != null && (System.currentTimeMillis() - lRolesChecked) < 1000 * 60 * 5)
			return roles;

		final Set<String> ret = new LinkedHashSet<>();

		for (final String sUsername : getNames()) {
			ret.add(sUsername);

			final Set<String> sRoles = LDAPHelper.checkLdapInformation("users=" + sUsername, "ou=Roles,", "uid");

			if (sRoles != null)
				for (final String s : sRoles)
					ret.add(StringFactory.get(s));
		}

		roles = ret;
		lRolesChecked = System.currentTimeMillis();

		return roles;
	}

	/**
	 * Get default user role, normally the same name as the account
	 *
	 * @return the name of the default role of this account
	 */
	public String getDefaultRole() {
		if (defaultRole != null)
			return defaultRole;

		final Set<String> allroles = getRoles();

		final String name = getName();

		if (allroles.size() == 0 || allroles.contains(getName()))
			defaultRole = name;
		else
			defaultRole = allroles.iterator().next();

		return defaultRole;
	}

	/**
	 * Check if this principal has the given role
	 *
	 * @param role
	 *            role to check
	 * @return <code>true</code> if the user belongs to this group
	 */
	public boolean hasRole(final String role) {
		if (role == null || role.length() == 0)
			return false;

		if (userRole.equals(role) || getRoles().contains(role))
			return true;

		return false;
	}

	/**
	 * Set the default role for this user. It can only be one of the roles declared
	 * in LDAP (or itself, or "users"). The method will silently refuse to set an
	 * invalid role and will keep the previous value.
	 *
	 * @param newRole
	 * @return the previous default role
	 */
	public String setRole(final String newRole) {
		final String oldRole = getDefaultRole();

		if (canBecome(newRole))
			defaultRole = newRole;

		return oldRole;
	}

	private boolean jobAgentFlag = false;

	/**
	 * Set the job agent flag when the identity is detected to be of one
	 */
	void setJobAgent() {
		jobAgentFlag = true;
	}

	/**
	 * Check if this is a JobAgent token certificate
	 *
	 * @return <code>true</code> if the DN indicates that this is a JobAgent
	 *         identity
	 */
	public boolean isJobAgent() {
		return jobAgentFlag;
	}

	private HashMap<String, String> extensions;

	/**
	 * Set extension information for job
	 *
	 * @param key
	 *            extension tag
	 * @param value
	 *            string value of key
	 */
	public void setExtension(final String key, final String value) {
		if (extensions == null)
			extensions = new HashMap<>();

		extensions.put(key, value);
	}

	/**
	 * Retrieve information about job extensions
	 *
	 * @param key
	 *            extension tag
	 * @return string value of key
	 */
	public String getExtension(final String key) {
		if (extensions != null)
			if (extensions.containsKey(key))
				return extensions.get(key);

		return null;
	}

	/**
	 * @return <code>true</code> if the DN indicates that this is a regular job.
	 */
	public boolean isJob() {
		if (extensions != null)
			return extensions.containsKey("queueid");

		return false;
	}

	/**
	 * @return the job ID, extracted from the certificate subject
	 */
	public Long getJobID() {
		if (extensions == null)
			return null;

		final String queueId = extensions.get("queueid");

		if (queueId == null)
			return null;

		return Long.valueOf(queueId);
	}

	/**
	 * @return the resubmission count, extracted from the certificate subject
	 */
	public Integer getResubmissionCount() {
		if (extensions == null)
			return null;

		final String resubmission = extensions.get("resubmission");

		if (resubmission == null)
			return null;

		return Integer.valueOf(resubmission);
	}

	/**
	 * Check if this principal can become the given user/role
	 *
	 * @param role
	 *            the role to verify
	 * @return <code>true</code> if the role is one of this principal's accounts or
	 *         is any of the roles assigned to any of the accounts,
	 *         <code>false</code> otherwise
	 */
	public boolean canBecome(final String role) {
		if (role == null || role.length() == 0)
			return false;

		final Set<String> names = getNames();

		if (names == null || names.size() == 0)
			return false;

		if (names.contains(role) || userRole.equals(role))
			return true;

		final Set<String> sRoles = getRoles();

		if (sRoles == null || sRoles.size() == 0)
			return false;

		if (sRoles.contains(role) || sRoles.contains("admin"))
			return true;

		return false;
	}

	/**
	 * Return the default user role
	 *
	 * @return user role
	 */
	public static String userRole() {
		return userRole;
	}

	/**
	 * Check if that role name authorizes admin privileges
	 *
	 * @param role
	 * @return is admin privileged or not
	 */
	public static boolean roleIsAdmin(final String role) {
		return admins.contains(role);
	}

	/**
	 * @return the endpoint where this guy came from
	 */
	public InetAddress getRemoteEndpoint() {
		return remoteEndpoint;
	}

	/**
	 * @return client's port number on its side of the socket
	 */
	public int getRemotePort() {
		return remotePort;
	}

	/**
	 * Upon accepting a request, set this address to where the connection came from
	 *
	 * @param remoteEndpoint
	 */
	public void setRemoteEndpoint(final InetAddress remoteEndpoint) {
		if (this.remoteEndpoint == null)
			this.remoteEndpoint = remoteEndpoint;
		else
			throw new IllegalAccessError("You are not allowed to overwrite this field!");
	}

	/**
	 * Upon accepting a request, set this port number to client's remote port number
	 *
	 * @param remotePort
	 */
	public void setRemotePort(final int remotePort) {
		if (this.remotePort <= 0)
			this.remotePort = remotePort;
		else
			throw new IllegalAccessError("You are not allowed to overwrite this field!");
	}

	/**
	 * Set complete client details from the socket address
	 *
	 * @param socketAddress
	 */
	public void setRemoteEndpoint(final InetSocketAddress socketAddress) {
		setRemoteEndpoint(socketAddress.getAddress());
		setRemotePort(socketAddress.getPort());
	}

	/**
	 * Returns user list for a role
	 *
	 * @param role
	 * @return list of users
	 */
	public static Set<String> getRoleMembers(final String role) {
		if (role == null || role.isBlank())
			return null;

		final Set<String> sUsers = LDAPHelper.checkLdapInformation("uid=" + role, "ou=Roles,", "users");
		return sUsers;
	}
}
