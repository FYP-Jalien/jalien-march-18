package utils.crawler;

import java.util.LinkedHashSet;
import java.util.Set;

import alien.user.LDAPHelperRemote;

/**
 * @author costing
 * @since Apr 15, 2020
 */
public class GetCEs {

	/**
	 * Debug method
	 *
	 * @param args
	 */
	public static void main(final String[] args) {

		for (final String se : new String[] { " ALICE::UPB::EOS" }) {
			final Set<String> siteNames = getCloseSites(se);
			System.err.println(siteNames);

			final Set<String> matchingCEs = new LinkedHashSet<>();

			for (final String siteName : siteNames)
				for (final String ce : getCEs(siteName))
					matchingCEs.add(siteName + "::" + ce);

			final StringBuilder requirements = new StringBuilder();

			System.err.println("GetCE " + matchingCEs);

			for (final String ce : matchingCEs) {
				if (requirements.length() > 0)
					requirements.append(" || ");

				requirements.append("(other.CE==\"ALICE::").append(ce).append("\")");
			}

			System.err.println(se + " : " + requirements);
		}
	}

	/**
	 * @param storageName
	 * @return a list of sites extracted from LDAP that are closest to a storage element
	 */
	private static Set<String> getCloseSites(final String storageName) {
		final Set<String> dns = LDAPHelperRemote.checkLdapInformation("closese=" + storageName, "ou=Sites,", "dn");

		final Set<String> ret = new LinkedHashSet<>();

		for (final String dn : dns) {
			final int idx = dn.lastIndexOf("ou=");

			if (idx >= 0)
				ret.add(dn.substring(idx + 3));
		}

		final int idx = storageName.indexOf("::");
		final int idx2 = storageName.lastIndexOf("::");

		if (idx > 0 && idx2 > idx) {
			final String siteFromStorage = storageName.substring(idx + 2, idx2);

			ret.addAll(LDAPHelperRemote.checkLdapInformation("ou=" + siteFromStorage + "*", "ou=Sites,", "ou", false));
		}

		return ret;
	}

	/**
	 * @param se
	 * @return JDL requirement for closest site to storage element
	 */
	public static StringBuilder getSiteJDLRequirement(final String se) {
		final Set<String> siteNames = getCloseSites(se);

		final Set<String> matchingCEs = new LinkedHashSet<>();

		for (final String siteName : siteNames)
			for (final String ce : getCEs(siteName))
				matchingCEs.add(siteName + "::" + ce);

		final StringBuilder requirements = new StringBuilder();

		for (final String ce : matchingCEs) {
			if (requirements.length() > 0)
				requirements.append(" || ");

			requirements.append("(other.CE==\"ALICE::").append(ce).append("\")");
		}

		requirements.insert(0, "(");
		requirements.append(")");

		return requirements;
	}

	/**
	 * @param siteName
	 * @return a set of computing elements from a specific site, extracted from LDAP
	 */
	private static Set<String> getCEs(final String siteName) {
		return LDAPHelperRemote.checkLdapInformation("(objectClass=AliEnCE)", "ou=CE,ou=Services,ou=" + siteName + ",ou=Sites,", "name");
	}
}