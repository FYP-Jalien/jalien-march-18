package alien.site;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import alien.config.ConfigUtils;
import alien.site.packman.CVMFS;
import alien.site.packman.PackMan;
import alien.user.UserFactory;

/**
 * @author mmmartin
 *
 */
public class SiteMap {

	private static final Logger logger = ConfigUtils.getLogger(SiteMap.class.getCanonicalName());

	private final HashMap<String, Object> siteMap = new HashMap<>();

	/**
	 * @param env
	 * @return the site parameters to send to the job broker (packages, ttl, ce/site...)
	 */
	public HashMap<String, Object> getSiteParameters(final Map<String, String> env) {
		if (env == null)
			return null;

		logger.log(Level.INFO, "Getting site map");

		// Local vars
		PackMan packMan = null;
		int origTtl;
		String partition = "";
		String ceRequirements = "";
		List<String> packages;
		List<String> installedPackages;
		final ArrayList<String> extrasites = new ArrayList<>();
		String site = null;
		String ce = null;
		String cehost = null;

		// Get hostname
		final String hostName = ConfigUtils.getLocalHostname();
		siteMap.put("Localhost", hostName);

		// ALIEN_CM_AS_LDAP_PROXY to send messages upstream through VoBox (no really used anymore in JAliEn?)
		String alienCm = hostName;
		if (env.containsKey("ALIEN_CM_AS_LDAP_PROXY"))
			alienCm = env.get("ALIEN_CM_AS_LDAP_PROXY");

		siteMap.put("alienCm", alienCm);

		// Getting PackMan instance and packages
		String installationMethod = "CVMFS";
		if (env.containsKey("installationMethod"))
			installationMethod = env.get("installationMethod");

		packMan = getPackman(installationMethod, env);
		// siteMap.put("PackMan", packMan);

		// Site name and CE name
		site = env.get("site");
		ce = env.get("CE");
		cehost = env.get("CEhost");

		// TTL
		if (env.containsKey("TTL"))
			origTtl = Integer.parseInt(env.get("TTL"));
		else
			origTtl = 12 * 3600;

		siteMap.put("TTL", Integer.valueOf(origTtl));

		// CE Requirements
		if (env.containsKey("cerequirements"))
			ceRequirements = env.get("cerequirements");

		// Partition
		if (env.containsKey("partition"))
			partition = env.get("partition");

		// Close storage
		if (env.containsKey("closeSE")) {
			final ArrayList<String> temp_sites = new ArrayList<>(Arrays.asList(env.get("closeSE").split(",")));
			for (String st : temp_sites) {
				st = st.split("::")[1];
				extrasites.add(st);
			}
		}

		// Get users from cerequirements field
		final ArrayList<String> users = getFieldContentsFromCerequirements(ceRequirements, CE_FIELD.Users);

		// Get nousers from cerequirements field
		final ArrayList<String> nousers = getFieldContentsFromCerequirements(ceRequirements, CE_FIELD.NoUsers);

		// Workdir
		String workdir = UserFactory.getUserHome();
		if (env.containsKey("WORKDIR"))
			workdir = env.get("WORKDIR");
		if (env.containsKey("TMPBATCH"))
			workdir = env.get("TMPBATCH");

		siteMap.put("workdir", workdir);

		// Setting values of the map
		final String platform = ConfigUtils.getPlatform();

		if (platform != null)
			siteMap.put("Platform", platform);

		// Only include packages if "CVMFS=1" is not present
		if (!siteMap.containsKey("CVMFS") || !siteMap.get("CVMFS").equals(Integer.valueOf(1))) {
			packages = packMan.getListPackages();
			installedPackages = packMan.getListInstalledPackages();

			// We prepare the packages for direct matching
			String packs = ",";
			Collections.sort(packages);
			for (final String pack : packages)
				packs += pack + ",,";

			packs = packs.substring(0, packs.length() - 1);

			String instpacks = ",";
			Collections.sort(installedPackages);
			for (final String pack : installedPackages)
				instpacks += pack + ",,";

			instpacks = instpacks.substring(0, instpacks.length() - 1);

			siteMap.put("Packages", packs);
			siteMap.put("InstalledPackages", instpacks);
		}
		siteMap.put("CE", ce);
		siteMap.put("Site", site);
		siteMap.put("CEhost", cehost);
		if (users.size() > 0)
			siteMap.put("Users", users);
		if (nousers.size() > 0)
			siteMap.put("NoUsers", nousers);
		if (extrasites.size() > 0)
			siteMap.put("Extrasites", extrasites);

		siteMap.put("Host", alienCm.split(":")[0]);

		if (env.containsKey("Disk"))
			siteMap.put("Disk", env.get("Disk"));
		else
			siteMap.put("Disk", Long.valueOf(JobAgent.getFreeSpace(workdir) / 1024));

		if (!partition.equals(""))
			siteMap.put("Partition", partition);

		return siteMap;
	}

	// Gets a PackMan instance depending on configuration (env coming from LDAP)
	private PackMan getPackman(final String installationMethod, final Map<String, String> envi) {
		switch (installationMethod) {
			case "CVMFS":
				siteMap.put("CVMFS", Integer.valueOf(1));
				return new CVMFS(envi.containsKey("CVMFS_PATH") ? envi.get("CVMFS_PATH") : "");
			default:
				siteMap.put("CVMFS", Integer.valueOf(1));
				return new CVMFS(envi.containsKey("CVMFS_PATH") ? envi.get("CVMFS_PATH") : "");
		}
	}

	/**
	 * The two options that can be extracted from the CE requirements (allowed or denied account names)
	 */
	public static enum CE_FIELD {
		/**
		 * Allowed account pattern
		 */
		Users(Pattern.compile("\\s*other.user\\s*==\\s*\"(\\w+)\"")),

		/**
		 * Denied account pattern
		 */
		NoUsers(Pattern.compile("\\s*other.user\\s*!=\\s*\"(\\w+)\""));

		private final Pattern pattern;

		private CE_FIELD(final Pattern pattern) {
			this.pattern = pattern;
		}
	}

	/**
	 * @param cereqs the CE requirements LDAP content
	 * @param field which field to extract (either "Users" or "NoUsers")
	 * @return the account names that match the given field constraint, or <code>null</code> if the field is not one of the above
	 */
	public static ArrayList<String> getFieldContentsFromCerequirements(final String cereqs, final CE_FIELD field) {
		final ArrayList<String> fieldContents = new ArrayList<>();

		if (cereqs != null && !cereqs.isBlank()) {
			final Matcher m = field.pattern.matcher(cereqs);

			while (m.find())
				fieldContents.add(m.group(1));
		}

		return fieldContents;
	}
}
