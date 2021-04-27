package alien.site;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
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
import apmon.BkThread;

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
		PackMan packMan;
		int origTtl;
		String partition = "";
		String ceRequirements = "";
		List<String> packages;
		List<String> installedPackages;
		final ArrayList<String> extrasites = new ArrayList<>();
		String site;
		String ce;
		String cehost;

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
		origTtl = 12 * 3600;

		// get value from env
		if (env.containsKey("TTL"))
			origTtl = Integer.parseInt(env.get("TTL"));

		// check if there will be a shutdown
		Long shutdownTime = MachineJobFeatures.getFeatureNumber("shutdowntime",
				MachineJobFeatures.FeatureType.MACHINEFEATURE);
		if (shutdownTime != null) {
			shutdownTime = Long.valueOf(shutdownTime.longValue() - System.currentTimeMillis() / 1000);
			logger.log(Level.INFO, "Shutdown is in " + shutdownTime + "s");

			origTtl = Integer.min(origTtl, shutdownTime.intValue());
		}

		logger.log(Level.INFO, "TTL is" + origTtl);

		siteMap.put("TTL", Integer.valueOf(origTtl));

		// CE Requirements
		if (env.containsKey("cerequirements"))
			ceRequirements = env.get("cerequirements");
		logger.log(Level.INFO, "CE requirements are " + ceRequirements);
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

		// Get required cpu cores from cerequirements field
		final ArrayList<String> requiredCpus = getFieldContentsFromCerequirements(ceRequirements, CE_FIELD.CpuCores);

		// Workdir
		String workdir = UserFactory.getUserHome();
		if (env.containsKey("WORKDIR"))
			workdir = env.get("WORKDIR");
		if (env.containsKey("TMPBATCH"))
			workdir = env.get("TMPBATCH");

		siteMap.put("workdir", workdir);

		long space = JobAgent.getFreeSpace(workdir) / 1024;

		// This is measured in MB
		if (env.containsKey("RESERVED_DISK"))
			space -= Long.parseLong(env.get("RESERVED_DISK"));

		logger.log(Level.INFO, "Disk space available " + space);

		// Used for multi-core job scheduling
		siteMap.put("Disk", Long.valueOf(space));

		// Get RAM
		long memorySize = ((com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean()).getTotalPhysicalMemorySize();

		// get from env or LDAP to cap number of CPUs
		if (env.containsKey("RESERVED_RAM"))
			memorySize -= Long.parseLong(env.get("RESERVED_RAM"));

		logger.log(Level.INFO, "Actual RAM: " + memorySize);

		// Get NRCPUs
		long potentialCpus, mjfCpus, numCpus = 1;

		// get from env or LDAP to cap number of CPUs
		if (env.containsKey("CPUCores")) {
			numCpus = Long.parseLong(env.get("CPUCores"));
			if (numCpus == 0) {
				try {
					// get from system
					numCpus = BkThread.getNumCPUs();

					potentialCpus = memorySize / (2 * 1024 * 1024 * 1024L);
					if (numCpus > potentialCpus)
						numCpus = potentialCpus;
				}
				catch (final Exception e) {
					logger.log(Level.WARNING, "Problem with getting CPUs from environment: " + e.toString());
				}

				mjfCpus = MachineJobFeatures.getFeatureNumberOrDefault("log_cores",
						MachineJobFeatures.FeatureType.MACHINEFEATURE, Long.valueOf(numCpus)).longValue();
				if (numCpus > mjfCpus)
					numCpus = mjfCpus;
			}
		}

		logger.log(Level.INFO, "CPU cores available " + numCpus);
		siteMap.put("CPUCores", Long.valueOf(numCpus));

		// Setting values of the map
		final String platform = ConfigUtils.getPlatform();

		if (platform != null)
			siteMap.put("Platform", platform);

		// Only include packages if "CVMFS=1" is not present
		if (!siteMap.containsKey("CVMFS") || !siteMap.get("CVMFS").equals(Integer.valueOf(1))) {
			packages = packMan.getListPackages();
			installedPackages = packMan.getListInstalledPackages();

			// We prepare the packages for direct matching

			final String packs = "," + String.join(",,", packages) + ",";

			final String instpacks = "," + String.join(",,", installedPackages) + ",";

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
		if (requiredCpus.size() == 1 || env.containsKey("RequiredCpusCe"))
			siteMap.put("RequiredCpusCe", requiredCpus.size() > 0 ? requiredCpus.get(0) : env.get("RequiredCpusCe"));
		if (extrasites.size() > 0)
			siteMap.put("Extrasites", extrasites);

		siteMap.put("Host", alienCm.split(":")[0]);

		if (env.containsKey("Disk"))
			siteMap.put("Disk", env.get("Disk"));
		else
			siteMap.put("Disk", Long.valueOf(JobAgent.getFreeSpace(workdir) / 1024));

		if (!"".equals(partition))
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
	public enum CE_FIELD {
		/**
		 * Allowed account pattern
		 */
		Users(Pattern.compile("\\s*other.user\\s*==\\s*\"(\\w+)\"")),

		/**
		 * Denied account pattern
		 */
		NoUsers(Pattern.compile("\\s*other.user\\s*!=\\s*\"(\\w+)\"")),

		/**
		 * CPU Cores pattern
		 */
		CpuCores(Pattern.compile("\\s*other.cpucores\\s*(>=|<=|>|<|==|=|!=)\\s*([0-9]+)"));

		private final Pattern pattern;

		CE_FIELD(final Pattern pattern) {
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
				if (field == CE_FIELD.CpuCores)
					fieldContents.add(m.group(1) + m.group(2));
				else
					fieldContents.add(m.group(1));
		}

		return fieldContents;
	}
}
