package alien.api.taskQueue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import alien.config.ConfigUtils;
import alien.site.SiteMap;
import alien.user.LDAPHelper;
import lazyj.DBFunctions;

/**
 * @author marta
 */
public class CE implements Serializable, Comparable<CE> {

	/**
	 *
	 */
	private static final long serialVersionUID = -5338699957055031926L;

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(CE.class.getCanonicalName());

	/**
	 * CE name
	 */
	public final String ceName;

	/**
	 * Maximum amount of running jobs
	 */
	public final int maxjobs;

	/**
	 * Maximum amount of queued jobs
	 */
	public final int maxqueued;

	/**
	 * Current status of the CE
	 */
	public final String status;

	/**
	 * Time To Live of the CE
	 */
	public long TTL;

	/**
	 * VoBox hostname
	 */
	public String host;

	/**
	 * Type of CE's Batchqueue
	 */
	public String type;

	/**
	 * Requirements imposed by the CE
	 */
	public String ceRequirements;

	/**
	 * Match args
	 */
	public String matcharg;

	/**
	 * CPU Cores
	 */
	public int matchCpuCores;

	/**
	 * Required CPU Cores
	 */
	public String requiredCpuCores;

	/**
	 * Partitions
	 */
	public List<String> partitions;

	/**
	 * Site
	 */
	public String site;

	/**
	 * Allowed users
	 */
	public List<String> users = new ArrayList<>();

	/**
	 * Disallowed users
	 */
	public List<String> nousers = new ArrayList<>();

	/**
	 * Build an arbitrary CE from the corresponding fields
	 *
	 * @param ceName
	 * @param maxjobs
	 * @param maxqueued
	 * @param status
	 */
	public CE(final String ceName, final int maxjobs, final int maxqueued, final String status) {
		this.ceName = ceName.toUpperCase();
		this.maxjobs = maxjobs;
		this.maxqueued = maxqueued;
		this.status = status;

		getLDAPFields();
	}

	/**
	 * Fills up the CE fields that are obtained from the LDAP database
	 *
	 */
	private void getLDAPFields() {
		final String[] ceUnits = ceName.split("::");
		this.site = ceUnits[1];
		String name;
		if (ceUnits.length > 2)
			name = ceUnits[2];
		else
			name = "";

		// This will be the dn for the CE
		final String ouCE = "ou=CE,ou=Services,ou=" + site + ",ou=Sites,";
		this.TTL = Long.parseLong(getLdapContentCE(ouCE, name, "TTL", String.valueOf(24 * 3600)));
		this.type = getLdapContentCE(ouCE, name, "type", "");
		this.host = getLdapContentCE(ouCE, name, "host", "");
		this.ceRequirements = getLdapContentCE(ouCE, name, "ceRequirements", "");
		this.requiredCpuCores = "";

		if (ceRequirements.contains("other.cpu")) {
			this.requiredCpuCores = SiteMap.getFieldContentsFromCerequirements(this.ceRequirements, SiteMap.CE_FIELD.RequiredCpuCores).get(0);
		}
		if (ceRequirements.contains("other.user")) {
			this.users = SiteMap.getFieldContentsFromCerequirements(this.ceRequirements, SiteMap.CE_FIELD.Users);
			this.nousers = SiteMap.getFieldContentsFromCerequirements(this.ceRequirements, SiteMap.CE_FIELD.NoUsers);
		}

		this.matcharg = getLdapContentCE(ouCE, name, "matcharg", "");
		if (this.matcharg.contains("CPUCORES")) {
			this.matchCpuCores = Integer.parseInt(SiteMap.getFieldContentsFromCerequirements(this.matcharg, SiteMap.CE_FIELD.MatchCpuCores).get(0));
		}
		final String partitionList = ConfigUtils.getPartitions(ceName);
		this.partitions = Arrays.asList(partitionList.split(","));

	}

	/**
	 * Gets content from specific ldap field
	 *
	 * @param ouCE dns to be queried
	 * @param ce name of the CE
	 * @param parameter ldap field to query
	 * @param defaultString default value in case none is returned
	 * @return value of the field
	 */
	private static String getLdapContentCE(final String ouCE, final String ce, final String parameter, final String defaultString) {
		final Set<String> param = LDAPHelper.checkLdapInformation("name=" + ce, ouCE, parameter);
		String joined = "";
		if (param.size() == 0)
			joined = defaultString;
		else
			joined = String.join(",", param);
		return joined;
	}

	/**
	 * @param db
	 */
	CE(final DBFunctions db) {
		this(db.gets("site").toUpperCase(), db.geti("maxrunning"), db.geti("maxqueued"), db.gets("blocked"));
	}

	/**
	 * @return CE name
	 */
	public String getName() {
		return ceName;
	}

	@Override
	public int compareTo(final CE ceToCompare) {
		return this.ceName.compareTo(ceToCompare.ceName);
	}

	@Override
	public boolean equals(final Object ceToCompare) {
		return ((CE) ceToCompare).ceName.equals(this.ceName);
	}

	@Override
	public int hashCode() {
		return this.ceName.hashCode();
	}

	@Override
	public String toString() {
		return "CE [ceName=" + ceName + ", maxjobs=" + maxjobs + ", maxqueued=" + maxqueued + ", status=" + status + ", TTL=" + TTL + ", host=" + host + ", type=" + type
				+ ", ceRequirements=" + ceRequirements + ", CPUCores=" + matchCpuCores + ", requiredCPUCores=" + requiredCpuCores + ", partitions= " + partitions + "]";
	}

}
