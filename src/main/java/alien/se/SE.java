package alien.se;

import java.io.IOException;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.PFN;
import alien.catalogue.access.AccessType;
import alien.catalogue.access.AuthorizationFactory;
import alien.config.ConfigUtils;
import alien.io.protocols.Factory;
import alien.io.protocols.SpaceInfo;
import alien.user.AliEnPrincipal;
import alien.user.LDAPHelperRemote;
import lazyj.DBFunctions;
import lazyj.StringFactory;

/**
 * @author costing
 * @since Nov 4, 2010
 */
public class SE implements Serializable, Comparable<SE> {

	/**
	 *
	 */
	private static final long serialVersionUID = -5338699957055031926L;

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(SE.class.getCanonicalName());

	/**
	 * Original name, without the capitalization, in case some app needs the nicer name
	 */
	public final String originalName;

	/**
	 * SE name
	 */
	public final String seName;

	/**
	 * SE number
	 */
	public final int seNumber;

	/**
	 * SE version number, if < 219, then triggers encrypted xrootd envelope creation over boolean needsEncryptedEnvelope
	 */
	public final int seVersion;

	/**
	 * QoS associated to this storage elements
	 */
	public final Set<String> qos;

	/**
	 * IO daemons
	 */
	public final String seioDaemons;

	/**
	 * SE storage path
	 */
	public final String seStoragePath;

	/**
	 * SE used space
	 */
	public long seUsedSpace;

	/**
	 * Number of files
	 */
	public long seNumFiles;

	/**
	 * Minimum size
	 */
	public final long seMinSize;

	/**
	 * SE type
	 */
	public final String seType;

	/**
	 * Access restricted to this users
	 */
	public final Set<String> exclusiveUsers;

	/**
	 * Exclusive write
	 */
	public final Set<String> seExclusiveWrite;

	/**
	 * Exclusive read
	 */
	public final Set<String> seExclusiveRead;

	/**
	 * triggered by the seVersion if < 200
	 */
	public final boolean needsEncryptedEnvelope;

	/**
	 * Access by SciTokens
	 */
	public final boolean needsSciTokensEnvelope;

	/**
	 * Demote write factor
	 */
	public final double demoteWrite;

	/**
	 * Demote read factor
	 */
	public final double demoteRead;

	/**
	 * Size, as declared in LDAP
	 */
	public long size;

	/**
	 * Other options set for this storage element
	 */
	public Map<String, String> options;

	/**
	 * Build an arbitrary SE from the corresponding fields
	 *
	 * @param seName
	 * @param seNumber
	 * @param qos
	 * @param seStoragePath
	 * @param seioDaemons
	 */
	public SE(final String seName, final int seNumber, final String qos, final String seStoragePath, final String seioDaemons) {
		this.originalName = seName;
		this.seName = StringFactory.get(seName.toUpperCase());
		this.seNumber = seNumber;
		this.qos = parseArray(qos);

		this.seVersion = 0;
		this.needsEncryptedEnvelope = true;
		this.needsSciTokensEnvelope = false;

		this.seStoragePath = seStoragePath;
		this.seioDaemons = seioDaemons;

		this.seNumFiles = 0;
		this.seMinSize = 0;
		this.demoteRead = 0;
		this.demoteWrite = 0;
		this.seUsedSpace = 0;
		this.exclusiveUsers = Collections.emptySet();
		this.seExclusiveRead = Collections.emptySet();
		this.seExclusiveWrite = Collections.emptySet();
		this.seType = "n/a";
	}

	/**
	 * @param db
	 */
	SE(final DBFunctions db) {
		originalName = StringFactory.get(db.gets("seName"));

		seName = StringFactory.get(originalName.toUpperCase());

		seNumber = db.geti("seNumber");

		qos = parseArray(db.gets("seQoS"));

		seVersion = db.geti("seVersion");

		// TODO: remove this, when the version in the DB is working and not
		// anymore overwritten to null
		needsEncryptedEnvelope = (seVersion < 200) && (!"ALICE::CERN::SETEST".equals(seName));

		needsSciTokensEnvelope = (seVersion >= 200);

		seioDaemons = StringFactory.get(db.gets("seioDaemons"));

		seStoragePath = StringFactory.get(db.gets("seStoragePath"));

		seUsedSpace = db.getl("seUsedSpace");

		seNumFiles = db.getl("seNumFiles");

		seMinSize = db.getl("seMinSize");

		seType = StringFactory.get(db.gets("seType"));

		exclusiveUsers = parseArray(db.gets("exclusiveUsers"));

		seExclusiveRead = parseArray(db.gets("seExclusiveRead"));

		seExclusiveWrite = parseArray(db.gets("seExclusiveWrite"));

		demoteWrite = db.getd("sedemotewrite");

		demoteRead = db.getd("sedemoteread");

		size = getSize();

		options = getOptions();
	}

	@Override
	public String toString() {
		return "SE: seName: " + seName + "\n" + "seNumber\t: " + seNumber + "\n" + "seVersion\t: " + seVersion + "\n" + "qos\t: " + qos + "\n" + "seioDaemons\t: " + seioDaemons + "\n"
				+ "seStoragePath\t: " + seStoragePath + "\n" + "seSize:\t: " + size + "\n" + "seUsedSpace\t: " + seUsedSpace + "\n" + "seNumFiles\t: " + seNumFiles + "\n" + "seMinSize\t: " + seMinSize
				+ "\n" + "seType\t: " + seType + "\n" + "exclusiveUsers\t: " + exclusiveUsers + "\n" + "seExclusiveRead\t: " + seExclusiveRead + "\n" + "seExclusiveWrite\t: " + seExclusiveWrite
				+ "\noptions:\t" + options;
	}

	/**
	 * @return SE name
	 */
	public String getName() {
		return seName;
	}

	/**
	 * @param qosRequest
	 * @return if this SE server the requested QoS type
	 */
	public boolean isQosType(final String qosRequest) {
		if (qosRequest == null)
			return false;

		return qos.contains(qosRequest.toLowerCase());
	}

	private static final NumberFormat twoDigits = new DecimalFormat("00");
	private static final NumberFormat fiveDigits = new DecimalFormat("00000");

	/**
	 * @return the protocol part
	 */
	public String generateProtocol() {
		return generateProtocol(seioDaemons, seStoragePath);
	}

	/**
	 * The protocol to access a storage element, including the storage prefix
	 *
	 * @param seioDaemons
	 * @param seStoragePath
	 * @return the protocol part
	 */
	public static String generateProtocol(final String seioDaemons, final String seStoragePath) {
		if (seioDaemons == null || seioDaemons.length() == 0)
			return null;

		String ret = seioDaemons;

		if (!ret.endsWith("/") || seStoragePath == null || !seStoragePath.startsWith("/"))
			ret += "/";

		if ((seStoragePath != null) && !seStoragePath.equals("/"))
			ret += seStoragePath;

		return ret;
	}

	/**
	 * @param guid
	 * @return the PFN for this storage
	 */
	public String generatePFN(final GUID guid) {
		return generatePFN(guid.guid);
	}

	/**
	 * @param guid
	 * @return the PFN for this storage
	 */
	public String generatePFN(final UUID guid) {
		String ret = generateProtocol();

		if (ret == null)
			return ret;

		ret += generatePath(guid.toString());
		return StringFactory.get(ret);
	}

	/**
	 * @param uuid
	 * @return path to this UUID (the two levels of folders to it)
	 */
	public static String generatePath(final String uuid) {
		return "/" + twoDigits.format(GUID.getCHash(uuid)) + "/" + fiveDigits.format(GUID.getHash(uuid)) + "/" + uuid;
	}

	/**
	 * @param s
	 * @return the set of elements
	 */
	public static Set<String> parseArray(final String s) {
		if (s == null)
			return null;

		final Set<String> ret = new LinkedHashSet<>();

		final StringTokenizer st = new StringTokenizer(s, ",");

		while (st.hasMoreTokens()) {
			final String tok = StringFactory.get(st.nextToken().trim().toLowerCase());

			if (tok.length() > 0)
				ret.add(tok);
		}

		return Collections.unmodifiableSet(ret);
	}

	/**
	 * Convert one of the sets to the database representation of it, a comma-separated list of elements
	 *
	 * @param set
	 * @return the comma separated list of the values in the given set
	 */
	public static String toArrayString(final Set<String> set) {
		if (set == null)
			return null;

		final StringBuilder sb = new StringBuilder(",");

		for (final String s : set)
			sb.append(s).append(',');

		return sb.toString();
	}

	@Override
	public int compareTo(final SE o) {
		return seName.compareTo(o.seName);
	}

	@Override
	public boolean equals(final Object obj) {
		if (obj == null)
			return false;

		if (!(obj instanceof SE))
			return false;

		return compareTo((SE) obj) == 0;
	}

	@Override
	public int hashCode() {
		return seName.hashCode();
	}

	/**
	 * Check if the user is allowed to read files from this storage element
	 *
	 * @param user
	 * @return <code>true</code> if allowed
	 */
	public boolean canRead(final AliEnPrincipal user) {
		if (seExclusiveRead.size() == 0)
			return true;

		return seExclusiveRead.contains(user.getName());
	}

	/**
	 * Check if the user is allowed to write files in this storage element
	 *
	 * @param user
	 * @return <code>true</code> if allowed
	 */
	public boolean canWrite(final AliEnPrincipal user) {
		if (seExclusiveWrite.size() == 0)
			return true;

		final boolean allowed = seExclusiveWrite.contains(user.getName());

		return allowed;
	}

	private final Map<String, String> getOptions() {
		final int idx = seName.indexOf("::");

		if (idx < 0)
			return Collections.emptyMap();

		final int idx2 = seName.lastIndexOf("::");

		if (idx2 <= idx)
			return Collections.emptyMap();

		final String site = seName.substring(idx + 2, idx2);
		final String name = seName.substring(idx2 + 2);

		final Set<String> ldapinfo = LDAPHelperRemote.checkLdapInformation("name=" + name, "ou=SE,ou=Services,ou=" + site + ",ou=Sites,", "options");

		if (ldapinfo == null || ldapinfo.size() == 0)
			return Collections.emptyMap();

		final Map<String, String> ret = new LinkedHashMap<>(ldapinfo.size());

		for (final String option : ldapinfo) {
			final StringTokenizer st = new StringTokenizer(option.trim(), "= \r\t\n");

			if (st.countTokens() == 2)
				ret.put(st.nextToken(), st.nextToken());
		}

		return Collections.unmodifiableMap(ret);
	}

	/**
	 * @return Storage Element declared size, in KB, or <code>-1</code> if the SE is not defined
	 */
	private final long getSize() {
		final int idx = seName.indexOf("::");

		if (idx < 0)
			return 0;

		final int idx2 = seName.lastIndexOf("::");

		if (idx2 <= idx)
			return 0;

		final String site = seName.substring(idx + 2, idx2);
		final String name = seName.substring(idx2 + 2);

		Set<String> ldapinfo = LDAPHelperRemote.checkLdapInformation("name=" + name, "ou=SE,ou=Services,ou=" + site + ",ou=Sites,", "savedir");

		if (ldapinfo == null || ldapinfo.size() == 0) {
			ldapinfo = LDAPHelperRemote.checkLdapInformation("name=" + name, "ou=SE,ou=Services,ou=" + site + ",ou=Sites,", "name");

			if (ldapinfo == null || ldapinfo.size() == 0)
				return -1;

			return 0;
		}

		long ret = 0;

		for (final String s : ldapinfo) {
			final StringTokenizer st = new StringTokenizer(s, ",");

			while (st.hasMoreTokens())
				try {
					ret += Long.parseLong(st.nextToken());
				}
				catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
					// ignore
				}
		}

		return ret;
	}

	/**
	 * @return space information
	 * @throws IOException
	 */
	public SpaceInfo getSpaceInfo() throws IOException {
		final PFN pfn = new PFN(GUIDUtils.createGuid(), this);

		pfn.pfn = generateProtocol();

		if (!seName.contains("::"))
			return null;

		try {
			final String reason = AuthorizationFactory.fillAccess(AuthorizationFactory.getDefaultUser(), pfn, AccessType.READ, true);

			if (reason != null)
				logger.log(Level.WARNING, "Cannot get access tokens to read the space information of " + originalName + " : " + reason);
		}
		catch (final Throwable t) {
			logger.log(Level.WARNING, "Got exception getting access tokens to read the space information of " + originalName, t);
		}

		final SpaceInfo info = Factory.xrootd.getSpaceInfo(pfn);

		if (info != null)
			info.applyReplicaCorrection(getReplicationFactor());

		return info;
	}

	/**
	 * Get the HTTP port where the same content can be accessed
	 * 
	 * @return the positive value of the HTTP port, or a negative value if this is not supported
	 */
	public int getHTTPPort() {
		final String httpPort = options.get("http_port");

		if (httpPort == null)
			return -1;

		try {
			return Integer.parseInt(httpPort);
		}
		catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
			logger.log(Level.WARNING, "HTTP port is defined but not as a number for " + seName);
			return -2;
		}
	}

	/**
	 * @return the SE internal replication factor, by which the total and used space will be divided for accounting purposes
	 */
	public double getReplicationFactor() {
		final String factor = options.get("replicationfactor");

		if (factor != null) {
			try {
				return Double.parseDouble(factor);
			}
			catch (NumberFormatException nfe) {
				logger.log(Level.WARNING, "Replication factor is not a number for " + seName, nfe);
			}
		}

		return 1;
	}

	/**
	 * Get the HTTPS port where the same content can be accessed
	 * 
	 * @return the positive value of the HTTPS port, or a negative value if this is not supported
	 */
	public int getHTTPSPort() {
		final String httpPort = options.get("https_port");

		if (httpPort == null)
			return -1;

		try {
			return Integer.parseInt(httpPort);
		}
		catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
			logger.log(Level.WARNING, "HTTPS port is defined but not as a number for " + seName);
			return -2;
		}
	}

	/**
	 * Debug method
	 *
	 * @param args
	 */
	public static void main(final String[] args) {
		try (DBFunctions db = ConfigUtils.getDB("alice_users")) {
			db.query("SELECT * FROM SE WHERE sename like 'ALICE::CERN::%';");

			while (db.moveNext()) {
				final SE se = new SE(db);

				System.err.println(se);

				System.err.println(se.getHTTPPort());

				System.err.println();
			}
		}
	}
}
