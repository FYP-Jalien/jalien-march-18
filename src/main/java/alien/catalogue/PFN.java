package alien.catalogue;

import java.io.Serializable;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;

import alien.catalogue.access.AccessTicket;
import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.se.SE;
import alien.se.SEUtils;
import alien.shell.commands.JAliEnCOMMander;
import lazyj.DBFunctions;
import lazyj.StringFactory;

/**
 * Wrapper around a G*L_PFN row
 *
 * @author costing
 *
 */
public class PFN implements Serializable, Comparable<PFN> {

	/**
	 *
	 */
	private static final long serialVersionUID = 3854116042004576123L;

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(PFN.class.getCanonicalName());

	/**
	 * Monitoring component
	 */
	static transient final Monitor monitor = MonitorFactory.getMonitor(PFN.class.getCanonicalName());

	/**
	 * guidID
	 */
	public int guidId;

	/**
	 * PFN
	 */
	public String pfn;

	/**
	 * SE number
	 */
	public int seNumber;

	/**
	 * index
	 */
	public int host;

	/**
	 * table name
	 */
	public int tableNumber;

	/**
	 * GUID references
	 */
	private UUID uuid;

	/**
	 * GUID
	 *
	 * @see #getGuid()
	 */
	private GUID guid;

	private Set<PFN> realPFNs = null;

	/**
	 * Access ticket, if needed
	 */
	public AccessTicket ticket = null;

	private final int hashCode;

	private transient SE overrideSE = null;

	/**
	 * flag to know is a PFN for LFN_CSD accesses
	 */
	public boolean isCsd = false;

	/**
	 * @param db
	 * @param host
	 * @param tableNumber
	 */
	PFN(final DBFunctions db, final int host, final int tableNumber) {
		this.host = host;
		this.tableNumber = tableNumber;

		init(db);

		hashCode = pfn.hashCode();
	}

	private void init(final DBFunctions db) {
		guidId = db.geti("guidId");

		pfn = StringFactory.get(db.gets("pfn"));

		if (pfn.startsWith("guid:///")) {
			// don't correct old PFNs that have for some reason an associated SE number even if they are pointers to archives
			seNumber = 0;
		}
		else {
			seNumber = db.geti("seNumber");

			if (seNumber > 0) {
				final SE se = SEUtils.getSE(seNumber);

				if (se != null && se.seioDaemons != null && se.seioDaemons.length() > 0 && !pfn.startsWith(se.seioDaemons)) {
					int idx = pfn.indexOf("://");

					if (idx > 0 && idx < 10) {
						idx = pfn.indexOf('/', idx + 3);

						if (idx > 0)
							pfn = se.seioDaemons + pfn.substring(idx);
					}
				}
			}
		}
	}

	/**
	 * Generate a new PFN
	 *
	 * @param guid
	 * @param se
	 */
	public PFN(final GUID guid, final SE se) {
		this.guid = guid;
		this.guidId = guid.guidId;
		this.pfn = se.generatePFN(guid);
		this.seNumber = se.seNumber;
		this.overrideSE = se;
		this.host = guid.host;
		this.tableNumber = guid.tableName;
		this.hashCode = this.pfn != null ? this.pfn.hashCode() : guid.hashCode();
	}

	/**
	 * Load a PFN Object from String
	 *
	 * @param pfn
	 * @param guid
	 * @param se
	 */
	public PFN(final String pfn, final GUID guid, final SE se) {
		this.guid = guid;
		this.guidId = guid.guidId;
		this.pfn = pfn;

		if (se != null) {
			this.seNumber = se.seNumber;
			this.overrideSE = se;
		}

		this.host = guid.host;
		this.tableNumber = guid.tableName;
		this.hashCode = this.pfn.hashCode();
	}

	/**
	 * Create PFN from LFN_CSD
	 *
	 * @param senumber
	 * @param pfn
	 * @param id
	 * @param size
	 */
	public PFN(final Integer senumber, final String pfn, final UUID id, final long size) {
		this.pfn = pfn; // TODO recreate the pfn with the seioDaemons
		this.seNumber = senumber.intValue();
		this.isCsd = true;
		this.uuid = id;
		this.guid = GUIDUtils.getGUID(id);

		if (this.guid != null)
			this.guid.size = size;

		this.hashCode = this.pfn != null ? this.pfn.hashCode() : (this.guid != null ? guid.hashCode() : id.hashCode());
	}

	/**
	 * @param path
	 *            correct path for case-sensitive locations
	 */
	public void setPath(final String path) {
		if (this.pfn.toLowerCase().endsWith(path.toLowerCase()))
			this.pfn = this.pfn.substring(0, this.pfn.length() - path.length()) + path;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder("PFN: guidId\t: ");
		sb.append(guidId).append("\npfn\t\t: ").append(pfn);
		sb.append("\nseNumber\t: ").append(seNumber);

		if (guid != null)
			sb.append("\nGUID cache value: ").append(guid);

		if (ticket != null)
			sb.append("\nAccess ticket attached (").append(ticket.getAccessType()).append(")");

		return sb.toString();
	}

	/**
	 * @return the PFN URL
	 */
	public String getPFN() {
		return pfn;
	}

	/**
	 * @return the URL to the same file, if the SE supports HTTP or HTTPS (must have a "http" tag and set at least one of "http_port" or "https_port" options to an integer value), or <code>null</code> if not
	 */
	public String getHttpURL() {
		if (!pfn.startsWith("root://"))
			return null;

		int httpPort = -1;

		boolean isHttps = false;

		final SE se = getSE();

		if (se != null && se.isQosType("http")) {
			httpPort = se.getHTTPPort();

			if (httpPort <= 0) {
				httpPort = se.getHTTPSPort();
				isHttps = httpPort > 0;
			}
		}

		String httpUrl = null;

		if (httpPort > 0) {
			final String protocol = isHttps ? "https://" : "http://";

			final String url = pfn.substring(7);

			final int idxColumn = url.indexOf(':');
			final int idxSlash = url.indexOf('/');

			if (idxColumn > 0 && idxSlash > idxColumn)
				httpUrl = protocol + url.substring(0, idxColumn + 1) + httpPort + url.substring(idxSlash);
			else if (idxSlash > 0)
				httpUrl = protocol + url.substring(0, idxSlash) + ":" + httpPort + url.substring(idxSlash);
		}

		return httpUrl;
	}

	/**
	 * @param pfn
	 * @return the UUID from this PFN, if it's a pointer to an archive, or <code>null</code> if not
	 */
	public static String getGUIDFromPFN(final String pfn) {
		if (pfn.startsWith("guid://")) {
			int idx = 7;

			while (pfn.charAt(idx) == '/' && idx < pfn.length() - 1)
				idx++;

			final int idx2 = pfn.indexOf('?', idx);

			if (idx2 < 0)
				return pfn.substring(idx);

			return pfn.substring(idx, idx2);
		}

		return null;
	}

	/**
	 * @return the physical locations
	 */
	public Set<PFN> getRealPFNs() {
		if (realPFNs != null)
			return realPFNs;

		final String sUuid = getGUIDFromPFN(pfn);

		if (sUuid != null) {
			final GUID archiveGuid = ConfigUtils.isCentralService() ? GUIDUtils.getGUID(UUID.fromString(sUuid)) : JAliEnCOMMander.getInstance().c_api.getGUID(sUuid);

			if (archiveGuid != null)
				realPFNs = archiveGuid.getPFNs();
			else
				realPFNs = null;
		}
		else {
			realPFNs = new LinkedHashSet<>(1);
			realPFNs.add(this);
		}

		return realPFNs;
	}

	/**
	 * Set the UUID, when known, to avoid reading from database
	 *
	 * @param uid
	 */
	void setUUID(final UUID uid) {
		uuid = uid;
	}

	/**
	 * Set the GUID, when known, to avoid reading from database
	 *
	 * @param guid
	 */
	void setGUID(final GUID guid) {
		this.guid = guid;
	}

	/**
	 * @return get the UUID associated to the GUID of which this entry is a replica
	 */
	public UUID getUUID() {
		if (uuid == null)
			if (guid != null)
				uuid = guid.guid;
			else
				getGuid();

		return uuid;
	}

	/**
	 * @return the GUID for this PFN
	 */
	public GUID getGuid() {
		if (guid == null)
			if (uuid != null)
				guid = GUIDUtils.getGUID(uuid);
			else {
				final Host h = CatalogueUtils.getHost(host);

				if (h == null)
					return null;

				try (DBFunctions db = h.getDB()) {
					if (monitor != null)
						monitor.incrementCounter("GUID_db_lookup");

					db.setReadOnly(true);

					db.query("SELECT * FROM G" + tableNumber + "L WHERE guidId=?;", false, Integer.valueOf(guidId));

					if (db.moveNext()) {
						guid = new GUID(db, host, tableNumber);
						uuid = guid.guid;
					}
				}
			}

		return guid;
	}

	@Override
	public int compareTo(final PFN o) {
		return pfn.compareTo(o.pfn);
	}

	@Override
	public boolean equals(final Object obj) {
		if (!(obj instanceof PFN))
			return false;

		return compareTo((PFN) obj) == 0;
	}

	@Override
	public int hashCode() {
		return this.hashCode;
	}

	private boolean isArchiveLinkedGUID() {
		return (pfn.toLowerCase()).startsWith("guid://");
	}

	/**
	 * @return the GUID to which this PFN points to
	 */
	public UUID retrieveArchiveLinkedGUID() {
		if (isArchiveLinkedGUID())
			return UUID.fromString(pfn.substring(8, 44));
		return null;
	}

	/**
	 * @param se
	 *            new SE
	 */
	public void setOverrideSE(final SE se) {
		this.overrideSE = se;
	}

	/**
	 * @return the SE
	 */
	public SE getSE() {
		if (this.overrideSE != null)
			return this.overrideSE;

		overrideSE = SEUtils.getSE(seNumber);

		return overrideSE;
	}
}
