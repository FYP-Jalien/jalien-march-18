package alien.catalogue.access;

import java.io.Serializable;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import alien.catalogue.BookingTable;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFN_CSD;
import alien.catalogue.PFN;
import alien.config.ConfigUtils;
import alien.io.protocols.Xrootd;
import alien.io.xrootd.envelopes.AuthzToken;
import alien.io.xrootd.envelopes.EncryptedAuthzToken;
import alien.io.xrootd.envelopes.SciTokensAuthzToken;
import alien.io.xrootd.envelopes.SignedAuthzToken;
import alien.se.SE;
import alien.se.SEUtils;
import lazyj.Format;

/**
 * @author ron
 *
 */
public class XrootDEnvelope implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6510022440471004424L;

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(BookingTable.class.getCanonicalName());

	/**
	 * the order the key-vals have to appear for sign and verify
	 */
	public static final String hashord = "turl-xurl-access-lfn-guid-zguid-size-md5-se";

	/**
	 * the access ticket this envelope belongs to
	 */
	public AccessType type = null;

	/**
	 * pfn of the file on the SE (proto:://hostdns:port//storagepath)
	 */
	public final PFN pfn;

	private String archiveAnchorFileName;

	/**
	 * A LFN that is pointing to this envelope's GUID/PFN us as a guid://
	 * archive link
	 */
	private LFN archiveAnchorLFN;

	/**
	 * Signed transaction url
	 */
	protected String turl;

	/**
	 * Secure envelope
	 */
	protected String secureEnvelope;

	/**
	 * Plain envelope
	 */
	protected String plainEnvelope;

	/**
	 * True if the envelope is encrypted
	 */
	protected String authzAttribute = "";

	static {
		// call the static initialization of Xrootd, to set up the URL handler in particular
		Xrootd.getXrootdDefaultPath();
	}

	/**
	 * @param type the access type
	 * @param pfn Physical File Name
	 */
	public XrootDEnvelope(final AccessType type, final PFN pfn) {
		this(type, pfn, null);
	}

	/**
	 * @param type the access type
	 * @param pfn Physical File Name
	 * @param lfnc Logical File Name (implementation for Cassandra DB)
	 */
	public XrootDEnvelope(final AccessType type, final PFN pfn, final LFN_CSD lfnc) {
		this.type = type;
		this.pfn = pfn;

		final AuthzToken authz;
		final SE referenceSE = this.pfn.getSE();

		if (referenceSE == null || referenceSE.needsEncryptedEnvelope)
			authz = new EncryptedAuthzToken();
		else if (referenceSE.needsSciTokensEnvelope)
			authz = new SciTokensAuthzToken();
		else
			authz = new SignedAuthzToken();

		authz.init(this, lfnc);
	}

	/**
	 * Create a encrypted envelope along verification only
	 *
	 * @param envelope
	 */
	public XrootDEnvelope(final String envelope) {
		this(envelope, envelope.contains("BEGIN ENVELOPE"));
	}

	/**
	 * Create a signed only envelope in order to verify it
	 *
	 * @param xrootdenvelope
	 * @param oldEnvelope
	 */
	public XrootDEnvelope(final String xrootdenvelope, final boolean oldEnvelope) {
		String envelope = xrootdenvelope;

		if (oldEnvelope) {

			String spfn = "";
			turl = "";
			String lfn = "";
			String guid = "";
			String se = "";
			long size = 0;
			String md5 = "";

			setPlainEnvelope(envelope);

			if (envelope.contains("<authz>")) {
				envelope = envelope.substring(envelope.indexOf("<file>") + 7, envelope.indexOf("</file>") - 2);

				final StringTokenizer st = new StringTokenizer(envelope, "\n");

				while (st.hasMoreTokens()) {
					final String tok = st.nextToken();
					final String key = tok.substring(tok.indexOf('<') + 1, tok.indexOf('>'));
					final String value = tok.substring(tok.indexOf('>') + 1, tok.lastIndexOf('<'));

					if ("access".equals(key))
						if (value.startsWith("write"))
							type = AccessType.WRITE;
						else if (value.equals("read"))
							type = AccessType.READ;
						else if (value.equals("delete"))
							type = AccessType.DELETE;
						else
							System.err.println("illegal access type!");
					else if ("turl".equals(key))
						turl = value;
					else if ("pfn".equals(key))
						spfn = value;
					else if ("lfn".equals(key))
						lfn = value;
					else if ("guid".equals(key))
						guid = value;
					else if ("size".equals(key))
						size = Long.parseLong(value);
					else if ("md5".equals(key))
						md5 = value;
					else if ("se".equals(key))
						se = value;
				}

				// The actual GUID object is dropped anyway so there is no need to do DB queries for this, like:
				// GUIDUtils.getGUID(UUID.fromString(guid), true);
				final GUID g = new GUID(UUID.fromString(guid));

				g.md5 = md5;
				g.size = size;
				if (turl.endsWith(spfn))
					spfn = turl;
				else {
					// turl has #archive
					if (turl.contains("#"))
						turl = turl.substring(0, turl.indexOf('#'));
					// turl has LFN rewrite for dCache etc
					if (turl.endsWith(lfn))
						turl = turl.replace(lfn, spfn);
				}

				this.pfn = new PFN(spfn, g, SEUtils.getSE(se));

			}
			else
				this.pfn = null;

		}
		else {

			final StringTokenizer st = new StringTokenizer(envelope, "\\&");
			String spfn = "";
			turl = "";
			String lfn = "";
			String guid = "";
			String se = "";
			long size = 0;
			String md5 = "";

			while (st.hasMoreTokens()) {
				final String tok = st.nextToken();

				final int idx = tok.indexOf('=');

				if (idx >= 0) {
					final String key = tok.substring(0, idx);
					final String value = tok.substring(idx + 1);

					if ("access".equals(key))
						if (value.startsWith("write"))
							type = AccessType.WRITE;
						else if (value.equals("read"))
							type = AccessType.READ;
						else if (value.equals("delete"))
							type = AccessType.DELETE;
						else
							System.err.println("illegal access type!");
					else if ("turl".equals(key))
						turl = value;
					else if ("pfn".equals(key))
						spfn = value;
					else if ("lfn".equals(key))
						lfn = value;
					else if ("guid".equals(key))
						guid = value;
					else if ("size".equals(key))
						size = Long.parseLong(value);
					else if ("md5".equals(key))
						md5 = value;
					else if ("se".equals(key))
						se = value;
				}
			}
			final GUID g = GUIDUtils.getGUID(UUID.fromString(guid), true);

			g.md5 = md5;
			g.size = size;
			if (turl.endsWith(spfn))
				spfn = turl;
			else {
				// turl has #archive
				if (turl.contains("#"))
					turl = turl.substring(0, turl.indexOf('#'));
				// turl has LFN rewrite for dCache etc
				if (turl.endsWith(lfn))
					turl = turl.replace(lfn, spfn);
			}

			this.pfn = new PFN(spfn, g, SEUtils.getSE(se));

			setPlainEnvelope(envelope);
		}
	}

	/**
	 * When the file member name is know, use this method directly
	 * 
	 * @param anchor
	 */
	public void setArchiveAnchor(final String anchor) {
		if (anchor != null)
			archiveAnchorFileName = anchor;
	}

	/**
	 * @return the name of the archive member to access
	 */
	public String getArchiveAnchorFileName() {
		return archiveAnchorFileName;
	}

	/**
	 * Set the LFN that is pointing to this envelope's GUID/PFN us as a guid://
	 * archive link
	 *
	 * @param anchor
	 *            Anchor LFN
	 */
	public void setArchiveAnchor(final LFN anchor) {
		archiveAnchorLFN = anchor;

		if (archiveAnchorFileName == null && archiveAnchorLFN != null)
			archiveAnchorFileName = archiveAnchorLFN.getFileName();
	}

	/**
	 * @return the member of the archive that should be extracted, as indicated in the original access request
	 */
	public LFN getArchiveAnchor() {
		return archiveAnchorLFN;
	}

	/**
	 * @return envelope plain text xml (for encrypted) or json (for SciToken)
	 */
	public String getPlainEnvelope() {
		return plainEnvelope;
	}

	/**
	 * Splitter of PFNs
	 */
	public static final Pattern PFN_EXTRACT = Pattern.compile("^\\w+://([\\w-]+(\\.[\\w-]+)*(:\\d+))?/(.*)$");

	/**
	 * @return URL of the storage. This is passed as argument to xrdcp and in
	 *         most cases it is the PFN but for DCACHE it is a special path ...
	 */
	public String getTransactionURL() {
		return turl;
	}

	public void setPlainEnvelope(final String plainEnvelope) {
		this.plainEnvelope = plainEnvelope;
	}

	public String addXURLForSpecialSEs(final String lfn) {

		final SE se = pfn.getSE();

		// $se =~ /dcache/i
		// $se =~ /alice::((RAL)|(CNAF))::castor/i
		// $se =~ /alice::RAL::castor2_test/i
		if (se != null && se.seName.toLowerCase().contains("dcache"))
			return se.seioDaemons + "/" + lfn;

		return null;
	}


	/**
	 * @param secureEnvelope the signed or encrypted XRootD envelope
	 */
	public void setSecureEnvelope(final String secureEnvelope) {
		this.secureEnvelope = secureEnvelope;
	}

	/**
	 * @return encrypted or signed envelope
	 */
	public String getSecureEnvelope() {
		return secureEnvelope;
	}

	public void setTransactionURL() {
		this.setTransactionURL(null);
	}

	/**
	 * @param lfnc the LFN implementation for Cassandra DB
	 *
	 */
	public void setTransactionURL(final LFN_CSD lfnc) {
		final SE se = pfn.getSE();

		if (se == null) {
			if (logger.isLoggable(Level.WARNING))
				logger.log(Level.WARNING, "Null SE for " + pfn);

			turl = pfn.pfn;
			return;
		}

		if (se.seName.indexOf("DCACHE") > 0) {
			final Set<LFN> lfns = pfn.getGuid().getLFNs(true);

			if (lfnc != null && lfnc.exists)
				turl = se.seioDaemons + "/" + lfnc.getCanonicalName();
			else if (lfns != null && !lfns.isEmpty())
				turl = se.seioDaemons + "/" + lfns.iterator().next().getCanonicalName();
			else
				turl = se.seioDaemons + "//NOLFN";
		}
		else {
			final Matcher m = PFN_EXTRACT.matcher(pfn.pfn);

			if (m.matches())
				if (archiveAnchorLFN != null)
					turl = se.seioDaemons + "/" + m.group(4) + "#" + archiveAnchorLFN.getFileName();
				else
					turl = se.seioDaemons + "/" + m.group(4);
			if (archiveAnchorLFN != null)
				turl = pfn.pfn + "#" + archiveAnchorLFN.getFileName();
			else
				turl = pfn.pfn;
		}
	}

	/**
	 * @param envelope
	 * @return the access envelope, encoded in a way that can be passed as either header or CGI parameter to HTTP requests
	 */
	public static String urlEncodeEnvelope(final String envelope) {
		return Format.replace(Format.encode(envelope), "+", "%20");
	}

	public void setAuthzAttribute() {
		this.authzAttribute = "authz=";
	}

	public String getAuthzAttribute() {
		return authzAttribute;
	}
}
