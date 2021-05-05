package alien.catalogue.access;

import java.io.Serializable;
import java.net.URL;
import java.util.HashMap;
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
	 * Signed envelope
	 */
	protected String signedEnvelope;

	/**
	 * UnSigned envelope
	 */
	protected String unSignedEnvelope;

	/**
	 * UnEncrypted envelope
	 */
	protected String unEncryptedEnvelope;

	/**
	 * Encrypted envelope
	 */
	protected String encryptedEnvelope;

	static {
		// call the static initialization of Xrootd, to set up the URL handler in particular
		Xrootd.getXrootdDefaultPath();
	}

	/**
	 * @param type
	 * @param pfn
	 */
	public XrootDEnvelope(final AccessType type, final PFN pfn) {
		this.type = type;
		this.pfn = pfn;

		setUnsignedEnvelope();
		setUnEncryptedEnvelope();
	}

	/**
	 * @param type
	 * @param pfn
	 * @param lfnc
	 */
	public XrootDEnvelope(final AccessType type, final PFN pfn, final LFN_CSD lfnc) {
		this.type = type;
		this.pfn = pfn;

		setUnsignedEnvelope(lfnc);
		setUnEncryptedEnvelope(lfnc);
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

			unEncryptedEnvelope = envelope;

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

			unSignedEnvelope = envelope;
		}
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
	}

	/**
	 * @return the member of the archive that should be extracted, as indicated in the original access request
	 */
	public LFN getArchiveAnchor() {
		return archiveAnchorLFN;
	}

	/**
	 * @return envelope xml
	 */
	public String getUnEncryptedEnvelope() {
		return unEncryptedEnvelope;
	}

	/**
	 * set envelope
	 */
	private void setUnEncryptedEnvelope() {

		final String access = type.toString().replace("write", "write-once");

		String sPFN = pfn.getPFN();

		try {
			final URL url = new URL(sPFN);
			sPFN = url.getPath().substring(1);
		}
		catch (@SuppressWarnings("unused") final Throwable t) {
			logger.log(Level.WARNING, "Cannot parse this URL, falling back to the legacy parsing method");

			final int idx = sPFN.indexOf("//");

			if (idx >= 0)
				sPFN = sPFN.substring(sPFN.indexOf("//", idx + 2) + 1);
		}

		final GUID guid = pfn.getGuid();

		final Set<LFN> lfns = guid.getLFNs();

		String ret = "<authz>\n  <file>\n" + "    <access>" + access + "</access>\n";

		String sturl = pfn.getPFN();
		if (archiveAnchorLFN != null)
			sturl += "#" + archiveAnchorLFN.getFileName();

		ret += "    <turl>" + Format.escHtml(sturl) + "</turl>\n";

		LFN refLFN = null;
		GUID refGUID = guid;

		if (archiveAnchorLFN != null) {
			refGUID = GUIDUtils.getGUID(archiveAnchorLFN);
			refLFN = archiveAnchorLFN;
		}
		else if (lfns != null && lfns.size() > 0)
			refLFN = lfns.iterator().next();

		if (refLFN != null)
			ret += "    <lfn>" + Format.escHtml(refLFN.getCanonicalName()) + "</lfn>\n";
		else
			ret += "    <lfn>/NOLFN</lfn>\n";

		final SE se = pfn.getSE();

		// files opened by ROOT for streaming don't have a known size yet
		if (refGUID.size > 0)
			ret += "    <size>" + refGUID.size + "</size>" + "\n";

		ret += "    <guid>" + Format.escHtml(refGUID.getName().toUpperCase()) + "</guid>\n";

		// same for MD5
		if (refGUID.md5 != null)
			ret += "    <md5>" + Format.escHtml(refGUID.md5) + "</md5>\n";

		ret += "    <pfn>" + Format.escHtml(sPFN) + "</pfn>\n" + "    <se>" + Format.escHtml(se != null ? se.getName() : "VO::UNKNOWN::SE") + "</se>\n" + "  </file>\n</authz>\n";

		unEncryptedEnvelope = ret;
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

	/**
		 *
		 */
	public void setTransactionURL() {
		final SE se = pfn.getSE();

		if (se == null) {
			if (logger.isLoggable(Level.WARNING))
				logger.log(Level.WARNING, "Null SE for " + pfn);

			turl = pfn.pfn;
			return;
		}

		if (se.seName.indexOf("DCACHE") > 0) {
			final GUID guid = pfn.getGuid();

			final Set<LFN> lfns = guid.getLFNs();

			if (lfns != null && lfns.size() > 0)
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
	 * @return url envelope
	 */
	public String getUnsignedEnvelope() {
		return unSignedEnvelope;
	}

	/**
	 * set url envelope
	 */
	public void setUnsignedEnvelope() {

		setTransactionURL();

		final GUID guid = pfn.getGuid();

		final Set<LFN> lfns = guid.getLFNs(true);

		final HashMap<String, String> e = new HashMap<>(8);

		e.put("turl", pfn.getPFN());
		if (archiveAnchorLFN != null)
			e.put("turl", pfn.getPFN() + "#" + archiveAnchorLFN.getFileName());

		e.put("access", type.toString());

		e.put("lfn", "/NOLFN");

		if (archiveAnchorLFN != null)
			e.put("lfn", archiveAnchorLFN.getCanonicalName());
		else if (lfns != null && lfns.size() > 0)
			e.put("lfn", lfns.iterator().next().getCanonicalName());

		if (archiveAnchorLFN == null) {
			e.put("guid", guid.getName());
			e.put("size", String.valueOf(guid.size));
			e.put("md5", guid.md5);

		}
		else {
			final GUID archiveAnchorGUID = GUIDUtils.getGUID(archiveAnchorLFN);
			e.put("zguid", guid.getName());
			e.put("guid", archiveAnchorGUID.getName());
			e.put("size", String.valueOf(archiveAnchorGUID.size));
			e.put("md5", archiveAnchorGUID.md5);
		}

		final SE se = pfn.getSE();

		if (se != null)
			if ("alice::cern::setest".equalsIgnoreCase(se.getName()))
				e.put("se", "alice::cern::testse");
			else
				e.put("se", se.getName());

		e.put("xurl", addXURLForSpecialSEs(e.get("lfn")));

		final StringTokenizer hash = new StringTokenizer(hashord, "-");

		final StringBuilder ret = new StringBuilder();
		final StringBuilder usedHashOrd = new StringBuilder();

		while (hash.hasMoreTokens()) {
			final String key = hash.nextToken();

			if (e.get(key) != null) {
				ret.append(key).append('=').append(e.get(key)).append('&');
				usedHashOrd.append(key).append('-');
			}
		}

		ret.append("hashord=").append(usedHashOrd).append("hashord");

		unSignedEnvelope = ret.toString();
	}

	private String addXURLForSpecialSEs(final String lfn) {

		final SE se = pfn.getSE();

		// $se =~ /dcache/i
		// $se =~ /alice::((RAL)|(CNAF))::castor/i
		// $se =~ /alice::RAL::castor2_test/i
		if (se != null && se.seName.toLowerCase().contains("dcache"))
			return se.seioDaemons + "/" + lfn;

		return null;
	}

	/**
	 * @param signedEnvelope
	 */
	public void setSignedEnvelope(final String signedEnvelope) {
		this.signedEnvelope = signedEnvelope;
	}

	/**
	 * @return the signed envelope
	 */
	public String getSignedEnvelope() {
		return signedEnvelope;
	}

	/**
	 * @param encryptedEnvelope
	 */
	public void setEncryptedEnvelope(final String encryptedEnvelope) {
		this.encryptedEnvelope = encryptedEnvelope;
	}

	/**
	 * @return encrypted envelope
	 */
	public String getEncryptedEnvelope() {
		return encryptedEnvelope;
	}

	/**
	 * set url envelope
	 *
	 * @param lfnc
	 */
	public void setUnsignedEnvelope(final LFN_CSD lfnc) {

		setTransactionURL(lfnc);

		// final GUID guid = pfn.getGuid();
		//
		// final Set<LFN> lfns = guid.getLFNs();

		final HashMap<String, String> e = new HashMap<>(8);

		e.put("turl", pfn.getPFN());
		if (archiveAnchorLFN != null)
			e.put("turl", pfn.getPFN() + "#" + archiveAnchorLFN.getFileName());

		e.put("access", type.toString());

		e.put("lfn", "/NOLFN");

		if (archiveAnchorLFN != null)
			e.put("lfn", archiveAnchorLFN.getCanonicalName());
		else if (lfnc.exists)
			e.put("lfn", lfnc.getCanonicalName());

		if (archiveAnchorLFN == null) {
			e.put("guid", lfnc.id.toString());
			e.put("size", String.valueOf(lfnc.size));
			e.put("md5", lfnc.checksum);

		}
		else {
			final GUID archiveAnchorGUID = GUIDUtils.getGUID(archiveAnchorLFN);
			e.put("zguid", lfnc.id.toString());
			e.put("guid", archiveAnchorGUID.getName());
			e.put("size", String.valueOf(archiveAnchorGUID.size));
			e.put("md5", archiveAnchorGUID.md5);
		}

		final SE se = pfn.getSE();

		if (se != null)
			if ("alice::cern::setest".equalsIgnoreCase(se.getName()))
				e.put("se", "alice::cern::testse");
			else
				e.put("se", se.getName());

		e.put("xurl", addXURLForSpecialSEs(e.get("lfn")));

		final StringTokenizer hash = new StringTokenizer(hashord, "-");

		final StringBuilder ret = new StringBuilder();
		final StringBuilder usedHashOrd = new StringBuilder();

		while (hash.hasMoreTokens()) {
			final String key = hash.nextToken();

			if (e.get(key) != null) {
				ret.append(key).append('=').append(e.get(key)).append('&');
				usedHashOrd.append(key).append('-');
			}
		}

		ret.append("hashord=").append(usedHashOrd).append("hashord");

		unSignedEnvelope = ret.toString();
	}

	/**
	 * @param lfnc
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
			// final GUID guid = pfn.getGuid();
			//
			// final Set<LFN> lfns = guid.getLFNs();

			if (lfnc.exists)
				turl = se.seioDaemons + "/" + lfnc.getCanonicalName();
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
	 * set envelope
	 *
	 * @param lfnc
	 */
	private void setUnEncryptedEnvelope(final LFN_CSD lfnc) {

		final String access = type.toString().replace("write", "write-once");

		String sPFN = pfn.getPFN();

		try {
			final URL url = new URL(sPFN);
			sPFN = url.getPath().substring(1);
		}
		catch (@SuppressWarnings("unused") final Throwable t) {
			logger.log(Level.WARNING, "Cannot parse this URL, falling back to the legacy parsing method");

			final int idx = sPFN.indexOf("//");

			if (idx >= 0)
				sPFN = sPFN.substring(sPFN.indexOf("//", idx + 2) + 1);
		}

		// final GUID guid = pfn.getGuid();
		//
		// final Set<LFN> lfns = guid.getLFNs();

		String ret = "<authz>\n  <file>\n" + "    <access>" + access + "</access>\n";

		String sturl = pfn.getPFN();
		if (archiveAnchorLFN != null)
			sturl += "#" + archiveAnchorLFN.getFileName();

		ret += "    <turl>" + Format.escHtml(sturl) + "</turl>\n";

		LFN_CSD refLFN = null;
		// UUID refGUID = null;

		if (archiveAnchorLFN != null) {
			// refGUID = lfnc.id;
			refLFN = lfnc;
		}
		else if (lfnc.exists)
			refLFN = lfnc;

		if (refLFN != null)
			ret += "    <lfn>" + Format.escHtml(refLFN.getCanonicalName()) + "</lfn>\n";
		else
			ret += "    <lfn>/NOLFN</lfn>\n";

		final SE se = pfn.getSE();

		ret += "    <size>" + lfnc.size + "</size>" + "\n" + "    <guid>" + Format.escHtml(lfnc.id.toString().toUpperCase()) + "</guid>\n" + "    <md5>" + Format.escHtml(lfnc.checksum) + "</md5>\n"
				+ "    <pfn>" + Format.escHtml(sPFN) + "</pfn>\n" + "    <se>" + Format.escHtml(se != null ? se.getName() : "VO::UNKNOWN::SE") + "</se>\n" + "  </file>\n</authz>\n";

		unEncryptedEnvelope = ret;
	}

	/**
	 * @param envelope
	 * @return the access envelope, encoded in a way that can be passed as either header or CGI parameter to HTTP requests
	 */
	public static String urlEncodeEnvelope(final String envelope) {
		return Format.replace(Format.encode(envelope), "+", "%20");
	}
}
