package alien.api.catalogue;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.api.Request;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.catalogue.access.AccessType;
import alien.catalogue.access.AuthorizationFactory;
import alien.config.ConfigUtils;
import alien.io.xrootd.envelopes.XrootDEnvelopeSigner;
import alien.se.SEUtils;
import alien.user.AliEnPrincipal;
import alien.user.AuthorizationChecker;

/**
 * 
 * @author ron
 * @since Jun 03, 2011
 */
public class PFNforReadOrDel extends Request {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6219657670649893255L;

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(PFNforReadOrDel.class.getCanonicalName());
	
	private final AccessType access;

	private final String site;
	private final LFN lfn;
	
	// don't remove this guid, if the guid is not send with the pfn to the client, the thing goes nuts!
//	private GUID guid = null;

	private final List<String> ses;
	private final List<String> exses;

	private List<PFN> pfns = null;

	/**
	 * Get PFNs to read
	 * 
	 * @param user
	 * @param role 
	 * @param site
	 * @param access
	 * @param lfn
	 * @param ses
	 * @param exses
	 */
	public PFNforReadOrDel(final AliEnPrincipal user, final String role, String site, AccessType access,
			LFN lfn, List<String> ses, List<String> exses) {
		setRequestUser(user);
		setRoleRequest(role);
		this.site = site;
		this.lfn = lfn;
		this.access = access;
		this.ses = ses;
		this.exses = exses;
	}

	@Override
	public void run() {

		GUID guid = GUIDUtils.getGUID(lfn.guid);

		LFN setArchiveAnchor = null;

		if (guid.getPFNs() != null) {

			pfns = SEUtils.sortBySiteSpecifySEs(guid.getPFNs(), site, true,
					SEUtils.getSEs(ses), SEUtils.getSEs(exses));
			
			try {
				for (PFN pfn : pfns) {

					String reason = AuthorizationFactory.fillAccess(getEffectiveRequester(), pfn, access);

					if (reason != null) {
						logger.log(Level.WARNING, "Access refused because: " + reason);
						continue;
					}
					UUID archiveLinkedTo = pfn.retrieveArchiveLinkedGUID();
					if (archiveLinkedTo != null) {
						GUID archiveguid = GUIDUtils.getGUID(archiveLinkedTo,
								false);
						setArchiveAnchor = lfn;
						List<PFN> apfns = SEUtils.sortBySiteSpecifySEs(
								GUIDUtils.getGUID(
										pfn.retrieveArchiveLinkedGUID())
										.getPFNs(), site, true, SEUtils
										.getSEs(ses), SEUtils.getSEs(exses));
						if (!AuthorizationChecker.canRead(archiveguid, getEffectiveRequester())) {
							logger.log(Level.WARNING, "Access refused because: Not allowed to read sub-archive");
							continue;
						}

						for (PFN apfn : apfns) {
							reason = AuthorizationFactory.fillAccess(getEffectiveRequester(), apfn, access);

							if (reason != null) {
								logger.log(Level.WARNING, "Access refused because: " + reason);
								continue;
							}
							
							logger.log(Level.FINE, "We have an evenlope candidate: "+ apfn.getPFN());

						}
					} 
				}

			} catch (Exception e) {
				logger.log(Level.SEVERE, "WE HAVE AN Exception", e);
			}
			if (pfns != null) {
				for (PFN pfn : pfns) {
					if (pfn.ticket.envelope == null) {
						logger.log(Level.WARNING, "Sorry ... Envelope is null!");
					} else {
						if(setArchiveAnchor!=null)
							pfn.ticket.envelope.setArchiveAnchor(setArchiveAnchor);
						try {
							// we need to both encrypt and sign, the later is
							// not
							// automatic
							XrootDEnvelopeSigner
									.signEnvelope(pfn.ticket.envelope);
						} catch (SignatureException e) {
							logger.log(Level.WARNING, "Sorry ... Could not sign the envelope (SignatureException)", e);
						} catch (InvalidKeyException e) {
							logger.log(Level.WARNING, "Sorry ... Could not sign the envelope (InvalidKeyException)", e);
						} catch (NoSuchAlgorithmException e) {
							logger.log(Level.WARNING, "Sorry ... Could not sign the envelope (NoSuchAlgorithmException)", e);
						}
					}
				}
			}
			else 
				logger.log(Level.WARNING, "Sorry ... No PFN to make an envelope for!");
		}
		else
			logger.log(Level.WARNING, "Sorry ... No PFNs for the file's GUID!");
		
		if(pfns==null)
			pfns = new ArrayList<PFN>(0);
		
		if(pfns.size()<1)
			logger.log(Level.WARNING, "Sorry ... No PFNs for the file's GUID!");
	}
	

	/**
	 * @return PFNs to read from
	 */
	public List<PFN> getPFNs() {
		return pfns;
	}

	@Override
	public String toString() {
			return "Asked for read/delete: " + this.lfn + "\n"
					+ "reply is: " + this.pfns;
	}
}
