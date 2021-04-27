package alien.api.catalogue;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.api.Request;
import alien.catalogue.CatalogEntity;
import alien.catalogue.LFN;
import alien.catalogue.LFN_CSD;
import alien.catalogue.PFN;
import alien.catalogue.access.AccessType;
import alien.catalogue.access.AuthorizationFactory;
import alien.config.ConfigUtils;
import alien.se.SEUtils;
import alien.user.AliEnPrincipal;

/**
 *
 * @author mmmartin
 * @since November 27, 2018
 */
public class PFNforReadOrDelCsd extends Request {

	/**
	 *
	 */
	private static final long serialVersionUID = 6219657670649893256L;

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(PFNforReadOrDelCsd.class.getCanonicalName());

	private final AccessType access;

	private final String site;
	private final CatalogEntity entity;

	// don't remove this guid, if the guid is not send with the pfn to the
	// client, the thing goes nuts!
	// private GUID guid = null;

	private final List<String> ses;
	private final List<String> exses;

	private List<PFN> pfns = null;

	/**
	 * Get PFNs to read
	 *
	 * @param user
	 * @param site
	 * @param access
	 * @param entity
	 * @param ses
	 * @param exses
	 */
	public PFNforReadOrDelCsd(final AliEnPrincipal user, final String site, final AccessType access, final CatalogEntity entity, final List<String> ses, final List<String> exses) {
		setRequestUser(user);
		this.site = site;
		this.entity = entity;
		this.access = access;
		this.ses = ses;
		this.exses = exses;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(entity.toString(), site, access.toString(), ses != null ? ses.toString() : null, exses != null ? exses.toString() : null);
	}

	@Override
	public void run() {
		final boolean setArchiveAnchor = false;

		pfns = new LinkedList<>();

		final LFN_CSD lfnc = (LFN_CSD) entity;
		final Set<PFN> pfns_lfnc = lfnc.getPFNs(); // TODO resolve whereis recursively

			try {
				for (final PFN pfn : pfns_lfnc) {
					// final UUID archiveLinkedTo = pfn.retrieveArchiveLinkedGUID();
					//
					// if (archiveLinkedTo != null) { // TODO linked to previous to-do above, the final LFNCSD would be resolved after recursion, with its PFNs
					// final GUID archiveguid = GUIDUtils.getGUID(archiveLinkedTo, false);
					//
					// setArchiveAnchor = true;
					//
					// if (!AuthorizationChecker.canRead(archiveguid, getEffectiveRequester())) {
					// logger.log(Level.WARNING, "Access refused because: Not allowed to read sub-archive");
					// continue;
					// }
					//
					// for (final PFN apfn : archiveguid.getPFNs()) {
					// final String reason = AuthorizationFactory.fillAccess(getEffectiveRequester(), apfn, access);
					//
					// if (reason != null) {
					// logger.log(Level.WARNING, "Access refused to " + apfn.getPFN() + " because: " + reason);
					// continue;
					// }
					//
					// logger.log(Level.FINE, "We have an envelope candidate: " + apfn.getPFN());
					//
					// pfns.add(apfn);
					// }
					// }
					// else {
					final String reason = AuthorizationFactory.fillAccessCsd(getEffectiveRequester(), lfnc, pfn, access, false);

					if (reason != null) {
						logger.log(Level.WARNING, "Access refused because: " + reason);
						continue;
					}

					pfns.add(pfn);
					// }
				}

			}
			catch (final Exception e) {
				logger.log(Level.SEVERE, "WE HAVE AN Exception", e);
			}

			if (pfns.size() > 0) {
				pfns = SEUtils.sortBySiteSpecifySEs(pfns, site, true, SEUtils.getSEs(ses), SEUtils.getSEs(exses), false);

				if (setArchiveAnchor)
					for (final PFN pfn : pfns)
						if (pfn.ticket.envelope == null)
							logger.log(Level.WARNING, "Can't set archive anchor on " + pfn.pfn + " since the envelope is null");
						else if (entity instanceof LFN || entity instanceof LFN_CSD)
							pfn.ticket.envelope.setArchiveAnchor((LFN) entity);
			}
			else
				logger.log(Level.WARNING, "Sorry ... No PFN to make an envelope for!");

		if (pfns.size() < 1)
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
		return "Asked for read/delete: " + this.entity + "\n" + "reply is: " + this.pfns;
	}
}
