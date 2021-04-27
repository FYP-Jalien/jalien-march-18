package alien.api.catalogue;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.api.Request;
import alien.catalogue.BookingTable;
import alien.catalogue.BookingTable.BOOKING_STATE;
import alien.catalogue.PFN;
import alien.catalogue.access.XrootDEnvelope;
import alien.catalogue.access.XrootDEnvelopeReply;
import alien.config.ConfigUtils;
import alien.io.xrootd.envelopes.XrootDEnvelopeSigner;
import alien.user.AliEnPrincipal;

/**
 *
 * @author ron
 * @since Jun 05, 2011
 */
public class RegisterEnvelopes extends Request {
	/**
	 *
	 */
	private static final long serialVersionUID = 6927727456767661381L;

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(RegisterEnvelopes.class.getCanonicalName());

	private List<String> signedEnvelopes = null;
	private List<PFN> pfns = null;

	private String encryptedEnvelope = null;
	private long size = 0;
	private String md5 = null;

	private BOOKING_STATE targetState = BOOKING_STATE.COMMITED;

	/**
	 * Register PFNs with envelopes
	 *
	 * @param user
	 * @param signedEnvelopes
	 * @param state what to do which the respective entries
	 */
	public RegisterEnvelopes(final AliEnPrincipal user, final List<String> signedEnvelopes, final BOOKING_STATE state) {
		setRequestUser(user);
		this.signedEnvelopes = signedEnvelopes;
		this.targetState = state;
	}

	/**
	 * Register PFNs with envelopes
	 *
	 * @param user
	 *
	 * @param encryptedEnvelope
	 * @param size
	 * @param md5
	 * @param state what to do which the respective entries
	 */
	public RegisterEnvelopes(final AliEnPrincipal user, final String encryptedEnvelope, final long size, final String md5, final BOOKING_STATE state) {
		setRequestUser(user);
		this.encryptedEnvelope = encryptedEnvelope;
		this.size = size;
		this.md5 = md5;
		this.targetState = state;
	}

	@Override
	public List<String> getArguments() {
		if (signedEnvelopes != null)
			return Arrays.asList(targetState.toString(), signedEnvelopes.toString());

		return Arrays.asList(targetState.toString(), md5, String.valueOf(size), encryptedEnvelope);
	}

	private boolean flagEntry(final PFN onePFN) {
		return BookingTable.mark(getEffectiveRequester(), onePFN, targetState) != null;
	}

	@Override
	public void run() {
		authorizeUserAndRole();

		if (signedEnvelopes != null) {
			pfns = new ArrayList<>(signedEnvelopes.size());

			for (final String env : signedEnvelopes)
				try {
					if (XrootDEnvelopeSigner.verifyEnvelope(env, true)) {
						final XrootDEnvelope xenv = new XrootDEnvelope(env);

						if (logger.isLoggable(Level.FINER))
							logger.log(Level.FINER, "Self Signature VERIFIED! : " + xenv.pfn.pfn);

						if (flagEntry(BookingTable.getBookedPFN(xenv.pfn.pfn))) {
							if (logger.isLoggable(Level.FINE))
								logger.log(Level.FINE, "Successfully moved " + xenv.pfn.pfn + " to the Catalogue");

							pfns.add(xenv.pfn);
						}
						else
							logger.log(Level.WARNING, "Could not commit self-signed " + xenv.pfn.pfn + " to the Catalogue");
					}
					else if (XrootDEnvelopeSigner.verifyEnvelope(env, false)) {
						final XrootDEnvelopeReply xenv = new XrootDEnvelopeReply(env);

						if (logger.isLoggable(Level.FINER))
							logger.log(Level.FINER, "SE Signature VERIFIED! : " + xenv.pfn.pfn);

						if (flagEntry(BookingTable.getBookedPFN(xenv.pfn.pfn))) {
							if (logger.isLoggable(Level.FINE))
								logger.log(Level.FINE, "Successfully moved " + xenv.pfn.pfn + " to the Catalogue");

							pfns.add(xenv.pfn);
						}
						else
							logger.log(Level.WARNING, "Could not commit " + xenv.pfn.pfn + " to the Catalogue");
					}
					else
						logger.log(Level.WARNING, "COULD NOT VERIFY ANY SIGNATURE!");

				}
				catch (final SignatureException e) {
					logger.log(Level.WARNING, "Wrong signature", e);
				}
				catch (final InvalidKeyException e) {
					logger.log(Level.WARNING, "Invalid key", e);
				}
				catch (final NoSuchAlgorithmException e) {
					logger.log(Level.WARNING, "No such algorithm", e);
				}
				catch (final IOException e) {
					logger.log(Level.WARNING, "IO Exception", e);
				}
		}
		else if (encryptedEnvelope != null) {
			pfns = new ArrayList<>(1);
			final XrootDEnvelope xenv;
			try {
				xenv = XrootDEnvelopeSigner.decryptEnvelope(encryptedEnvelope);
			}
			catch (final Exception e) {
				logger.log(Level.WARNING, "Error decrypting envelope", e);
				return;
			}

			PFN bookedpfn = null;

			try {
				bookedpfn = BookingTable.getBookedPFN(xenv.pfn.pfn);
			}
			catch (final Exception e) {
				logger.log(Level.WARNING, "Error getting the PFN: ", e);
				return;
			}

			if (bookedpfn != null) {
				if (size != 0)
					bookedpfn.getGuid().size = size;

				if (md5 != null && md5.length() > 0 && !"0".equals(md5))
					bookedpfn.getGuid().md5 = md5;

				try {
					if (flagEntry(bookedpfn)) {
						if (logger.isLoggable(Level.FINE))
							logger.log(Level.FINE, "Successfully moved " + xenv.pfn.pfn + " to the Catalogue");

						pfns.add(bookedpfn);
					}
					else
						logger.log(Level.WARNING, "Unable to register " + xenv.pfn.pfn + " in the Catalogue");
				}
				catch (final Exception e) {
					logger.log(Level.WARNING, "Error registering pfn", e);
				}
			}
			else
				logger.log(Level.WARNING, "Could not find this booked pfn: " + xenv.pfn.pfn);
		}
	}

	/**
	 * @return PFNs to write on
	 */
	public List<PFN> getPFNs() {
		return pfns;
	}

	@Override
	public String toString() {
		return "Asked to register: " + (encryptedEnvelope != null ? " encrypted envelope " + encryptedEnvelope : "signed envelopes: " + signedEnvelopes) + ", reply is: " + this.pfns;
	}
}
