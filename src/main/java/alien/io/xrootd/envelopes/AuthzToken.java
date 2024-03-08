package alien.io.xrootd.envelopes;

import alien.catalogue.LFN_CSD;
import alien.catalogue.access.XrootDEnvelope;

import java.security.GeneralSecurityException;

public abstract class AuthzToken {

    /**
     * Initializes the plain envelope
     */
    public void init(final XrootDEnvelope envelope, final LFN_CSD lfnc) {
        envelope.setPlainEnvelope("");
    }

    /**
     * A method to seal the XrootDEnvelope by encrypting or signing it
     *
     * @param envelope the XrootDEnvelope to be signed or encrypted
     * @return the sealed XrootDEnvelope
     */
    public abstract String seal(final XrootDEnvelope envelope) throws GeneralSecurityException;

    /**
     * A method to unseal the XrootDEnvelope by decrypting it
     *
     * @param rawToken the encrypted XrootDEnvelope
     * @return the plain XrootDEnvelope
     */
    public String unseal(final String rawToken) throws GeneralSecurityException {
        throw new UnsupportedOperationException();
    }

}
