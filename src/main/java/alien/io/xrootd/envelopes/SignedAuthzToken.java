package alien.io.xrootd.envelopes;

import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFN_CSD;
import alien.catalogue.access.XrootDEnvelope;
import alien.config.ConfigUtils;
import alien.config.JAliEnIAm;
import alien.se.SE;

import java.security.GeneralSecurityException;
import java.security.Signature;
import java.security.interfaces.RSAPrivateKey;
import java.util.HashMap;
import java.util.Set;
import java.util.StringTokenizer;

public class SignedAuthzToken extends AuthzToken {

    private final RSAPrivateKey AuthenPrivKey;

    public SignedAuthzToken(final RSAPrivateKey PrivKey) {
        this.AuthenPrivKey = PrivKey;
    }

    public SignedAuthzToken() {
        this.AuthenPrivKey = null;
    }

    /**
     * Initializes the plain envelope
     */
    @Override
    public void init(final XrootDEnvelope envelope, final LFN_CSD lfnc) {
        if (envelope.pfn == null) {
            throw new RuntimeException("Got an envelope with null pfn");
        }

        final GUID guid;
        final Set<LFN> lfns;

        if (lfnc == null) {
            envelope.setTransactionURL();
            guid = envelope.pfn.getGuid();
            lfns = guid.getLFNs(true);
        }
        else {
            envelope.setTransactionURL(lfnc);
            guid = null;
            lfns = null;
        }

        final HashMap<String, String> e = new HashMap<>(8);

        e.put("turl", envelope.pfn.getPFN());
        final LFN archiveAnchorLFN = envelope.getArchiveAnchor();

        if (archiveAnchorLFN != null)
            e.put("turl", envelope.pfn.getPFN() + "#" + archiveAnchorLFN.getFileName());

        e.put("access", envelope.type.toString());

        e.put("lfn", "/NOLFN");

        if (archiveAnchorLFN != null)
            e.put("lfn", archiveAnchorLFN.getCanonicalName());
        else if (lfnc != null && lfnc.exists)
            e.put("lfn", lfnc.getCanonicalName());
        else if (lfns != null && !lfns.isEmpty())
            e.put("lfn", lfns.iterator().next().getCanonicalName());

        if (archiveAnchorLFN == null) {
            e.put("size", String.valueOf(lfnc != null ? lfnc.size : guid.size));
            e.put("md5", lfnc != null ? lfnc.checksum : guid.md5);
            e.put("guid", lfnc != null ? lfnc.id.toString() : guid.getName());
        }
        else {
            final GUID archiveAnchorGUID = GUIDUtils.getGUID(archiveAnchorLFN);
            e.put("zguid", lfnc != null ? lfnc.id.toString() : guid.getName());
            e.put("guid", archiveAnchorGUID.getName());
            e.put("size", String.valueOf(archiveAnchorGUID.size));
            e.put("md5", archiveAnchorGUID.md5);
        }

        final SE se = envelope.pfn.getSE();

        if (se != null)
            if ("alice::cern::setest".equalsIgnoreCase(se.getName()))
                e.put("se", "alice::cern::testse");
            else
                e.put("se", se.getName());

        e.put("xurl", envelope.addXURLForSpecialSEs(e.get("lfn")));

        final StringTokenizer hash = new StringTokenizer(XrootDEnvelope.hashord, "-");

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

        envelope.setPlainEnvelope(ret.toString());
    }

    @Override
    public String seal(final XrootDEnvelope envelope) throws GeneralSecurityException {
        final long issued = System.currentTimeMillis() / 1000L;
        final long expires = issued + 60 * 60 * 24;

        final String toBeSigned = envelope.getPlainEnvelope() + "-issuer-issued-expires&issuer=" + JAliEnIAm.whatsMyName() + "_" + ConfigUtils.getLocalHostname() + "&issued=" + issued + "&expires="
                + expires;

        final Signature signer = Signature.getInstance("SHA384withRSA");

        signer.initSign(AuthenPrivKey);

        signer.update(toBeSigned.getBytes());

        return toBeSigned + "&signature=" + Base64.encode(signer.sign());
    }
}
