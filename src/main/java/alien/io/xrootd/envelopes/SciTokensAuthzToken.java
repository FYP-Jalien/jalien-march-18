package alien.io.xrootd.envelopes;

import alien.api.TomcatServer;
import alien.catalogue.access.XrootDEnvelope;
import alien.config.ConfigUtils;

import java.security.interfaces.RSAPrivateKey;

public class SciTokensAuthzToken extends AuthzToken {

    private final RSAPrivateKey AuthenPrivKey;

    public SciTokensAuthzToken(final RSAPrivateKey PrivKey) {
        this.AuthenPrivKey = PrivKey;
    }

    public SciTokensAuthzToken() {
        this.AuthenPrivKey = null;
    }

    @Override
    public String seal(final XrootDEnvelope envelope) {
        // We will generate a token with both read and write capabilities
        String sPFN = envelope.getTransactionURL();
        final int idx = sPFN.indexOf("//");
        sPFN = sPFN.substring(sPFN.indexOf("//", idx + 2) + 1);
        return JWTGenerator.create()
                .withIssuer("https://" + ConfigUtils.getLocalHostname() + ":" + TomcatServer.getPort() + "/")
                .withSubject("aliprod")
                .withAudience("https://wlcg.cern.ch/jwt/v1/any")
                .withPrivateKey(this.AuthenPrivKey)
                .withExpirationTime(3600)
                .withScope("storage." + envelope.type.toString() + ":" + sPFN)
                .sign();
    }
}
