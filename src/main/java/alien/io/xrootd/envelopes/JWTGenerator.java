package alien.io.xrootd.envelopes;

import alien.api.TomcatServer;
import alien.config.ConfigUtils;
import alien.user.JAKeyStore;
import lazyj.Format;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.pkcs.PKCSException;

import java.io.IOException;
import java.security.*;
import java.security.interfaces.RSAPrivateKey;
import java.time.Instant;
import java.util.*;
import java.util.Base64;

public class JWTGenerator {

    public static class Builder {

        private String issuer = "https://" + ConfigUtils.getLocalHostname() + ":" + TomcatServer.getPort() + "/";
        private String audience = "https://wlcg.cern.ch/jwt/v1/any";
        private String subject = "";
        private String scope = "";
        private long expirationTime = 3600;
        private String privateKeyPath = "/etc/grid-security/hostkey.pem";
        private RSAPrivateKey privateKey = null;
        private String jwtid = "";

        public Builder withIssuer(String issuer) {
            this.issuer = issuer;
            return this;
        }

        public Builder withAudience(String audience) {
            this.audience = audience;
            return this;
        }

        public Builder withSubject(String subject) {
            this.subject = subject;
            return this;
        }

        public Builder withScope(String scope) {
            this.scope = scope;
            return this;
        }

        public Builder withExpirationTime(long expirationTime) {
            this.expirationTime = expirationTime;
            return this;
        }

        public Builder withPrivateKeyPath(String privateKeyPath) {
            this.privateKeyPath = privateKeyPath;
            return this;
        }

        public Builder withPrivateKey(RSAPrivateKey privateKey) {
            this.privateKey = privateKey;
            return this;
        }

        public Builder withJWTId(String jwtId) {
            this.jwtid = jwtId;
            return this;
        }

        /**
         * Generates a JWT token by signing the header, payload, and signature.
         *
         * @return the JWT token as a string
         */
        public String sign() {
            if (jwtid.isEmpty()) {
                jwtid = Base64.getUrlEncoder().encodeToString(UUID.randomUUID().toString().getBytes());
            }

            // Create the JWT header
            byte[] headerBytes = "{\"typ\":\"JWT\",\"alg\":\"RS256\",\"wlcg.ver\":\"1.0\"}".getBytes();
            final String headerBase64 = Base64.getUrlEncoder().withoutPadding().encodeToString(headerBytes);

            // Create the JWT payload
            final Map<String, Object> payloadMap = new HashMap<>();
            payloadMap.put("iss", issuer);
            payloadMap.put("aud", audience);
            payloadMap.put("sub", subject);
            payloadMap.put("iat", Instant.now().getEpochSecond());
            payloadMap.put("exp", (Instant.now().getEpochSecond() + expirationTime));
            payloadMap.put("jti", jwtid);
            payloadMap.put("nbf", Instant.now().getEpochSecond());
            payloadMap.put("scope", scope);

            final String payload = Format.toJSON(payloadMap, false).toString();
            final String payloadBase64 = Base64.getUrlEncoder().withoutPadding().encodeToString(payload.getBytes());

            // Create the JWT signature
            byte[] signatureBytes;
            try {
                final PrivateKey privateKey = (this.privateKey != null) ?
                        this.privateKey :
                        JAKeyStore.loadPrivX509(privateKeyPath, "".toCharArray());

                final Signature signature = Signature.getInstance("SHA256withRSA");
                signature.initSign(privateKey);
                final String concatenatedHeaderPayload = headerBase64 + "." + payloadBase64;
                signature.update(concatenatedHeaderPayload.getBytes());
                signatureBytes = signature.sign();
            } catch (InvalidKeyException | SignatureException | IOException | OperatorCreationException |
                     PKCSException | NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
            final String signatureBase64 = Base64.getUrlEncoder().withoutPadding().encodeToString(signatureBytes);

            // Construct the JWT token
            return headerBase64 + "." + payloadBase64 + "." + signatureBase64;
        }
    }


    /**
     * Generates a JSON Web Token (JWT) with the default parameters.
     *
     * @param args unused
     * @throws Exception if an error occurs during the JWT generation process
     */
    public static void main(String[] args) throws Exception {
        final String token = create()
                .withIssuer("https://" + ConfigUtils.getLocalHostname() + ":8080/")
                .withSubject("aliprod")
                .withAudience("https://wlcg.cern.ch/jwt/v1/any")
                .withPrivateKeyPath("/etc/grid-security/tokenkey.pem")
                .withExpirationTime(3600)
                .withScope("storage.write:/eos/dev/alice/test1")
                .sign();
        System.out.println("Generated JWT: " + token);
    }

    public static Builder create() {
        return new Builder();
    }
}
