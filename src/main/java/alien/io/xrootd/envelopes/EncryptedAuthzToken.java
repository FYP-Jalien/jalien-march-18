package alien.io.xrootd.envelopes;

import alien.catalogue.*;
import alien.catalogue.access.XrootDEnvelope;
import alien.config.ConfigUtils;
import alien.se.SE;
import lazyj.Format;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.Security;
import java.security.Signature;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.Set;
import java.util.Stack;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class does the decoding/decryption + encoding/encryption of a given authorization token which has to apply to the following format:
 * <p>
 * -----BEGIN SEALED CIPHER----- .. .. (Base64-encoded cipher) .. -----END SEALED CIPHER----- -----BEGIN SEALED ENVELOPE----- .. .. (Base64-encoded envelope) .. -----END SEALED ENVELOPE-----
 * <p>
 * The result is an authorization token object. Based on original dCache code from Martin Radicke
 *
 * @author ron
 */
public class EncryptedAuthzToken extends AuthzToken {

    /**
     * Logger
     */
    static final Logger logger = ConfigUtils.getLogger(BookingTable.class.getCanonicalName());

    // delimiters used to split the raw token into Cipher and Sealed Envelope
    private final static String CYPHER_START = "-----BEGIN SEALED CIPHER-----";
    private final static String CYPHER_END = "-----END SEALED CIPHER-----";
    private final static String ENVELOPE_START = "-----BEGIN SEALED ENVELOPE-----";
    private final static String ENVELOPE_END = "-----END SEALED ENVELOPE-----";

    // Blowfish initialisation vector
    private final static byte[] BLOWFISH_IV = "$KJh#(}q".getBytes();

    // raw cipher and Sealed Envelope
    private StringBuilder cipherEncryptedBase64;
    private StringBuilder envelopeEncryptedBase64;

    // decrypted blowfish key
    private byte[] symmetricKey;

    // created blowfish key
    private SecretKeySpec freshBlowfish;

    // extracted SHA1-signature to verify envelope data
    private byte[] signature;

    // the envelope data itself (token payload)
    private byte[] envelope;

    // local private key
    // private RSAPrivateKey privKey;

    // remote (e.g. from the file catalogue) public key
    // private RSAPublicKey pubKey;

    // the four key pairs necessary for encryption
    private final RSAPrivateKey AuthenPrivKey;
    private RSAPublicKey AuthenPubKey;
    private RSAPrivateKey SEPrivKey;
    private final RSAPublicKey SEPubKey;

    static {
        // the security provider used for decryption/verification
        Security.addProvider(new BouncyCastleProvider());
    }

    /**
     * Creates a new instance either for encryption or decryption
     *
     * @param PrivKey
     * @param PubKey
     * @param Decrypt
     * @throws GeneralSecurityException
     */
    public EncryptedAuthzToken(final RSAPrivateKey PrivKey, final RSAPublicKey PubKey, final boolean Decrypt) throws GeneralSecurityException {

        if (Decrypt) {
            this.SEPrivKey = PrivKey;
            this.AuthenPubKey = PubKey;
        }

        this.AuthenPrivKey = PrivKey;
        this.SEPubKey = PubKey;
    }

    public EncryptedAuthzToken() {
        this.AuthenPrivKey = null;
        this.SEPubKey = null;
    }


    /**
     * Initializes the plain envelope
     */
    @Override
    public void init(final XrootDEnvelope envelope, final LFN_CSD lfnc) {
        if (envelope.pfn == null) {
            throw new RuntimeException("Got an envelope with null pfn");
        }

        final GUID refGUID;
        final Set<LFN> lfns;

        if (lfnc == null) {
            lfns = envelope.pfn.getGuid().getLFNs(true);
        } else {
            lfns = null;
        }

        final String access = envelope.type.toString().replace("write", "write-once");

        String sPFN = envelope.pfn.getPFN();

        try {
            final URL url = new URL(sPFN);
            sPFN = url.getPath().substring(1);
        } catch (@SuppressWarnings("unused") final Throwable t) {
            logger.log(Level.WARNING, "Cannot parse this URL, falling back to the legacy parsing method");

            final int idx = sPFN.indexOf("//");

            if (idx >= 0)
                sPFN = sPFN.substring(sPFN.indexOf("//", idx + 2) + 1);
        }

        String ret = "<authz>\n  <file>\n" + "    <access>" + access + "</access>\n";

        String sturl = envelope.pfn.getPFN();
        final LFN archiveAnchorLFN = envelope.getArchiveAnchor();
        if (archiveAnchorLFN != null)
            sturl += "#" + archiveAnchorLFN.getFileName();

        ret += "    <turl>" + Format.escHtml(sturl) + "</turl>\n";

        if (lfnc != null && lfnc.exists) {
            ret += "    <lfn>" + Format.escHtml(lfnc.getCanonicalName()) + "</lfn>\n";
        }
        if (archiveAnchorLFN != null) {
            refGUID = GUIDUtils.getGUID(archiveAnchorLFN);
            ret += "    <lfn>" + Format.escHtml(archiveAnchorLFN.getCanonicalName()) + "</lfn>\n";
        } else if (lfns != null && !lfns.isEmpty()) {
            refGUID = envelope.pfn.getGuid();
            ret += "    <lfn>" + Format.escHtml(lfns.iterator().next().getCanonicalName()) + "</lfn>\n";
        }
        else {
            refGUID = null;
            ret += "    <lfn>/NOLFN</lfn>\n";
        }

        // files opened by ROOT for streaming don't have a known size yet
        if (lfnc != null) {
            ret += "    <size>" + lfnc.size + "</size>" + "\n";
            ret += "    <md5>" + Format.escHtml(lfnc.checksum) + "</md5>\n";
            ret += "    <guid>" + Format.escHtml(lfnc.id.toString().toUpperCase()) + "</guid>\n";
        } else if (refGUID != null) {
            if (refGUID.size > 0)
                ret += "    <size>" + refGUID.size + "</size>" + "\n";
            if (refGUID.md5 != null)
                ret += "    <md5>" + Format.escHtml(refGUID.md5) + "</md5>\n";

            ret += "    <guid>" + Format.escHtml(refGUID.getName().toUpperCase()) + "</guid>\n";
        }

        final SE se = envelope.pfn.getSE();
        ret += "    <pfn>" + Format.escHtml(sPFN) + "</pfn>\n" +
                "    <se>" + Format.escHtml(se != null ? se.getName() : "VO::UNKNOWN::SE") + "</se>\n" +
                "  </file>\n</authz>\n";

        envelope.setPlainEnvelope(ret);
    }

    /**
     * Does the actual creation and encryption/encoding of a token.
     *
     * @param envelope the XRootD envelope to encrypt
     * @return the encrypted envelope or NULL if signature could not be verified
     * @throws GeneralSecurityException
     */
    @Override
    public String seal(final XrootDEnvelope envelope) throws GeneralSecurityException {
        // System.out.println("starting encryption of:" + (new
        // String(envelope)));

        // encrypt UUID/CIPHER with the remote public key
        // get RSA-sealed cipher (aka session- or symmetric key(
        encryptSealedCipher();
        // System.out.println("starting cipherEncrypted64:"
        // + new String(cipherEncryptedBase64));

        // create signature and envelope with symmetric key using Blowfish
        encryptSealedEnvelope();
        // System.out.println("starting envelopeEncrypted64:"
        // + new String(envelopeEncryptedBase64));

        envelope.setAuthzAttribute();

        return getToken();
    }

    /**
     * Encrypts the first component of the sealed token, which contains the session key (aka symmetric key).
     *
     * @throws GeneralSecurityException
     */
    private void encryptSealedCipher() throws GeneralSecurityException {
        final KeyGenerator keyGenerator = KeyGenerator.getInstance("Blowfish", "BC");
        keyGenerator.init(128);

        boolean ok;

        byte[] bkey;

        // ugly hack : make sure there is no NULL character that would confuse C
        do {
            freshBlowfish = (SecretKeySpec) keyGenerator.generateKey();

            ok = true;

            bkey = freshBlowfish.getEncoded();

            for (int i = bkey.length - 1; i >= 0; i--)
                if (bkey[i] == 0) {
                    ok = false;
                    break;
                }
        } while (!ok);

        freshBlowfish = new SecretKeySpec(bkey, 0, 16, "Blowfish");

        final byte[] key = new byte[17];
        System.arraycopy(bkey, 0, key, 0, 16);
        key[16] = (byte) '\0';
        final SecretKeySpec freshBlowfishDASHED = new SecretKeySpec(key, 0, 17, "Blowfish");

        final Cipher cipher = Cipher.getInstance("RSA/NONE/PKCS1Padding", "BC");

        cipher.init(Cipher.WRAP_MODE, SEPubKey);

        final byte[] encryptedCipher = cipher.wrap(freshBlowfishDASHED);

        // encode base64
        final String sCipherEncryptedBase64 = Base64Moded.encodeBytes(encryptedCipher);

        this.cipherEncryptedBase64 = new StringBuilder(sCipherEncryptedBase64);
    }

    /**
     * Encrypts the actual envelope (the 2nd component) using the symmetric key and extracts the signature.
     *
     * @throws GeneralSecurityException
     */
    private void encryptSealedEnvelope() throws GeneralSecurityException {

        signature = signEnvelope();

        final Cipher cipher = Cipher.getInstance("Blowfish/CBC/PKCS5Padding", "BC");

        cipher.init(Cipher.ENCRYPT_MODE, freshBlowfish, new IvParameterSpec(BLOWFISH_IV));

        final byte[] encryptedEnvelope = cipher.doFinal(envelope);

        // Base64-decode envelope
        final byte[] encryptedEnvelopeFinal = new byte[encryptedEnvelope.length + 4 + signature.length];

        encryptedEnvelopeFinal[0] = ((byte) (signature.length >> 24));
        encryptedEnvelopeFinal[1] = ((byte) ((signature.length << 8) >> 24));
        encryptedEnvelopeFinal[2] = ((byte) ((signature.length << 16) >> 24));
        encryptedEnvelopeFinal[3] = ((byte) ((signature.length << 24) >> 24));

        System.arraycopy(signature, 0, encryptedEnvelopeFinal, 4, signature.length);

        System.arraycopy(encryptedEnvelope, 0, encryptedEnvelopeFinal, 4 + signature.length, encryptedEnvelope.length);

        final String sEnvelopeEncryptedBase64 = Base64Moded.encodeBytes(encryptedEnvelopeFinal);
        this.envelopeEncryptedBase64 = new StringBuilder(sEnvelopeEncryptedBase64);

    }

    private static final char[] DIGITS = new char[]{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    /**
     * @param bytes
     * @return hex string
     */
    public static String toHex(final byte[] bytes) {
        final char[] out = new char[bytes.length * 2]; // 2 hex characters per
        // byte

        for (int i = 0; i < bytes.length; i++) {
            out[2 * i] = DIGITS[bytes[i] < 0 ? 8 + (bytes[i] + 128) / 16 : bytes[i] / 16]; // append
            // sign
            // bit
            // for
            // negative
            // bytes
            out[2 * i + 1] = DIGITS[bytes[i] < 0 ? (bytes[i] + 128) % 16 : bytes[i] % 16];
        }
        return new String(out); // char sequence to string
    }

    /**
     * Verifies the authenticity of the envelope by comparing the SHA1 hash of the envelope with the signature
     *
     * @return true after successful verification
     * @throws GeneralSecurityException
     */
    private byte[] signEnvelope() throws GeneralSecurityException {

        final Signature signer = Signature.getInstance("SHA1withRSA", "BC");
        signer.initSign(AuthenPrivKey);
        signer.update(envelope);
        return signer.sign();
    }

    /**
     * Splits the raw token (see class description for format) into its two components cipher and envelope
     */
    private String getToken() {

        return CYPHER_START + "\n" + cipherEncryptedBase64.toString() + "\n" + CYPHER_END + "\n" + ENVELOPE_START + "\n" + envelopeEncryptedBase64.toString() + "\n" + ENVELOPE_END + "\n";
    }

    /**
     * Does the actual decryption/decoding of the raw token. This method should not be called for more than one times.
     *
     * @param encryptedEnvelope the encrypted envelope string
     * @return the decrypted envelope or NULL if signature could not be verified
     * @throws GeneralSecurityException
     */
    @Override
    public String unseal(final String encryptedEnvelope) throws GeneralSecurityException {

        // split token into cipher and envelope
        splitToken(encryptedEnvelope);

        // get RSA-sealed cipher (aka session- or symmetric key(
        decryptSealedCipher();

        // System.out.println("sealed cipher decrypted");
        // decrypt signature and envelope with symmetric key using Blowfish
        decryptSealedEnvelope();
        // System.out.println("sealed envelope decrypted");
        // verify envelope using the signature
        if (!verifyEnvelope()) {
            System.out.println("VERIFICATION FAILED!");
            return null;
        }

        return new String(envelope);
    }

    /**
     * Decrypts the first component of the sealed token, which contains the session key (aka symmetric key).
     *
     * @throws GeneralSecurityException
     */
    private void decryptSealedCipher() throws GeneralSecurityException {

        // decode base64
        final byte[] encryptedCipher = Base64Moded.decode(cipherEncryptedBase64.toString());

        // RSA-decrypt the session key by using the local private key
        final Cipher cipher = Cipher.getInstance("RSA/NONE/PKCS1Padding", "BC");
        cipher.init(Cipher.UNWRAP_MODE, SEPrivKey);

        final Key key = cipher.unwrap(encryptedCipher, "Blowfish", Cipher.SECRET_KEY);

        symmetricKey = key.getEncoded();
    }

    /**
     * Decrypts the actual envelope (the 2nd component) using the symmetric key and extracts the signature.
     *
     * @throws GeneralSecurityException
     */
    private void decryptSealedEnvelope() throws GeneralSecurityException {

        // Base64-decode envelope
        final byte[] encryptedEnvelope = Base64Moded.decode(envelopeEncryptedBase64.toString());
        // logger.debug("Sealed envelope total: "+encryptedEnvelope.length);

        // envelope format:
        // ================
        // 1. signature_length[4] !! integer in big endian (network byte order)
        // 2. signature[signature_length] !! RSA-privately encrypted SHA1-hash of
        // envelope_data
        // 3. envelope_data[encryptedEnvelope.length - signature_length - 4] !!
        // the payload of the token, Blowfish-encrypted

        // usually big endian, but for legacy reasons little endian for now
        // (going to be changed in next
        // Alien file catalogue version

        // big endian
        final int signatureLength = encryptedEnvelope[0] & 0xff << 24 | encryptedEnvelope[1] & 0xff << 16 | encryptedEnvelope[2] & 0xff << 8 | encryptedEnvelope[3] & 0xff;

        final int envelopeOffset = 4 + signatureLength;

        // store signature into a separate buffer
        signature = new byte[signatureLength];
        System.arraycopy(encryptedEnvelope, 4, signature, 0, signatureLength);

        final SecretKeySpec symKeySpec = new SecretKeySpec(symmetricKey, 0, (symmetricKey.length - 1), "Blowfish");

        // BC provider doing blowfish decryption
        final Cipher cipher = Cipher.getInstance("Blowfish/CBC/PKCS5Padding", "BC");
        cipher.init(Cipher.DECRYPT_MODE, symKeySpec, new IvParameterSpec(BLOWFISH_IV));
        envelope = cipher.doFinal(encryptedEnvelope, envelopeOffset, encryptedEnvelope.length - envelopeOffset);
    }

    /**
     * Verifies the authenticity of the envelope by comparing the SHA1 hash of the envelope with the signature
     *
     * @return true after successful verification
     * @throws GeneralSecurityException
     */
    private boolean verifyEnvelope() throws GeneralSecurityException {

        final Signature signer = Signature.getInstance("SHA1withRSA", "BC");
        signer.initVerify(AuthenPubKey);
        signer.update(envelope);
        return signer.verify(signature);
    }

    /**
     * Splits the raw token (see class description for format) into its two components cipher and envelope
     *
     * @param rawToken the token which is going to be split
     * @throws GeneralSecurityException
     */
    private void splitToken(final String rawToken) throws GeneralSecurityException {
        cipherEncryptedBase64 = new StringBuilder();
        envelopeEncryptedBase64 = new StringBuilder();

        final Stack<String> stack = new Stack<>();

        try (LineNumberReader input = new LineNumberReader(new StringReader(rawToken))) {
            String line;

            while ((line = input.readLine()) != null) {

                if (line.equals(CYPHER_START)) {
                    stack.push(CYPHER_START);
                    continue;
                }

                if (line.equals(CYPHER_END)) {
                    if (!stack.peek().equals(CYPHER_START))
                        throw new GeneralSecurityException("Illegal format: Cannot parse encrypted cipher");
                    stack.pop();
                    continue;
                }

                if (line.equals(ENVELOPE_START)) {
                    // check if ENVELOPE part is not nested in CYPHER part
                    if (!stack.isEmpty())
                        throw new GeneralSecurityException("Illegal format: Cannot parse encrypted envelope");
                    stack.push(ENVELOPE_START);
                    continue;
                }

                if (line.equals(ENVELOPE_END)) {
                    if (!stack.peek().equals(ENVELOPE_START))
                        throw new GeneralSecurityException("Illegal format: Cannot parse encrypted envelope");
                    stack.pop();
                    continue;
                }

                if (stack.isEmpty())
                    continue;

                if (stack.peek().equals(CYPHER_START)) {
                    cipherEncryptedBase64.append(line);
                    continue;
                }

                if (stack.peek().equals(ENVELOPE_START)) {
                    envelopeEncryptedBase64.append(line);
                }
            }
        } catch (final IOException e) {
            throw new GeneralSecurityException("error reading from token string", e);
        }
    }

    /**
     * Helper method to print out an array in hex notation.
     *
     * @param name   the name to prefix the hex dump
     * @param array  the array which will be dumped
     * @param offset the offset from where the dump will start
     * @param len    the number of bytes to be dumped
     */
    @SuppressWarnings("unused")
    private static String arrayToHex(final String name, final byte[] array, final int offset, final int len) {
        if (array == null)
            return "";

        final StringBuilder sb = new StringBuilder(name + ": ");
        for (int i = offset; i < offset + len; i++) {
            final String s = Integer.toHexString(array[i] & 0xff);
            if (s.length() == 1)
                sb.append("0");
            sb.append(s.toUpperCase());

        }

        sb.append(" (total:");
        sb.append(len);
        sb.append(" bytes)");

        return sb.toString();
    }

    /**
     * This method parses the decrypted envelope and returns its representation object.
     *
     * @return an envelope object
     * @throws GeneralSecurityException   is thrown if envelope has expired
     * @throws CorruptedEnvelopeException is thrown if a parsing error occurs
     */
    public Envelope getEnvelope() throws CorruptedEnvelopeException, GeneralSecurityException {
        return new Envelope(new String(envelope));
    }

    /**
     * @return the envelope
     */
    public String getEnvelopeString() {
        return new String(envelope);
    }

}
