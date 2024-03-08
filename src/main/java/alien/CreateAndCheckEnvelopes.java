package alien;

import java.security.GeneralSecurityException;
import java.security.Security;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;

import alien.catalogue.access.XrootDEnvelope;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import alien.io.xrootd.envelopes.EncryptedAuthzToken;
import alien.user.JAKeyStore;
import alien.user.UserFactory;

/**
 * @author ron
 *
 */
public class CreateAndCheckEnvelopes {

	/**
	 * Debugger method
	 *
	 * @param args
	 */
	public static void main(final String[] args) {

		Security.addProvider(new BouncyCastleProvider());

		final String AuthenPrivLocation = UserFactory.getUserHome() + System.getProperty("file.separator") + ".alien" + System.getProperty("file.separator") + "authen"
				+ System.getProperty("file.separator") + "lpriv.pem";
		final String AuthenPubLocation = UserFactory.getUserHome() + System.getProperty("file.separator") + ".alien" + System.getProperty("file.separator") + "authen"
				+ System.getProperty("file.separator") + "lpub.pem";
		final String SEPrivLocation = UserFactory.getUserHome() + System.getProperty("file.separator") + ".alien" + System.getProperty("file.separator") + "authen"
				+ System.getProperty("file.separator") + "rpriv.pem";
		final String SEPubLocation = UserFactory.getUserHome() + System.getProperty("file.separator") + ".alien" + System.getProperty("file.separator") + "authen"
				+ System.getProperty("file.separator") + "rpub.pem";

		RSAPrivateKey authenPrivKey = null;
		RSAPublicKey authenPubKey = null;
		RSAPrivateKey sePrivKey = null;
		RSAPublicKey sePubKey = null;

		try {
			authenPrivKey = (RSAPrivateKey) JAKeyStore.loadPrivX509(AuthenPrivLocation, null);

			authenPubKey = (RSAPublicKey) JAKeyStore.loadPubX509(AuthenPubLocation, false)[0].getPublicKey();
		}
		catch (@SuppressWarnings("unused") final Throwable t) {
			// ignore
		}

		try {
			sePrivKey = (RSAPrivateKey) JAKeyStore.loadPrivX509(SEPrivLocation, null);

			sePubKey = (RSAPublicKey) JAKeyStore.loadPubX509(SEPubLocation, false)[0].getPublicKey();
		}
		catch (@SuppressWarnings("unused") final Throwable t) {
			// ignore
		}

		final RSAPrivateKey AuthenPrivKey = authenPrivKey;
		final RSAPublicKey AuthenPubKey = authenPubKey;
		final RSAPrivateKey SEPrivKey = sePrivKey;
		final RSAPublicKey SEPubKey = sePubKey;

		// String ticket = "<authz>\n <file>\n"
		// + " <access>read</access>\n"
		// +
		// " <turl>root://voalice16.cern.ch:1094//02/44930/de81d1c8-2e18-11e0-b66a-001cc4624d66</turl>\n"
		// + " <lfn>/alice/cern.ch/user/s/sschrein/jtest</lfn>\n"
		// + " <size>9096</size>\n"
		// + " <se>ALICE::CERN::ALICEDISK</se>\n"
		// + " <guid>DE81D1C8-2E18-11E0-B66A-001CC4624D66</guid>\n"
		// + " <md5>e73f3a05b652affbf22ce7b1128c1869</md5>\n"
		// + " <pfn>/02/44930/de81d1c8-2e18-11e0-b66a-001cc4624d66</pfn>\n"
		// + " </file>\n</authz>\n";

		final String ticket = "<authz>\n  <file>\n" + "    <access>read</access>\n" + "    <turl>root://pcepalice11.cern.ch:1094//tmp/xrd/00/19194/02bbaa0a-2e32-11e0-b69a-001e0b24002f</turl>\n"
				+ "    <lfn>/pcepalice11/user/a/admin/juduididid</lfn>\n" + "    <size>72624</size>\n" + "    <se>pcepalice11::CERN::XRD</se>\n"
				+ "    <guid>02BBAA0A-2E32-11E0-B69A-001E0B24002F</guid>\n" + "    <md5>21c88efc53d16fbaa6543955de92a7c7</md5>\n"
				+ "    <pfn>/tmp/xrd/00/19194/02bbaa0a-2e32-11e0-b69a-001e0b24002f</pfn>\n" + "  </file>\n</authz>\n";

		System.out.println("About to be encrypted: " + ticket);

		try {
			final EncryptedAuthzToken enAuthz = new EncryptedAuthzToken(AuthenPrivKey, SEPubKey, false);

			final String enticket = enAuthz.seal(new XrootDEnvelope(ticket));

			System.out.println("We encrypted: " + enticket);

			final EncryptedAuthzToken deAuthz = new EncryptedAuthzToken(SEPrivKey, AuthenPubKey, true);

			final String deticket = deAuthz.unseal(enticket);

			System.out.println("We decrypted: " + deticket);
			System.out.println();
			System.out.println();
			System.out.println();
			System.out.println();

			final String enticket2 = "-----BEGIN SEALED CIPHER-----\n" + "TIwz7t9qfChJ7UL9SxhbBHI2ELRhmacz9O1Dk-7XZLaR3qttvhyA9HmSTAslQ1qysF4vMyGjpqtM\n"
					+ "WmfZ-1OTG81vB4Jqwxs0M+v4oKV2w1SZH7PXZuRyF94KV1LY8oss4Jba13iiLEvCycGD+3bulQMQ\n" + "STySEtZmJUATpS0meTw=\n" + "-----END SEALED CIPHER-----\n"
					+ "-----BEGIN SEALED ENVELOPE-----\n" + "AAAAgHUrrqgqol7ch7OX0FonFfOGw+8WQ6zciO6oGm5FUr3nimQMIAeMN7s8rO1H-EBK16W6XzqZ\n"
					+ "uMeHCbFfgIeqdn2MiZPn9wd1Lm-eFi3ETIVlNoidvaT6BmiJofe0IsWYIcON2D5rCHj7c-tklBXF\n" + "um1mkwPyM0TJn-Z9yUSFFDTJ4u7payLCPygEPixG2zqGGvsQuyVwWs4OexftWea72CYTVr0xj5Sk\n"
					+ "jIjljPYPXifMfWCqw3BOzpYr582WE5FQyBjxFRngSA+XDGmOZ0VRyL0xAlMbBoORjc7bf0jBJKkO\n" + "sN4thdxPROt12VWSoH2xpY8zu1qnbW4YIvUC8LCPyzj1z9s3uXTSl2dBRxoHnNqKCZI3dpPi311p\n"
					+ "qMyg-XgrvJkUt5JLTHmCTrID6OpCiEwuN0f4TmxlP+GXH7tmtuo-nbdWTx7fQmT5sHzo9htjaffv\n" + "fsfXK5J7YTjdk1SuLhwOWaBAxee49l5+Y43AMf2zS41kO0w+Ccat3h9UOje1gtdA4TePuMxwH9YL\n"
					+ "YL5MTKLf18y-UFrtnFey9IJ-HE2FhTC2ibwKorEExciBlJYjhRhnivGT+psMZ04rNdyK1fygHZmk\n" + "lbcV1R5-rIpkkJcAmJ0C2JSm0OhW99QM8WwtgA8UsZT5yp3du-Mqn3myL8k6WkGeCTc2iEgUYTXC\n"
					+ "tqaESC8R6B8BRSkpMjjA8qHXw5YenWlxPcdOe4RPF4XBSFy1ZSFwA30+jnFYOA1dgHIcNbNv+HcE\n" + "DX9QiQEjbh4Tbe81PLsosbKBhgatAzVrgVHdjD--cpKEQ-ZNT24gfeUEvZRO+uVcWcEEsylWnLXP\n"
					+ "VcQlSuiZcTKFr-JA+NtJ1xUmPl-Z-7JfOWNl8X+pGpNKywyKE9+GYKonOfcuQSUHVnh9Xc5MD1qA\n" + "eyDmkY3lyo9r5K2Puo32Vj8FvFyU8fBDplbm+9HuOTyZ2U4il0sfh7iH0Yzq3YQ7tgVzWtjm-xNi\n"
					+ "H+GgEAtdAKjUfBiumgr92jkM5oPxpG3DB2Ze9WYFc+ZwdL5U2uHxcsJGbS6gS8+gWagA6424QdSf\n" + "WBKaVAL+L4YazZGOerhfTc8Mco2cQw==\n" + "-----END SEALED ENVELOPE-----";

			System.out.println("We gonne decrypt perl envelope: " + enticket2);

			final EncryptedAuthzToken deAuthz2 = new EncryptedAuthzToken(SEPrivKey, AuthenPubKey, true);

			final String deticket2 = deAuthz2.unseal(enticket2);

			System.out.println("We decrypted: " + deticket2);

			final String t5 = "-----BEGIN SEALED CIPHER-----\n" + "T3PzT0pfDZiA6slhrR4QVqcrIOb8XK7ZsYmjbc-qFiV5LhJ4fqAq1WtzXZjXJhT4Y5b-N3SGJZdA\n"
					+ "rOeVe6rdssMqmTVjhv2dl0p5diC00hxuArAKznV728mclkKXwSpTF9KJH914NWhALvDCj7NPaHUy\n" + "iZ2I9ZAy6ZcbCGzX3x0=\n" + "-----END SEALED CIPHER-----\n"
					+ "-----BEGIN SEALED ENVELOPE-----\n" + "AAAAgJqyuaPd4vCiFdxrkM1zHpOTnyKRk8bvWwe8xC+yS0QVDrXj5iK8uXSJftezaXtH9TAVJgMm\n"
					+ "j8OLhkwxDVimTeZ+Ru6Fz0LkWgubjsJa8I2sP75p0uNhOtC4Ch4GTZgHwL0vz4PMrPyuV6T4hl7j\n" + "MLpl9u63XTekV1RFhNVVdBtnYzZXpjPcb1Hjd2KeiisyZZ8ul27xSaIx9+SRhylPufQ3yq7wCgI6\n"
					+ "iNadD+3P0GkWPNRqMRi4eMbzBLCMvNSrrk6K4wC23n2Foao-fsLLntO4pXHs4GT27vRhaR7W+XYS\n" + "WwxY-hHpnZtZFWUhFlUEdlewkV-VbrnOnfo+MUR1QXQ7khTPEnTTVirzq-Bg-Epv9qWHGWgnTlny\n"
					+ "5OBgcV6pR-vUj2Pmmw7W10xVBJa8R0XcZt2FfGHnR3XGCnH2XcAUTATUW5kPkh3tX9xw33eDXHMC\n" + "FexczgkJKbiF8f24k6hEFJ3hZdNUWMXRV9WLkcpF9514j33K8i9p3KYhuO5fYGa0etJecVGeCohA\n"
					+ "jLMEIqvVPqjqxIApR8hGq5sdSsXxnSYvQ2BLf4GPtGOEvBcLz3V1zWA8jRmyWFc8Uy3csFFx+jEG\n" + "wOBVNEw0sf8d2r7NB1WeXndae+c2Xgei8kcQ2N6QD6UgGOLmt+DIyzWKR0iOP19FMOSU1jc7Dg-R\n"
					+ "F89cFAgCLUwvkTV8ESdSLeKlxaOtNtavD8oqliPMJBjVP5H-KdPbPvzIdom-DxsQmomq8-y95MD3\n" + "-FWcq0NDxkPzVIqeJlyLYQ1F7B8tyUOPAhkZE0pt89YvTq5NGSGqr79nc5XH-J9ZN7IZrUB508A2\n"
					+ "2eHfEbNIk3qAIOUCDDe7jDUq97Ce0sFlLzWJoyzFAkZ62ifS2MpCE2RN9QZWeAazk+7uOToS4DQ6\n" + "GbaNLSxKws4cWqVFGJ4q4CF1bhyUrljsVwaTUj7a7wlnjRDC51VfHTOokwiBMdLCrPGb9qkSwNBU\n"
					+ "NkQMLNucTPz-z-18q4u+wybWbPGjrqy2+ZB6Y3i6z6N1lEXyH0rc\n" + "-----END SEALED ENVELOPE-----";

			System.out.println();
			System.out.println();
			System.out.println();
			System.out.println();
			System.out.println("We gonne THIS decrypt perl envelope: " + t5);

			final EncryptedAuthzToken deAuthz3 = new EncryptedAuthzToken(SEPrivKey, AuthenPubKey, true);

			final String deticket5 = deAuthz3.unseal(t5);

			System.out.println("We decrypted: " + deticket5);

		}
		catch (final GeneralSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
