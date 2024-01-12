package alien.api.token;

import java.io.IOException;
import java.io.StringWriter;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAmount;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;

import alien.api.Request;
import alien.catalogue.access.AuthorizationFactory;
import alien.config.ConfigUtils;
import alien.user.AliEnPrincipal;
import alien.user.LDAPHelperRemote;
import alien.user.UserFactory;
import io.github.olivierlemasle.ca.CA;
import io.github.olivierlemasle.ca.Certificate;
import io.github.olivierlemasle.ca.CsrWithPrivateKey;
import io.github.olivierlemasle.ca.DistinguishedName;
import io.github.olivierlemasle.ca.DnBuilder;
import io.github.olivierlemasle.ca.RootCertificate;
import io.github.olivierlemasle.ca.Signer.SignerWithSerial;
import io.github.olivierlemasle.ca.ext.CertExtension;
import io.github.olivierlemasle.ca.ext.ExtKeyUsageExtension;
import io.github.olivierlemasle.ca.ext.KeyUsageExtension;
import io.github.olivierlemasle.ca.ext.KeyUsageExtension.KeyUsage;

/**
 * Get a limited duration (token) certificate for users to authenticate with
 *
 * @author costing
 * @since 2017-07-04
 */
public class GetTokenCertificate extends Request {
	private static final long serialVersionUID = 7799371357160254760L;

	private static final RootCertificate rootCert;

	/**
	 * Logging component
	 */
	static transient final Logger logger = ConfigUtils.getLogger(AuthorizationFactory.class.getCanonicalName());

	static {
		if (ConfigUtils.isCentralService()) {
			final String caFile = ConfigUtils.getConfig().gets("ca.file",
					UserFactory.getUserHome() + System.getProperty("file.separator") + ".globus" + System.getProperty("file.separator") + "alien.p12");

			final String caAlias = ConfigUtils.getConfig().gets("ca.alias", "alien");

			final String caPassword = ConfigUtils.getConfig().gets("ca.password");

			RootCertificate rootCertTemp = null;

			try {
				rootCertTemp = CA.loadRootCertificate(caFile, caPassword.toCharArray(), caAlias);
			}
			catch (final Throwable t) {
				System.err.println("Exception loading root CA certificate from " + caFile + " (alias " + caAlias + "), password '" + caPassword + "'");
				System.err.println(t.getMessage());
				t.printStackTrace();
			}

			rootCert = rootCertTemp;
		}
		else
			rootCert = null;
	}

	/**
	 * Get AliEn CA certificate
	 *
	 * @return AliEn CA certificate
	 */
	public static X509Certificate getRootPublicKey() {
		if (rootCert != null)
			return rootCert.getX509Certificate();

		return null;
	}

	// outgoing fields
	/**
	 * Which type of certificate is requested
	 */
	final TokenCertificateType certificateType;

	/**
	 * Requested extension to it (job ID in particular)
	 */
	final String extension;

	/**
	 * Requested validity. It's just a hint, function of the certificate type the validity is limited to 2 days (job agent and jobs) or one month (user tokens). But see {@link TokenCertificateType}
	 * for the current limits imposed to them.
	 */
	final int validity;

	/**
	 * User requesting this operation
	 */
	final String requestedUser;

	// incoming fields

	private X509Certificate certificate = null;
	private PrivateKey privateKey = null;

	/**
	 * Create a token certificate request for a specific user and role plus the
	 * other required fields
	 *
	 * @param user
	 * @param requestedUser
	 * @param certificateType
	 * @param extension
	 * @param validity
	 *            the certificate the user presented to identify itself. This
	 *            will restrict the validity of the issued token
	 */
	public GetTokenCertificate(final AliEnPrincipal user, final String requestedUser, final TokenCertificateType certificateType, final String extension, final int validity) {
		setRequestUser(user);

		this.certificateType = certificateType;
		this.extension = extension;
		this.validity = validity;
		this.requestedUser = requestedUser;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(this.certificateType.toString(), this.extension, String.valueOf(this.validity), this.requestedUser);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		if (certificateType == null)
			throw new IllegalArgumentException("Certificate type cannot be null");

		DnBuilder builder = CA.dn().setC("ch").setO("AliEn2");

		final String requester = getEffectiveRequester().getDefaultUser();

		final boolean isAdmin = getEffectiveRequester().canBecome("admin");

		final ExtKeyUsageExtension extKeyUsage;

		CertExtension san = null;

		switch (certificateType) {
			case USER_CERTIFICATE:
				if (getEffectiveRequester().isJob() || getEffectiveRequester().isJobAgent())
					throw new IllegalArgumentException("You can't request a User token as JobAgent or Job");

				final String requested = getEffectiveRequester().canBecome(requestedUser) ? requestedUser : requester;

				final String cn;

				// if an admin requests a user token for a role that it doesn't directly have, switch to that identity
				if (isAdmin && !getEffectiveRequester().hasRole(requested))
					cn = requested;
				else
					cn = requester;

				builder = builder.setCn("Users").setCn(cn).setOu(requested);

				// User token can be used as both client identity and local (JBox) server identity
				// CERN also adds "E-mail Protection" and "Microsoft Encrypted File System" (the later one doesn't have a corresponding value yet)
				extKeyUsage = ExtKeyUsageExtension.create(KeyPurposeId.id_kp_clientAuth, KeyPurposeId.id_kp_serverAuth, KeyPurposeId.id_kp_emailProtection);

				/*
				 * Set the email addresses known for this user as extensions, similar to what CERN does (though the actual extension is different, i.e.:
				 *
				 * (ours)
				 * X509v3 Subject Alternative Name:
				 * email:Costin.Grigoras@cern.ch
				 *
				 * (CERN)
				 * X509v3 Subject Alternative Name:
				 * othername:<unsupported>, email:Costin.Grigoras@cern.ch
				 */
				final Set<String> emailAddresses = LDAPHelperRemote.getEmails(requested);

				final GeneralName[] nameArray = new GeneralName[4 + (emailAddresses != null ? emailAddresses.size() : 0)];

				// Token certificates can be used by JBox to listen on, so only localhost should validate
				nameArray[0] = new GeneralName(GeneralName.dNSName, "localhost");
				nameArray[1] = new GeneralName(GeneralName.dNSName, "localhost.localdomain");
				nameArray[2] = new GeneralName(GeneralName.dNSName, "127.0.0.1");
				nameArray[3] = new GeneralName(GeneralName.dNSName, "::1");

				if (emailAddresses != null) {
					int idx = 4;

					for (final String email : emailAddresses)
						nameArray[idx++] = new GeneralName(GeneralName.rfc822Name, email);

					san = new CertExtension(Extension.subjectAlternativeName, false, new GeneralNames(nameArray));
				}

				break;
			case JOB_TOKEN:
				if (!getEffectiveRequester().isJobAgent() && !isAdmin)
					throw new IllegalArgumentException("Only a JobAgent can ask for a Job token, " + getEffectiveRequester() + " is not one");

				if (extension == null || extension.length() == 0)
					throw new IllegalArgumentException("Job token requires the job ID to be passed as certificate extension");

				// A JobWrapper must act as both client (in interacting with upstream services) and server (WebSocketS towards the payload)
				extKeyUsage = ExtKeyUsageExtension.create(KeyPurposeId.id_kp_clientAuth, KeyPurposeId.id_kp_serverAuth);

				// Payload runs on the same machine, advertise only localhost as acceptable target for clients
				final GeneralNames names = new GeneralNames(new GeneralName[] {
						new GeneralName(GeneralName.dNSName, "localhost"),
						new GeneralName(GeneralName.dNSName, "localhost.localdomain"),
						new GeneralName(GeneralName.dNSName, "127.0.0.1"),
						new GeneralName(GeneralName.dNSName, "::1") });

				san = new CertExtension(Extension.subjectAlternativeName, false, names);

				builder = builder.setCn("Jobs").setCn(requestedUser).setOu(requestedUser);
				break;
			case JOB_AGENT_TOKEN:
				if (!getEffectiveRequester().canBecome("vobox"))
					throw new IllegalArgumentException("You don't have permissions to ask for a JobAgent token");

				// A JobAgent is client only, uses the certificate to request an actual job
				extKeyUsage = ExtKeyUsageExtension.create(KeyPurposeId.id_kp_clientAuth);

				builder = builder.setCn("JobAgent");
				break;
			case HOST:
				if (!isAdmin)
					throw new IllegalArgumentException("Only admin can do that");

				// Central service or VoBox should be able to act as both client and server
				extKeyUsage = ExtKeyUsageExtension.create(KeyPurposeId.id_kp_clientAuth, KeyPurposeId.id_kp_serverAuth);

				final GeneralNames hostName = new GeneralNames(new GeneralName(GeneralName.dNSName, extension));
				san = new CertExtension(Extension.subjectAlternativeName, false, hostName);

				builder = builder.setOu("ALICE");
				break;
			default:
				throw new IllegalArgumentException("Sorry, what?");
		}

		if (extension != null && extension.length() > 0)
			if (certificateType == TokenCertificateType.HOST)
				builder = builder.setCn(extension);
			else
				builder = builder.setOu(extension);

		final DistinguishedName userDN = builder.build();

		final CsrWithPrivateKey csr = CA.createCsr().generateRequest(userDN);

		int actualValidity = validity;

		if (actualValidity <= 0)
			actualValidity = 2;

		if (actualValidity > certificateType.getMaxValidity())
			if (isAdmin)
				if (actualValidity > 400) {
					logger.log(Level.INFO, "An admin has requested a validity of " + actualValidity + " days for " + userDN + ", shrinking it to 400 days only");
					actualValidity = 400;
				}
				else
					logger.log(Level.INFO, "An admin has requested a validity of " + actualValidity + " days for " + userDN + ", granting it");
			else {
				logger.log(Level.WARNING, requester + " has requested a validity of " + actualValidity + " days for " + userDN + ", shrinking it to 2 days");
				actualValidity = 2;
			}

		final TemporalAmount amount = Period.ofDays(actualValidity);

		ZonedDateTime notAfter = ZonedDateTime.now().plus(amount);

		if (!isAdmin || (certificateType != TokenCertificateType.HOST && certificateType != TokenCertificateType.USER_CERTIFICATE)) {
			// Admin can generate host and user certificates unbound by its own certificate duration, anything else is checked against requester's identity
			if (getEffectiveRequester().getUserCert() != null) {
				final ZonedDateTime userNotAfter = getEffectiveRequester().getUserCert()[0].getNotAfter().toInstant().atZone(ZoneId.systemDefault());

				if (notAfter.isAfter(userNotAfter)) {
					logger.log(Level.WARNING,
							"Restricting the validity of " + userDN + " to " + userNotAfter + " since the requested validity of " + notAfter + " is beyond requester's identity expiration time");
					notAfter = userNotAfter;
				}
			}
			else {
				throw new IllegalArgumentException("When issuing a user certificate you need to pass the current one, that will limit the validity of the issued token");
			}

			// The validity is further constrained to the identity of the forwarding agent
			final java.security.cert.X509Certificate partnerCertificateChain[] = getPartnerCertificate();

			if (partnerCertificateChain != null)
				for (final java.security.cert.X509Certificate partner : partnerCertificateChain) {
					final ZonedDateTime partnerNotAfter = partner.getNotAfter().toInstant().atZone(ZoneId.systemDefault());

					if (notAfter.isAfter(partnerNotAfter)) {
						logger.log(Level.WARNING,
								"Restricting the validity of " + userDN + " to " + partnerNotAfter + " since the requested validity of " + notAfter + " is beyond partner's identity expiration time");
						notAfter = partnerNotAfter;
					}
				}
		}

		// Give a grace time of 2 hours to compensate for WNs that are running behind with the clock
		final ZonedDateTime notBefore = ZonedDateTime.now().minusHours(2);

		final SignerWithSerial signer = rootCert.signCsr(csr).setRandomSerialNumber().setNotAfter(notAfter).setNotBefore(notBefore).addExtension(extKeyUsage);

		final KeyUsageExtension usage = KeyUsageExtension.create(KeyUsage.DIGITAL_SIGNATURE, KeyUsage.KEY_ENCIPHERMENT, KeyUsage.NON_REPUDIATION);
		signer.addExtension(usage);

		if (san != null)
			signer.addExtension(san);

		final Certificate cert = signer.sign();

		certificate = cert.getX509Certificate();
		privateKey = csr.getPrivateKey();
	}

	/**
	 * Get the signed public key of the user token certificate
	 *
	 * @return public key
	 */
	public X509Certificate getCertificate() {
		return certificate;
	}

	/**
	 * Get the signed public key of the user token certificate in PEM format
	 *
	 * @return the PEM formatted public key
	 */
	public String getCertificateAsString() {
		return convertToPEM(certificate);
	}

	/**
	 * Get the private key of the user token certificate pair
	 *
	 * @return the private key
	 */
	public PrivateKey getPrivateKey() {
		return privateKey;
	}

	/**
	 * Get the private key of the user token certificate pair in PEM format
	 *
	 * @return the PEM formatted private key
	 */
	public String getPrivateKeyAsString() {
		return convertToPEM(privateKey);
	}

	/**
	 * Helper function to convert any security object to String
	 *
	 * @param securityObject
	 *            one of X509Certificate or PrivateKey object types
	 * @return the PEM representation of the given object
	 */
	public static final String convertToPEM(final Object securityObject) {
		if (securityObject == null)
			return null;

		final StringWriter sw = new StringWriter(2000);
		try (JcaPEMWriter writer = new JcaPEMWriter(sw)) {
			writer.writeObject(securityObject);
		}
		catch (final IOException e) {
			e.printStackTrace();
		}
		return sw.toString();
	}
	/*
	 * @Override
	 * public String getKey() {
	 * // only cache user tokens, job tokens have the job ID in them and cannot
	 * // be effectively cached
	 * if (certificateType == TokenCertificateType.USER_CERTIFICATE)
	 * return getEffectiveRequester().getName();
	 *
	 * return null;
	 * }
	 *
	 * @Override
	 * public long getTimeout() {
	 * // for the same user don't generate another certificate for 10 minutes
	 * // but return the same one
	 * return 1000L * 60 * 10;
	 * }
	 */
}
