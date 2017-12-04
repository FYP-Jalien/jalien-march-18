package alien.api.aaa;

import java.io.IOException;
import java.io.StringWriter;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAmount;
import java.util.logging.Logger;

import org.bouncycastle.openssl.jcajce.JcaPEMWriter;

import alien.api.Request;
import alien.catalogue.access.AuthorizationFactory;
import alien.config.ConfigUtils;
import alien.user.AliEnPrincipal;
import io.github.olivierlemasle.ca.CA;
import io.github.olivierlemasle.ca.Certificate;
import io.github.olivierlemasle.ca.CsrWithPrivateKey;
import io.github.olivierlemasle.ca.DistinguishedName;
import io.github.olivierlemasle.ca.DnBuilder;
import io.github.olivierlemasle.ca.RootCertificate;

/**
 * Get a limited duration (token) certificate for users to authenticate with
 *
 * @author costing
 * @since 2017-07-04
 */
public class GetTokenCertificate extends Request {
	private static final long serialVersionUID = 7799371357160254760L;

	private static final RootCertificate rootCert;
	static transient final Logger logger = ConfigUtils.getLogger(AuthorizationFactory.class.getCanonicalName());

	static {
		if (ConfigUtils.isCentralService()) {
			final String caFile = ConfigUtils.getConfig().gets("ca.file",
					System.getProperty("user.home") + System.getProperty("file.separator") + ".globus" + System.getProperty("file.separator") + "alien.p12");

			final String caAlias = ConfigUtils.getConfig().gets("ca.alias", "alien");

			final String caPassword = ConfigUtils.getConfig().gets("ca.password");

			RootCertificate rootCertTemp = null;

			try {
				rootCertTemp = CA.loadRootCertificate(caFile, caPassword.toCharArray(), caAlias);
			} catch (final Throwable t) {
				System.err.println("Exception loading root CA certificate from " + caFile + " (alias " + caAlias + "), password '" + caPassword + "'");
				System.err.println(t.getMessage());
				t.printStackTrace();
			}

			rootCert = rootCertTemp;
		}
		else
			rootCert = null;
	}

	// outgoing fields
	final TokenCertificateType certificateType;
	final String extension;
	final int validity;
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
	public void run() {
		if (certificateType == null)
			throw new IllegalArgumentException("Certificate type cannot be null");

		DnBuilder builder = CA.dn().setC("ch").setO("AliEn");

		final String requester = getEffectiveRequester().getDefaultUser();
		final String requested = getEffectiveRequester().canBecome(requestedUser) ? requestedUser : requester;

		switch (certificateType) {
		case USER_CERTIFICATE:
			if (getEffectiveRequester().isJob() || getEffectiveRequester().isJobAgent())
				throw new IllegalArgumentException("You can't request a User token as JobAgent or Job");

			builder = builder.setCn("Users").setCn(requester).setOu(requested);
			break;
		case JOB_TOKEN:
			if (!getEffectiveRequester().isJobAgent())
				throw new IllegalArgumentException("Only a JobAgent can ask for a Job token");

			if (extension == null || extension.length() == 0)
				throw new IllegalArgumentException("Job token requires the job ID to be passed as certificate extension");

			builder = builder.setCn("Jobs").setCn(requester).setOu(requester);
			break;
		case JOB_AGENT_TOKEN:
			if (!getEffectiveRequester().canBecome("vobox"))
				throw new IllegalArgumentException("You don't have permissions to ask for a JobAgent token");

			builder = builder.setCn("JobAgent");
			break;
		default:
			throw new IllegalArgumentException("Sorry, what?");
		}

		if (extension != null && extension.length() > 0)
			builder = builder.setOu(extension);

		final DistinguishedName userDN = builder.build();

		final CsrWithPrivateKey csr = CA.createCsr().generateRequest(userDN);

		final TemporalAmount amount = Period.ofDays(validity <= 0 || validity > certificateType.getMaxValidity() ? 2 : validity);

		ZonedDateTime notAfter = ZonedDateTime.now().plus(amount);

		if (getEffectiveRequester().getUserCert() != null) {
			final ZonedDateTime userNotAfter = getEffectiveRequester().getUserCert()[0].getNotAfter().toInstant().atZone(ZoneId.systemDefault());

			if (notAfter.isAfter(userNotAfter))
				notAfter = userNotAfter;
		}
		else {
			throw new IllegalArgumentException("When issuing a user certificate you need to pass the current one, that will limit the validity of the issued token");
		}

		final javax.security.cert.X509Certificate partnerCertificateChain[] = getPartnerCertificate();

		if (partnerCertificateChain != null)
			for (final javax.security.cert.X509Certificate partner : partnerCertificateChain) {
				final ZonedDateTime partnerNotAfter = partner.getNotAfter().toInstant().atZone(ZoneId.systemDefault());

				if (notAfter.isAfter(partnerNotAfter))
					notAfter = partnerNotAfter;
			}

		final Certificate cert = rootCert.signCsr(csr).setRandomSerialNumber().setNotAfter(notAfter).sign();

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
		} catch (final IOException e) {
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
