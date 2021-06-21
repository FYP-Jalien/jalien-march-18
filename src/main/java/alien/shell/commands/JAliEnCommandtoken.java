package alien.shell.commands;

import java.net.InetAddress;
import java.util.List;
import java.util.logging.Level;

import alien.api.Dispatcher;
import alien.api.ServerException;
import alien.api.token.GetTokenCertificate;
import alien.api.token.TokenCertificateType;
import alien.shell.ErrNo;
import alien.user.AliEnPrincipal;
import alien.user.UserFactory;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * @author vyurchen
 *
 */
public class JAliEnCommandtoken extends JAliEnBaseCommand {

	private TokenCertificateType tokentype = TokenCertificateType.USER_CERTIFICATE;
	private String requestedUser = null; // user1 can ask for token for user2
	private int validity; // Default validity depends on token type
	private String extension = null; // Token extension (jobID for job tokens)

	/**
	 * @param commander
	 * @param alArguments
	 */
	public JAliEnCommandtoken(final JAliEnCOMMander commander, final List<String> alArguments) {
		super(commander, alArguments);

		try {
			final OptionParser parser = new OptionParser();

			parser.accepts("u").withRequiredArg();
			parser.accepts("jobid").withRequiredArg();
			parser.accepts("v").withRequiredArg().ofType(Integer.class);
			parser.accepts("t").withRequiredArg();
			parser.accepts("hostname").withRequiredArg();
			parser.accepts("f");

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			if (options.has("u"))
				requestedUser = (String) options.valueOf("u");

			// set token type defaults if hostname or jobid was passed as option, don't require '-t' explicitly
			if (options.has("hostname"))
				tokentype = TokenCertificateType.HOST;

			if (options.has("jobid"))
				tokentype = TokenCertificateType.JOB_TOKEN;

			if (options.has("t")) {
				switch ((String) options.valueOf("t")) {
					case "job":
						tokentype = TokenCertificateType.JOB_TOKEN;
						break;
					case "jobagent":
						tokentype = TokenCertificateType.JOB_AGENT_TOKEN;
						break;
					case "host":
						tokentype = TokenCertificateType.HOST;
						break;
					case "user":
						tokentype = TokenCertificateType.USER_CERTIFICATE;
						break;
					default:
						commander.setReturnCode(ErrNo.EINVAL, "Unknown token type " + tokentype);
						setArgumentsOk(false);
						return;
				}
			}

			validity = tokentype.getMaxValidity();

			if (options.has("v"))
				validity = ((Integer) options.valueOf("v")).intValue();

			if (validity > tokentype.getMaxValidity() && !commander.getUser().canBecome("admin")) {
				commander.printErrln("Reducing the validity from " + validity + " to " + tokentype.getMaxValidity() + " as this is the max allowed value for this type of certificate");
				validity = tokentype.getMaxValidity();
			}

			if (tokentype == TokenCertificateType.JOB_TOKEN) {
				if (options.has("jobid"))
					extension = (String) options.valueOf("jobid");
				else {
					commander.setReturnCode(ErrNo.EINVAL, "You should pass a job extension for this type of certificate request");
					setArgumentsOk(false);
				}

				if (requestedUser == null) {
					commander.setReturnCode(ErrNo.EINVAL, "You also have to pass the user for which to run this job id");
					setArgumentsOk(false);
				}
			}

			if (tokentype == TokenCertificateType.HOST)
				if (options.has("hostname")) {
					extension = ((String) options.valueOf("hostname")).trim();

					if (extension.length() == 0 || !extension.contains(".")) {
						commander.setReturnCode(ErrNo.EINVAL, "Please pass a FQDN as hostname instead of `" + extension + "`");
						setArgumentsOk(false);
					}
					else
						try {
							final InetAddress[] addresses = InetAddress.getAllByName(extension);

							commander.printOut("Addresses of " + extension + ": ");

							for (InetAddress addr : addresses)
								commander.printOut(addr.getHostAddress() + " ");

							commander.printOutln();
						}
						catch (@SuppressWarnings("unused") final Throwable t) {
							if (options.has("f")) {
								commander.printErrln("The indicated hostname `" + extension + "` cannot be resolved! Generating the certificate as requested, but DNS should be fixed");
							}
							else {
								commander.setReturnCode(ErrNo.ENXIO, "hostname `" + extension + "` cannot be resolved");
								setArgumentsOk(false);
							}
						}
				}
				else {
					commander.setReturnCode(ErrNo.EINVAL, "You must indicate the hostname for which to issue the certificate!");
					setArgumentsOk(false);
				}
		}
		catch (final OptionException e) {
			commander.setReturnCode(ErrNo.EINVAL, e.getMessage());
			setArgumentsOk(false);
		}
	}

	@Override
	public void run() {
		GetTokenCertificate tokenreq = new GetTokenCertificate(commander.user, requestedUser, tokentype, extension, validity);

		try {
			tokenreq = Dispatcher.execute(tokenreq);
		}
		catch (final ServerException e1) {
			logger.log(Level.WARNING, "Cannot get the token you asked for", e1);
			commander.setReturnCode(ErrNo.EREMOTEIO, "Server didn't execute your request, reason was: " + e1.getMessage());
			return;
		}

		// Try to switch user
		final java.security.cert.X509Certificate[] cert = commander.user.getUserCert();
		final String defaultuser = commander.user.getDefaultUser();
		AliEnPrincipal switchUser;

		if (requestedUser != null) {
			if (commander.user.canBecome(requestedUser)) {
				if ((switchUser = UserFactory.getByUsername(requestedUser)) != null)
					commander.user = switchUser;
				else if ((switchUser = UserFactory.getByRole(requestedUser)) != null)
					commander.user = switchUser;
				else
					commander.setReturnCode(ErrNo.EINVAL, "User " + requestedUser + " cannot be found. Abort");

				commander.user.setUserCert(cert);
				commander.user.setDefaultUser(defaultuser);
			}
			else
				commander.setReturnCode(ErrNo.EPERM, "Switching user " + commander.user.getName() + " to [" + requestedUser + "] failed");
		}

		// Return tokens
		if (tokenreq != null) {
			commander.printOut("tokencert", tokenreq.getCertificateAsString());
			commander.printOut("tokenkey", tokenreq.getPrivateKeyAsString());

			final String dn = UserFactory.transformDN(tokenreq.getCertificate().getSubjectX500Principal().getName());
			commander.printOutln("DN: " + dn);

			if (tokentype == TokenCertificateType.HOST) {
				// Print a warning in case the VoBox hostnames are not associated to an account in LDAP
				final AliEnPrincipal account = UserFactory.getByDN(dn);

				if (account == null)
					commander.printOutln("  No user is mapped to this DN");
				else
					commander.printOutln("  Mapped to " + account.getDefaultUser());
			}

			commander.printOutln("Expires: " + tokenreq.getCertificate().getNotAfter());
			commander.printOutln();
			commander.printOut(tokenreq.getCertificateAsString());
			commander.printOutln();
			commander.printOut(tokenreq.getPrivateKeyAsString());
		}
		else
			commander.setReturnCode(ErrNo.EINVAL, "User " + requestedUser + " cannot be found. Abort");
	}

	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("token", "[-options]"));
		commander.printOutln(helpStartOptions());
		commander.printOutln(helpOption("-u <username>", "switch to another role of yours"));
		commander.printOutln(helpOption("-v <validity (days)>", "default depends on token type"));
		commander.printOutln(helpOption("-t <tokentype>", "can be one of: job, jobagent, host, user (default)"));
		commander.printOutln(helpOption("-jobid <job DN extension>", "expected to be present in a job token"));
		commander.printOutln(helpOption("-hostname <FQDN>", "required for a host certificate"));
		commander.printOutln();
	}

	@Override
	public boolean canRunWithoutArguments() {
		return true;
	}

}
