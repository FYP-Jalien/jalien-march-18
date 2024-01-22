package alien.shell.commands;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import alien.shell.ErrNo;
import alien.user.AliEnPrincipal;
import alien.user.LDAPHelperRemote;
import alien.user.UsersHelper;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import lazyj.Format;

/**
 * Some info about the current identity
 *
 * @author costing
 * @since Aug 17, 2020
 */
public class JAliEnCommandwhoami extends JAliEnBaseCommand {

	private boolean verbose = false;

	@Override
	public void run() {
		final AliEnPrincipal user = commander.getUser();

		// all other current user info is available to the API only
		commander.printOut("username", user.getName());
		commander.printOut("role", user.getDefaultRole());
		commander.printOut("roles", String.join(",", user.getRoles()));

		if (verbose) {
			commander.printOutln("Username: " + user.getName());

			final ArrayList<String> otherRoles = new ArrayList<>(user.getRoles());

			otherRoles.remove(user.getDefaultRole());

			commander.printOut("Role: " + user.getDefaultRole());

			if (otherRoles.size() > 0)
				commander.printOut(" (other roles: " + String.join(", ", otherRoles) + ")");

			commander.printOutln();
		}
		else
			// simple shell printout
			commander.printOutln(user.getName());

		Set<String> names = LDAPHelperRemote.checkLdapInformation("uid=" + user.getName(), "ou=People,", "gecos");

		if (names == null)
			names = LDAPHelperRemote.checkLdapInformation("uid=" + user.getName(), "ou=People,", "cn");

		if (names != null && names.size() > 0) {
			if (verbose)
				commander.printOutln("Full name: " + String.join(", ", names));

			commander.printOut("fullname", String.join(",", names));
		}

		final Set<String> emails = LDAPHelperRemote.getEmails(user.getName());

		if (emails != null && emails.size() > 0) {
			if (verbose)
				commander.printOutln("Email: " + String.join(", ", emails));

			commander.printOut("email", String.join(",", emails));
		}

		if (user.getUserCert() != null) {
			final ZonedDateTime userNotAfter = user.getUserCert()[0].getNotAfter().toInstant().atZone(ZoneId.systemDefault());

			commander.printOut("certificate_expires", String.valueOf(userNotAfter.toEpochSecond()));
			commander.printOut("certificate_dn", user.getUserCert()[0].getSubjectDN().toString());

			if (verbose)
				commander.printOutln("Certificate DN: " + user.getUserCert()[0].getSubjectDN().toString() + " (expiring on " + userNotAfter + ", which is in "
						+ Format.toInterval(userNotAfter.toEpochSecond() * 1000L - System.currentTimeMillis()) + ")");
		}

		if (user.getRemoteEndpoint() != null) {
			commander.printOut("connected_from", user.getRemoteEndpoint().getHostAddress());

			if (verbose)
				commander.printOutln("Connected from: " + user.getRemoteEndpoint().getHostAddress());
		}

		final String userHomeDir = UsersHelper.getHomeDir(user.getName());

		if (userHomeDir != null) {
			commander.printOut("homedir", userHomeDir);

			if (verbose)
				commander.printOutln("Home directory: " + userHomeDir);
		}
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("whoami", ""));
		commander.printOutln(helpStartOptions());
		commander.printOutln(helpOption("-v", "verbose details of the current identity"));
	}

	/**
	 * this command can run without arguments
	 *
	 * @return <code>true</code>
	 */
	@Override
	public boolean canRunWithoutArguments() {
		return true;
	}

	/**
	 * Constructor needed for the command factory in commander
	 *
	 * @param commander
	 *
	 * @param alArguments
	 *            the arguments of the command
	 */
	public JAliEnCommandwhoami(final JAliEnCOMMander commander, final List<String> alArguments) {
		super(commander, alArguments);

		try {
			final OptionParser parser = new OptionParser();

			parser.accepts("v");

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			verbose = options.has("v");
		}
		catch (final OptionException | IllegalArgumentException e) {
			commander.setReturnCode(ErrNo.EINVAL, e.getMessage());
			setArgumentsOk(false);
		}
	}
}
