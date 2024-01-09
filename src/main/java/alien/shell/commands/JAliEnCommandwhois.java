package alien.shell.commands;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import alien.shell.ErrNo;
import alien.user.AliEnPrincipal;
import alien.user.LDAPHelperRemote;
import alien.user.UserFactory;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import lazyj.Format;

/**
 * @author costing
 * @since 2018-09-11
 */
public class JAliEnCommandwhois extends JAliEnBaseCommand {

	private boolean search = false;

	private boolean fullNameSearch = false;

	private boolean emailSearch = false;

	private boolean dnSearch = false;

	private final List<String> searchFor = new ArrayList<>();

	/**
	 * execute the whois
	 */
	@Override
	public void run() {
		final Set<String> usernames = new TreeSet<>();

		for (final String s : searchFor)
			if (search || fullNameSearch || emailSearch || dnSearch) {
				String searchQuery = "(uid=*" + s + "*)";

				if (fullNameSearch)
					searchQuery = "(|" + searchQuery + "(gecos=*" + s + "*)(cn=*" + s + "*))";

				if (emailSearch)
					if (searchQuery.endsWith("))"))
						searchQuery = searchQuery.substring(0, searchQuery.length() - 1) + "(email=*" + s + "*))";
					else
						searchQuery = "(|" + searchQuery + "(email=*" + s + "*))";

				if (dnSearch)
					if (searchQuery.endsWith("))"))
						searchQuery = searchQuery.substring(0, searchQuery.length() - 1) + "(subject=*" + s + "*))";
					else
						searchQuery = "(|" + searchQuery + "(subject=*" + s + "*))";

				final Set<String> uids = LDAPHelperRemote.checkLdapInformation(searchQuery, "ou=People,", "uid");

				if (uids != null)
					usernames.addAll(uids);
			}
			else {
				usernames.add(s);
			}

		for (final String s : usernames) {
			final AliEnPrincipal principal = UserFactory.getByUsername(s);

			if (principal == null)
				commander.printOutln("Username not found: " + s);
			else
				printUser(principal);
		}
	}

	private void printUser(final AliEnPrincipal principal) {
		commander.printOutln("Username: " + principal.getName());
		commander.printOut("username", principal.getName());

		Set<String> names = LDAPHelperRemote.checkLdapInformation("uid=" + principal.getName(), "ou=People,", "gecos");

		if (names == null)
			names = LDAPHelperRemote.checkLdapInformation("uid=" + principal.getName(), "ou=People,", "cn");

		printCollection("Full name", names, false);

		printCollection("Roles", principal.getRoles(), false);

		printCollection("Email", LDAPHelperRemote.getEmails(principal.getName()), true);

		printCollection("Subject", LDAPHelperRemote.checkLdapInformation("uid=" + principal.getName(), "ou=People,", "subject"), true);

		commander.printOutln();
		commander.outNextResult();
	}

	private void printCollection(final String key, final Collection<?> collection, final boolean newlines) {
		if (collection != null && collection.size() > 0) {
			commander.printOut("  " + key + ": ");

			boolean first = true;

			if (newlines && collection.size() > 1)
				first = false;

			final StringBuilder sb = new StringBuilder();

			for (final Object o : collection) {
				if (!first)
					commander.printOut(newlines ? "\n    " : ", ");

				if (sb.length() > 0)
					sb.append(", ");

				sb.append(o.toString());

				commander.printOut(o.toString());

				first = false;
			}

			commander.printOut(Format.replace(key.toLowerCase(), " ", "_"), sb.toString());

			commander.printOutln();
		}
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("whois", "[account name]"));
		commander.printOutln(helpStartOptions());
		commander.printOutln(helpOption("-s", "search for the given string(s) in usernames"));
		commander.printOutln(helpOption("-f", "also search in full names"));
		commander.printOutln(helpOption("-e", "search in email addresses too"));
		commander.printOutln(helpOption("-d", "search in X509 DN (subject) fields"));
		commander.printOutln(helpOption("-a", "search for the given string in all the above fields"));

		commander.printOutln();
	}

	/**
	 * whois cannot run without arguments
	 *
	 * @return <code>false</code>
	 */
	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * Constructor needed for the command factory in JAliEnCOMMander
	 *
	 * @param commander
	 *
	 * @param alArguments
	 *            the arguments of the command
	 * @throws OptionException
	 */
	public JAliEnCommandwhois(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);
		try {
			final OptionParser parser = new OptionParser();

			parser.accepts("h");
			parser.accepts("s");
			parser.accepts("f");
			parser.accepts("e");
			parser.accepts("d");
			parser.accepts("a");

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			if (options.has("h")) {
				printHelp();
				return;
			}

			search = options.has("s") || options.has("a");
			fullNameSearch = options.has("f") || options.has("a");
			emailSearch = options.has("e") || options.has("a");
			dnSearch = options.has("d") || options.has("a");

			for (final Object o : options.nonOptionArguments())
				searchFor.add(o.toString());

			if (searchFor.size() == 0)
				setArgumentsOk(false);
		}
		catch (final OptionException | IllegalArgumentException e) {
			commander.setReturnCode(ErrNo.EINVAL, e.getMessage());
			setArgumentsOk(false);
		}
	}
}
