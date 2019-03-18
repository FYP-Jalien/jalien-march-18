package alien.shell.commands;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import alien.user.AliEnPrincipal;
import alien.user.LDAPHelper;
import alien.user.UserFactory;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * @author costing
 * @since 2018-09-11
 */
public class JAliEnCommandwhois extends JAliEnBaseCommand {

	private boolean search = false;

	private boolean fullNameSearch = false;

	private final List<String> searchFor = new ArrayList<>();

	/**
	 * execute the whois
	 */
	@Override
	public void run() {
		Set<String> usernames = new TreeSet<>();

		for (final String s : searchFor)
			if (search || fullNameSearch) {
				String searchQuery = "(uid=*" + s + "*)";

				if (fullNameSearch)
					searchQuery = "(|" + searchQuery + "(gecos=*" + s + "*)(cn=*" + s + "*))";

				final Set<String> uids = LDAPHelper.checkLdapInformation(searchQuery, "ou=People,", "uid");

				if (uids != null)
					usernames.addAll(uids);
			}
			else {
				usernames.add(s);
			}

		for (String s : usernames) {
			final AliEnPrincipal principal = UserFactory.getByUsername(s);

			if (principal == null)
				commander.printOutln("Username not found: " + s);
			else
				printUser(principal);
		}
	}

	private void printUser(final AliEnPrincipal principal) {
		commander.printOutln("Username: " + principal.getName());

		Set<String> names = LDAPHelper.checkLdapInformation("uid=" + principal.getName(), "ou=People,", "gecos");

		if (names == null)
			names = LDAPHelper.checkLdapInformation("uid=" + principal.getName(), "ou=People,", "cn");

		printCollection("Full name", names);

		printCollection("Roles", principal.getRoles());

		printCollection("Email", LDAPHelper.getEmails(principal.getName()));

		printCollection("Subject", LDAPHelper.checkLdapInformation("uid=" + principal.getName(), "ou=People,", "subject"));

		commander.printOutln();
	}

	private void printCollection(final String key, final Collection<?> collection) {
		if (collection != null && collection.size() > 0) {
			commander.printOut("  " + key + ": ");

			boolean first = true;

			for (final Object o : collection) {
				if (!first)
					commander.printOut(", ");

				commander.printOut(o.toString());

				first = false;
			}

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
		commander.printOutln(helpOption("-g", "use the lfn as guid"));
		commander.printOutln(helpOption("-r", "resolve links (do not give back pointers to zip archives)"));
		commander.printOutln();
	}

	/**
	 * whereis cannot run without arguments
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

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			if (options.has("h")) {
				printHelp();
				return;
			}
			search = options.has("s");
			fullNameSearch = options.has("f");

			for (final Object o : options.nonOptionArguments())
				searchFor.add(o.toString());

			if (searchFor.size() == 0)
				throw new IllegalArgumentException();

		} catch (final OptionException | IllegalArgumentException e) {
			printHelp();
			throw e;
		}
	}

}
