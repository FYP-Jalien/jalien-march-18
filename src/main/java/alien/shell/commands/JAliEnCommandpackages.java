package alien.shell.commands;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import alien.catalogue.Package;
import alien.shell.ErrNo;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * @author ron
 * @since Nov 23, 2011
 */
public class JAliEnCommandpackages extends JAliEnBaseCommand {

	private List<Package> packs = null;

	private String platform;

	private Set<String> filters = null;

	@Override
	public void run() {
		packs = commander.c_api.getPackages(platform);

		if (packs != null) {
			Collections.sort(packs);
			
			for (final Package p : packs) {
				boolean display = false;

				if (filters == null || filters.size() == 0)
					display = true;
				else
					for (String filter : filters)
						if (p.getFullName().toLowerCase().contains(filter)) {
							display = true;
							break;
						}

				if (display) {
					commander.outNextResult();
					commander.printOut("packages", p.getFullName());
					commander.printOutln(padSpace(1) + p.getFullName());
				}
			}
		}
		else {
			commander.setReturnCode(ErrNo.ENODATA, "Couldn't find any packages.");
		}
	}

	private static String getPackagePlatformName() {

		String ret = System.getProperty("os.name");

		if (System.getProperty("os.arch").contains("amd64"))
			ret += "-x86_64";

		else if (ret.toLowerCase().contains("mac") && System.getProperty("os.arch").contains("ppc"))
			ret = "Darwin-PowerMacintosh";

		return ret;
	}

	/**
	 * printout the help info, none for this command
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("packages", "  list available packages"));

		commander.printOutln(helpOption("-platform", "Platform name, default " + getPackagePlatformName()));
		commander.printOutln(helpOption("-all", "List packages on all platforms. Equivalent to '-p all'"));

		commander.printOutln();
	}

	/**
	 * cd can run without arguments
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
	public JAliEnCommandpackages(final JAliEnCOMMander commander, final List<String> alArguments) {
		super(commander, alArguments);

		platform = getPackagePlatformName();

		try {
			final OptionParser parser = new OptionParser();

			parser.accepts("platform").withRequiredArg().describedAs("Platform type").ofType(String.class);
			parser.accepts("all");

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			if (options.has("platform"))
				platform = options.valueOf("platform").toString();

			if (options.has("all"))
				platform = "all";

			if (options.nonOptionArguments().size() > 0) {
				filters = new LinkedHashSet<>();
				for (Object o : options.nonOptionArguments())
					filters.add(o.toString().toLowerCase());
			}
		}
		catch (final OptionException | IllegalArgumentException e) {
			commander.setReturnCode(ErrNo.EINVAL, e.getMessage());
			setArgumentsOk(false);
		}
	}
}
