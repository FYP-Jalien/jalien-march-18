package alien.shell.commands;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import alien.catalogue.GUIDUtils;
import alien.shell.ErrNo;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * @author costing
 * @since 2018-08-15
 */
public class JAliEnCommandguidinfo extends JAliEnBaseCommand {
	private ArrayList<String> alPaths = null;

	private boolean printGUIDInfo(final String guidName) {
		commander.printOutln(guidName);
		commander.printOut("guid", guidName);

		UUID uuid;
		try {
			uuid = UUID.fromString(guidName);
		}
		catch (@SuppressWarnings("unused") final Exception e) {
			commander.printErrln("  Invalid UUID");
			commander.printOut("error", "Invalid UUID");
			return false;
		}

		final long gTime = GUIDUtils.epochTime(uuid);
		final String mac = GUIDUtils.getMacAddr(uuid);

		commander.printOutln("  Created: " + (new Date(gTime)) + " (" + gTime + ") by " + mac);
		commander.printOut("mtime", String.valueOf(gTime));
		commander.printOut("mac", mac);

		return true;
	}

	@Override
	public void run() {
		for (final String lfnName : this.alPaths) {
			printGUIDInfo(lfnName);
			commander.outNextResult();
		}
	}

	private static final OptionParser parser = new OptionParser();

	static {
		parser.accepts("v");
	}

	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("stat", "[-v] <filename1> [<or uuid>] ..."));
		commander.printOutln(helpOption("-v", "More details on the status."));
		commander.printOutln();
	}

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
	public JAliEnCommandguidinfo(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);
		try {
			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			alPaths = new ArrayList<>(options.nonOptionArguments().size());
			alPaths.addAll(optionToString(options.nonOptionArguments()));
		}
		catch (final OptionException e) {
			commander.setReturnCode(ErrNo.EINVAL, e.getMessage());
			setArgumentsOk(false);
		}
	}
}
