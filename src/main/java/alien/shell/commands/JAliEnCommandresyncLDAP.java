package alien.shell.commands;

import java.util.List;

import alien.optimizers.catalogue.PeriodicOptimiser;
import alien.shell.ShellColor;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * @author Marta
 * @since May 5, 2021
 */
public class JAliEnCommandresyncLDAP extends JAliEnBaseCommand {

	private boolean lastLog = false;

	@Override
	public void run() {
		commander.printOutln("Print last log flag set to " + this.lastLog);
		commander.printOutln("Starting manual resyncLDAP ");

		String logOutput = PeriodicOptimiser.manualResyncLDAP(this.lastLog);
		commander.printOutln(ShellColor.jobStateRed() + logOutput + ShellColor.reset());

		commander.printOutln("Manual resyncLDAP completed");

	}

	/**
	 * @return the arguments as a String array
	 */
	public String[] getArgs() {
		return alArguments.size() > 1 ? alArguments.subList(1, alArguments.size()).toArray(new String[0]) : null;
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln("Usage: resyncLDAP -l <boolean printLastNonEmptyLog>");
		commander.printOutln();
		commander.printOutln(helpParameter("Synchronizes the DB with the updated values in LDAP"));
		commander.printOutln(helpParameter("-l : Boolean set to print last non empty update log"));
		commander.printOutln();
	}

	/**
	 * Constructor needed for the command factory in commander
	 *
	 * @param commander
	 *
	 * @param alArguments
	 *            the arguments of the command
	 */
	public JAliEnCommandresyncLDAP(final JAliEnCOMMander commander, final List<String> alArguments) {
		super(commander, alArguments);

		final OptionParser parser = new OptionParser();
		parser.accepts("l");
		final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));
		final List<String> params = optionToString(options.nonOptionArguments());
		// check for at least 1 argument1
		if (params.size() >= 1)
			this.lastLog = Boolean.parseBoolean(params.get(0));
		else
			this.lastLog = false;
	}

	@Override
	public boolean canRunWithoutArguments() {
		return true;
	}
}
