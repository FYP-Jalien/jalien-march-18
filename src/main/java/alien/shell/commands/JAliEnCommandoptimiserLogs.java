package alien.shell.commands;

import java.util.ArrayList;
import java.util.List;

import alien.optimizers.DBSyncUtils;
import alien.shell.ShellColor;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * @author Marta
 * @since May 5, 2021
 */
public class JAliEnCommandoptimiserLogs extends JAliEnBaseCommand {

	private List<String> classes;
	private boolean verbose;
	private int frequency;

	@Override
	public void run() {

		if (frequency != 0 && classes != null && !classes.isEmpty())
			DBSyncUtils.modifyFrequency(frequency, classes);

		String log = "";
		if (classes != null && !classes.isEmpty()) {
			for (String className : classes) {
				log = log + DBSyncUtils.getLastLog(className, verbose) + "\n";
			}
			commander.printOutln(ShellColor.jobStateRed() + log + ShellColor.reset());
		} else
			commander.printOutln(ShellColor.jobStateRed() + "None or wrong classes were introduced" + ShellColor.reset());

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
		commander.printOutln("Usage: lastOptimiserLog [-v] [-f <frequency>] <classnames or metanames>");
		commander.printOutln();
		commander.printOutln(helpParameter("Gets the last log from the optimiser"));
		commander.printOutln(helpParameter("-v : Verbose, displays the frequency, last run timestamp and log"));
		commander.printOutln(helpParameter("-f <value> : Frequency, in seconds"));
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
	public JAliEnCommandoptimiserLogs(final JAliEnCOMMander commander, final List<String> alArguments) {
		super(commander, alArguments);

		verbose = false;
		frequency = 0;

		final OptionParser parser = new OptionParser();
		parser.accepts("f").withRequiredArg();
		parser.accepts("v");

		final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

		if (options.has("v"))
			verbose = true;

		if (options.has("f")) {
			try {
				frequency = Integer.valueOf(options.valueOf("f").toString()).intValue();
				frequency = frequency > -1 ? frequency * 1000 : frequency;
			} catch (NumberFormatException e) {
				frequency = -1;
			}
		}

		final List<String> params = optionToString(options.nonOptionArguments());
		// check for at least 1 arguments
		if (params.size() < 1)
			return;

		List<String> originalClasses = params;
		this.classes = new ArrayList<>();
		for (String classname : originalClasses) {
			if (classname.equals("ResyncLDAP.*") || classname.equals("resyncLDAP")) {
				addResyncLDAPParameter("users");
				addResyncLDAPParameter("roles");
				addResyncLDAPParameter("SEs");
			}
			if (classname.equals("users") || classname.equals("roles") || classname.equals("SEs"))
				addResyncLDAPParameter(classname);
		}
	}

	private void addResyncLDAPParameter(String parameter) {
		String prefix = "alien.optimizers.catalogue.ResyncLDAP.";
		if (!this.classes.contains(prefix + parameter)) {
			this.classes.add(prefix + parameter);
		}
	}

	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}
}
