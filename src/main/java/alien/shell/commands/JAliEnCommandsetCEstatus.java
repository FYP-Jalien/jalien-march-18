package alien.shell.commands;

import java.util.Arrays;
import java.util.List;

import alien.shell.ShellColor;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * @author marta
 *
 */
public class JAliEnCommandsetCEstatus extends JAliEnBaseCommand {
	private String statusValue;
	private List<String> ceNames;

	@Override
	public void run() {

		final List<String> updatedCEs = commander.q_api.setCEStatus(statusValue, ceNames);
		if (updatedCEs.size() > 0)
			commander.printOutln(ShellColor.jobStateGreen() + "Success: " + ShellColor.reset() + " Status " + statusValue + " correctly set to CEs " + updatedCEs);
		else
			commander.printOutln(ShellColor.jobStateRed() + "Error: " + ShellColor.reset() + " Could not set status " + statusValue + " to CEs " + ceNames);

	}

	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln("setCEstatus: Sets the status of a set of Computing Elements");
		commander.printOutln(helpUsage("listpartitions", " [-status status] [CE name]  [CE name]  ..."));
		commander.printOutln(helpStartOptions());
		commander.printOutln(helpOption("-status", "Status to be set for the CEs (open / locked)"));
	}

	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * @param commander
	 * @param alArguments
	 * @throws Exception
	 */
	public JAliEnCommandsetCEstatus(final JAliEnCOMMander commander, final List<String> alArguments) throws Exception {
		super(commander, alArguments);

		final OptionParser parser = new OptionParser();
		parser.accepts("status").withRequiredArg();

		final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

		if (options.has("status")) {
			statusValue = String.valueOf(options.valueOf("status"));
		}

		if (statusValue.toLowerCase().contains("open") || "open".contains(statusValue.toLowerCase()))
			statusValue = "open";
		else if (statusValue.toLowerCase().contains("locked") || "locked".contains(statusValue.toLowerCase()))
			statusValue = "locked";
		else
			throw new Exception("The status can only be defined to open and locked");

		ceNames = optionToString(options.nonOptionArguments());
		if (ceNames.isEmpty())
			throw new Exception("The list of given CE names should not be empty");
	}
}
