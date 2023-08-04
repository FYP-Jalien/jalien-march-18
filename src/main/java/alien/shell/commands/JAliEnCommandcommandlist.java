package alien.shell.commands;

import java.util.List;

/**
 * Get the user available internal commands
 * 
 * @author costing
 * @since Aug 17, 2020
 */
public class JAliEnCommandcommandlist extends JAliEnBaseCommand {

	private final boolean interactiveShell;

	@Override
	public void run() {
		if (interactiveShell) {
			// behavior needed by the Java interactive shell, @see BusyBox
			commander.printOutln(JAliEnCOMMander.getCommandList(commander.getUser()));
		}
		else {
			for (String command : commander.getUserAvailableCommands())
				commander.printOutln(command);

			for (String command : commander.getUserAvailableCommands()) {
				commander.printOut("commandlist", command);
				commander.outNextResult();
			}
		}
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("commandlist", ""));
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
	public JAliEnCommandcommandlist(final JAliEnCOMMander commander, final List<String> alArguments) {
		super(commander, alArguments);

		interactiveShell = alArguments != null && alArguments.contains("-i");
	}
}
