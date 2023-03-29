package alien.shell.commands;

import java.util.List;

/**
 * @author ron
 * @since June 4, 2011
 * @author sraje (Shikhar Raje, IIIT Hyderabad)
 * @since Modified 27th July, 2012
 */
public class JAliEnCommandecho extends JAliEnBaseCommand {

	@Override
	public void run() {
		for (int idx = 0; idx < alArguments.size(); idx++)
			commander.printOut("arg" + idx, alArguments.get(idx));

		commander.printOutln(String.join(" ", alArguments));
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("echo", "[text]"));
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
	public JAliEnCommandecho(final JAliEnCOMMander commander, final List<String> alArguments) {
		super(commander, alArguments);
	}
}
