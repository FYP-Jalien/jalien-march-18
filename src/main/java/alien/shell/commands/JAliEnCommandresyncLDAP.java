package alien.shell.commands;

import java.util.List;

import alien.api.Dispatcher;
import alien.api.ServerException;
import alien.api.catalogue.ManualResyncLDAP;
import alien.shell.ErrNo;
import alien.shell.ShellColor;

/**
 * @author Marta
 * @since May 5, 2021
 */
public class JAliEnCommandresyncLDAP extends JAliEnBaseCommand {

	@Override
	public void run() {
		try {
			final ManualResyncLDAP manualCall = Dispatcher.execute(new ManualResyncLDAP());

			commander.printOutln(ShellColor.jobStateRed() + manualCall.getLogOutput().trim() + ShellColor.reset());
		}
		catch (final ServerException e) {
			commander.setReturnCode(ErrNo.ENODATA, "Could not get the log output from resyncLDAP command : " + e.getMessage());
			return;
		}
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
		commander.printOutln("Usage: resyncLDAP");
		commander.printOutln();
		commander.printOutln(helpParameter("Synchronizes the DB with the updated values in LDAP"));
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
	}

	@Override
	public boolean canRunWithoutArguments() {
		return true;
	}
}
