package alien.shell.commands;

import java.util.List;
import java.util.logging.Level;

import alien.config.ConfigUtils;
import alien.optimizers.catalogue.PeriodicOptimiser;
import lazyj.DBFunctions;
/**
 * @author Marta
 * @since May 5, 2021
 */
public class JAliEnCommandresyncLDAP extends JAliEnBaseCommand {

	@Override
	public void run() {
		commander.printOutln("Starting manual resyncLDAP ");
		try (DBFunctions db = ConfigUtils.getDB("alice_users"); DBFunctions db2 = ConfigUtils.getDB("ADMIN");) {
			if (db == null || db2 == null) {
				logger.log(Level.INFO, "Could not get DBs!");
				return;
			}

			PeriodicOptimiser.checkLdapSyncTable(db);

			PeriodicOptimiser.manualResyncLDAP();

			commander.printOutln("Manual esyncLDAP completed");
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
		commander.printOutln(helpUsage("submit", "<URL>"));
		commander.printOutln();
		commander.printOutln(helpParameter("<URL> => <LFN>"));
		commander.printOutln(helpParameter("<URL> => file:///<local path>"));
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
