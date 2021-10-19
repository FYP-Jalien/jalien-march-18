package alien.shell.commands;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Level;

import alien.config.ConfigUtils;
import alien.user.LDAPHelper;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * @author marta
 *
 */
public class JAliEnCommandlistpartitions extends JAliEnBaseCommand {
	private List<String> partitionsToQuery = new ArrayList<>();

	private boolean verboseOutput = false;

	@Override
	public void run() {

		final TreeMap<String, String> partitions = new TreeMap<>();
		final String partitionList = ConfigUtils.getPartitions("*");
		logger.log(Level.INFO, "A total of " + partitionList.length() + " partitions were obtained from LDAP.");
		for (final String partition : partitionList.split(",")) {
			if (((partitionsToQuery.size() > 0 && partitionsToQuery.contains(partition)) || partitionsToQuery.size() == 0)) {
				final String ceList = getCEListFromPartition(partition);
				partitions.put(partition, ceList);
			}
		}

		commander.printOutln("Partition listing");
		if (!verboseOutput) {
			commander.printOutln(padLeft("Partition name", 15));
			for (final String partition : partitions.keySet())
				commander.printOutln(padLeft(partition, 15));
		}
		else {
			commander.printOutln(padLeft("Partition name", 15) + "\t\t Member CEs");
			for (final Map.Entry<String, String> partition : partitions.entrySet())
				commander.printOutln(padLeft(partition.getKey(), 15) + "\t\t" + partition.getValue());
		}
	}

	private static String getCEListFromPartition(final String partition) {
		final Set<String> ceList = LDAPHelper.checkLdapInformation("name=" + partition, "ou=Partitions,", "CEname");
		return String.join(",", ceList);
	}

	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln("listpartitions: print all (or a subset) of the defined partitions");
		commander.printOutln(helpUsage("listpartitions", " [-v] [Partition name] [Partition name] ..."));
		commander.printOutln(helpStartOptions());
		commander.printOutln(helpOption("-v", "print verbose output (including member CEs)"));
	}

	@Override
	public boolean canRunWithoutArguments() {
		return true;
	}

	/**
	 * @param commander
	 * @param alArguments
	 * @throws OptionException
	 */
	public JAliEnCommandlistpartitions(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);

		final OptionParser parser = new OptionParser();
		parser.accepts("v");

		final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

		verboseOutput = options.has("v");

		partitionsToQuery = optionToString(options.nonOptionArguments());
	}
}
