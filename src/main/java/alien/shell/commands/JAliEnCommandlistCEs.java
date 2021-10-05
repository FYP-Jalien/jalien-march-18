package alien.shell.commands;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import alien.api.taskQueue.CE;
import alien.shell.ShellColor;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * @author marta
 *
 */
public class JAliEnCommandlistCEs extends JAliEnBaseCommand {
	private List<String> cesToQuery = new ArrayList<>();
	private final List<String> partitionsToFilter = new ArrayList<>();

	private boolean printSummary = false;
	private boolean printVerbose = false;

	@Override
	public void run() {

		final List<CE> results = commander.c_api.getCEs(cesToQuery);

		final List<CE> filteredCEs = new ArrayList<>(results.size());

		int maxCENameLength = 0;

		for (final CE ce : results) {
			if (!ce.ceName.contains("::"))
				continue;

			maxCENameLength = Math.max(maxCENameLength, ce.ceName.length());

			filteredCEs.add(ce);
		}

		if (!partitionsToFilter.isEmpty()) {
			final ArrayList<CE> cesToExclude = new ArrayList<>();
			for (final CE candidateCE : filteredCEs) {
				for (final String partition : partitionsToFilter) {
					if (!candidateCE.partitions.contains(partition)) {
						cesToExclude.add(candidateCE);
					}
				}
			}
			filteredCEs.removeAll(cesToExclude);
		}

		commander.printOutln("CE Listing ");
		if (printVerbose)
			commander.printOutln(padLeft("CE name", maxCENameLength) + "\t" + padRight("Status", 4) + "\tMax running\t Max queued\t  TTL\t    Type\t\t\t\t\t  Host\t\t\tPartitions");
		else
			commander.printOutln(padLeft("CE name", maxCENameLength) + "\t" + padRight("Status", 4) + "\tMax running\t Max queued\t  TTL\t    Type\t\tHost");

		long summaryRunningJobs = 0;
		long summaryQueuedJobs = 0;

		for (final CE ce : filteredCEs) {
			final int maxRunning = ce.maxjobs;
			final int maxQueued = ce.maxqueued;
			String status = ce.status;
			final long ttl = ce.TTL;
			final String host = ce.host;
			final String type = ce.type;
			final String partitions = String.join(",", ce.partitions);
			summaryRunningJobs += maxRunning;
			summaryQueuedJobs += maxQueued;

			switch (status) {
				case "open":
					status = ShellColor.jobStateGreen() + status + ShellColor.reset();
					break;
				case "locked":
					status = ShellColor.jobStateRed() + status + ShellColor.reset();
					break;
				default:
					status = ShellColor.jobStateYellow() + status + ShellColor.reset();
			}

			if (printVerbose)
				commander.printOutln(padLeft(String.format("%1$" + maxCENameLength + "s", ce.ceName), maxCENameLength) + "\t" + padLeft(status, 4) + "\t\t"
						+ String.format("%3d", Integer.valueOf(maxRunning)) + "\t\t" + String.format("%3d", Integer.valueOf(maxQueued))
						+ "\t" + String.format("%3d", Long.valueOf(ttl)) + "\t" + padLeft(type, 8) + "\t" + padLeft(host, 40) + "\t\t" + padLeft(partitions, 8));
			else
				commander.printOutln(padLeft(String.format("%1$" + maxCENameLength + "s", ce.ceName), maxCENameLength) + "\t" + padLeft(status, 4) + "\t\t"
						+ String.format("%3d", Integer.valueOf(maxRunning)) + "\t\t" + String.format("%3d", Integer.valueOf(maxQueued))
						+ "\t" + String.format("%3d", Long.valueOf(ttl)) + "\t" + padLeft(type, 8) + "\t" + padLeft(host, 8));

			commander.printOut("ceName", ce.ceName);
			commander.printOut("status", ce.status);
			commander.printOut("maxRunning", String.valueOf(maxRunning));
			commander.printOut("maxQueued", String.valueOf(maxQueued));
			commander.printOut("partitions", partitions);
			commander.outNextResult();
		}

		if (printSummary && filteredCEs.size() > 1) {
			commander.printOutln();
			commander.printOutln(String.format("%1$" + maxCENameLength + "s", "TOTAL: " + filteredCEs.size() + " CEs") + "\t \t" + padLeft(String.valueOf(summaryRunningJobs), 8)
					+ "\t" + padLeft(String.valueOf(summaryQueuedJobs), 8));
		}
	}

	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln("listCEs: print all (or a subset) of the defined CEs with their details");
		commander.printOutln(helpUsage("listCEs", " [-s] [CE name] [CE name] ..."));
		commander.printOutln(helpStartOptions());
		commander.printOutln(helpOption("-s", "print summary information"));
		commander.printOutln(helpOption("-p", "filter by partition names"));
		commander.printOutln(helpOption("-v", "print verbose output (including partitions)"));
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
	public JAliEnCommandlistCEs(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);

		final OptionParser parser = new OptionParser();
		parser.accepts("s");
		parser.accepts("p").withRequiredArg();
		parser.accepts("v");

		final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

		printSummary = options.has("s");
		printVerbose = options.has("v");
		if (options.has("p")) {
			for (final Object o : options.valuesOf("p")) {
				partitionsToFilter.addAll(Arrays.asList(o.toString().split(",")));
			}
		}
		cesToQuery = optionToString(options.nonOptionArguments());
	}
}
