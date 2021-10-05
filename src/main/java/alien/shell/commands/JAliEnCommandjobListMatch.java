package alien.shell.commands;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;

import alien.api.taskQueue.CE;
import alien.shell.ShellColor;
import joptsimple.OptionException;

/**
 * @author marta
 *
 */
public class JAliEnCommandjobListMatch extends JAliEnBaseCommand {

	private long jobId = 0;

	@Override
	public void run() {

		logger.log(Level.INFO, "Getting matching CEs for job ID " + jobId);

		final HashMap<CE, Object> matchingCEs = commander.q_api.getMatchingCEs(jobId);

		final List<CE> filteredCEs = new ArrayList<>(matchingCEs.size());

		int maxCENameLength = 0;

		for (final CE ce : matchingCEs.keySet()) {
			if (!ce.ceName.contains("::"))
				continue;

			maxCENameLength = Math.max(maxCENameLength, ce.ceName.length());

			filteredCEs.add(ce);
		}
		commander.printOutln("CE Listing ");
		commander.printOutln(padLeft("CE name", maxCENameLength) + "\t" + padRight("Status", 4) + "\tMax running\t Max queued\t  TTL\t    Type\tHost");
		logger.log(Level.INFO, "Found " + filteredCEs.size() + " that could potentially run the job");

		for (final CE ce : filteredCEs) {
			String status = ce.status;
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

			commander.printOutln(padLeft(String.format("%1$" + maxCENameLength + "s", ce.ceName), maxCENameLength) + "\t" + padLeft(status, 4) + "\t\t"
					+ String.format("%3d", Integer.valueOf(ce.maxjobs)) + "\t\t" + String.format("%3d", Integer.valueOf(ce.maxqueued))
					+ "\t" + String.format("%3d", Long.valueOf(ce.TTL)) + "\t" + padLeft(ce.type, 8) + "\t" + padLeft(ce.host, 8));

			commander.printOut("ceName", ce.ceName);
			commander.printOut("status", String.valueOf(ce.status));
			commander.printOut("maxRunning", String.valueOf(ce.maxjobs));
			commander.printOut("maxQueued", String.valueOf(ce.maxqueued));
			commander.outNextResult();
		}
	}

	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln("jobListMatch: print all the CEs that can run a certain job");
		commander.printOutln(helpUsage("jobListMatch", " [jobId]"));
		commander.printOutln(helpStartOptions());
	}

	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * @param commander
	 * @param alArguments
	 * @throws OptionException
	 */
	public JAliEnCommandjobListMatch(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);

		try {
			if (alArguments.size() > 0) {
				try {
					jobId = Long.parseLong(alArguments.get(0));
				}
				catch (@SuppressWarnings("unused") final NumberFormatException e) {
					commander.printErrln("Invalid job id specification: " + jobId);
					setArgumentsOk(false);
				}
			}
		}
		catch (@SuppressWarnings("unused") final OptionException e) {
			setArgumentsOk(false);
		}
	}
}
