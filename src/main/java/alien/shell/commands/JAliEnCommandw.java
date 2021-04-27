package alien.shell.commands;

import java.util.Formatter;
import java.util.List;
import java.util.Map;

import alien.api.taskQueue.GetUptime.UserStats;
import alien.api.taskQueue.TaskQueueApiUtils;
import joptsimple.OptionException;

/**
 * @author ron
 * @since Oct 27, 2011
 */
public class JAliEnCommandw extends JAliEnBaseCommand {

	private static final String format = "%3d. %-20s | %12s | %12s%n";
	private static final String formatH = "     %-20s | %12s | %12s%n";

	private static final String separator = "--------------------------+--------------+--------------\n";

	@Override
	public void run() {
		final Map<String, UserStats> stats = TaskQueueApiUtils.getUptime();

		if (stats == null)
			return;

		final UserStats totals = new UserStats();

		final StringBuilder sb = new StringBuilder();

		try (Formatter formatter = new Formatter(sb)) {
			formatter.format(formatH, "Account name", "Active jobs", "Waiting jobs");

			sb.append(separator);

			int i = 0;

			for (final Map.Entry<String, UserStats> entry : stats.entrySet()) {
				final String username = entry.getKey();
				final UserStats us = entry.getValue();

				i++;

				formatter.format(format, Integer.valueOf(i), username, String.valueOf(us.runningJobs), String.valueOf(us.waitingJobs));

				totals.add(us);
			}

			sb.append(separator);

			formatter.format(formatH, "TOTAL", String.valueOf(totals.runningJobs), String.valueOf(totals.waitingJobs));
		}

		commander.printOut("value", sb.toString());
		commander.printOut(sb.toString());
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("w", "Show currently active users on the Grid"));
		commander.printOutln();
	}

	/**
	 * mkdir cannot run without arguments
	 *
	 * @return <code>false</code>
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
	 * @throws OptionException
	 */
	public JAliEnCommandw(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);
	}
}
