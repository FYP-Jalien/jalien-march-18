package alien.shell.commands;

import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;
import java.util.Map;

import alien.api.taskQueue.GetUptime.UserStats;
import alien.api.taskQueue.TaskQueueApiUtils;
import alien.shell.ErrNo;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * @author ron
 * @since Oct 27, 2011
 */
public class JAliEnCommandw extends JAliEnBaseCommand {

	private static final String format = "%3d. %-20s | %12s | %12s%n";
	private static final String formatH = "     %-20s | %12s | %12s%n";

	private static final String separator = "--------------------------+--------------+--------------\n";

	private int sortBy = 0;
	private boolean reversed = false;

	@Override
	public void run() {
		final Map<String, UserStats> stats = TaskQueueApiUtils.getUptime();

		if (stats == null)
			return;

		final UserStats totals = new UserStats();

		final StringBuilder sb = new StringBuilder();

		final ArrayList<Map.Entry<String, UserStats>> toDisplay = new ArrayList<>(stats.entrySet());

		if (sortBy == 0)
			toDisplay.sort((e1, e2) -> reversed ? e2.getKey().compareTo(e1.getKey()) : e1.getKey().compareTo(e2.getKey()));

		if (sortBy == 1)
			toDisplay.sort((e1, e2) -> reversed ? e1.getValue().runningJobs - e2.getValue().runningJobs : e2.getValue().runningJobs - e1.getValue().runningJobs);

		if (sortBy == 2)
			toDisplay.sort((e1, e2) -> reversed ? e1.getValue().waitingJobs - e2.getValue().waitingJobs : e2.getValue().waitingJobs - e1.getValue().waitingJobs);

		try (Formatter formatter = new Formatter(sb)) {
			formatter.format(formatH, "Account name", "Active jobs", "Waiting jobs");

			sb.append(separator);

			int i = 0;

			for (final Map.Entry<String, UserStats> entry : toDisplay) {
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
		commander.printOutln(helpStartOptions());
		commander.printOutln(helpOption("-a", "Sort by the number of active jobs"));
		commander.printOutln(helpOption("-w", "Sort by the number of waiting jobs"));
		commander.printOutln(helpOption("-r", "Reverse sorting order"));
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

		try {
			final OptionParser parser = new OptionParser();

			parser.accepts("w");
			parser.accepts("a");
			parser.accepts("r");

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			reversed = options.has("r");

			if (options.has("a"))
				sortBy = 1;

			if (options.has("w"))
				sortBy = 2;
		}
		catch (final OptionException | IllegalArgumentException e) {
			commander.setReturnCode(ErrNo.EINVAL, e.getMessage());
			setArgumentsOk(false);
		}
	}
}
