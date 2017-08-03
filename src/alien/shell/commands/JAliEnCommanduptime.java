package alien.shell.commands;

import java.util.ArrayList;
import java.util.Map;

import alien.api.taskQueue.GetUptime.UserStats;
import alien.api.taskQueue.TaskQueueApiUtils;
import joptsimple.OptionException;

/**
 * @author ron
 * @since Oct 27, 2011
 */
public class JAliEnCommanduptime extends JAliEnBaseCommand {

	@Override
	public void run() {
		final Map<String, UserStats> stats = TaskQueueApiUtils.getUptime();

		if (stats == null)
			return;

		final UserStats totals = new UserStats();

		for (final UserStats u : stats.values())
			totals.add(u);
		if (out.isRootPrinter()) {
			out.setField(" running jobs", " " + totals.runningJobs);
			out.setField(" waiting jobs", " " + totals.waitingJobs);
			out.setField(" active users", " " + stats.size());
		}
		else
			out.printOutln(totals.runningJobs + " running jobs, " + totals.waitingJobs + " waiting jobs, " + stats.size() + " active users");
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		out.printOutln();
		out.printOutln(helpUsage("uptime", ""));
		out.printOutln(helpStartOptions());
		out.printOutln();
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
	 * @param out
	 *
	 * @param alArguments
	 *            the arguments of the command
	 * @throws OptionException
	 */
	public JAliEnCommanduptime(final JAliEnCOMMander commander, final UIPrintWriter out, final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);

	}
}
