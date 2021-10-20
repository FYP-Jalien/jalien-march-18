package alien.shell.commands;

import java.util.List;

import alien.api.taskQueue.TaskQueueApiUtils;
import alien.quotas.FileQuota;
import alien.quotas.Quota;
import alien.shell.ErrNo;
import alien.user.AliEnPrincipal;
import alien.user.UserFactory;
import joptsimple.OptionException;

/**
 *
 */
public class JAliEnCommandjquota extends JAliEnBaseCommand {
	private boolean isAdmin;
	private String command;

	private AliEnPrincipal user_to_set;
	private String param_to_set;
	private Long value_to_set;

	@Override
	public void run() {
		if (!this.command.equals("list") && !(isAdmin && this.command.equals("set"))) {
			commander.setReturnCode(ErrNo.EINVAL, "Wrong command passed: " + this.command);
			printHelp();
			return;
		}

		final String username = user_to_set.getName();

		if (command.equals("list")) {
			final Quota q = TaskQueueApiUtils.getJobsQuota(user_to_set);
			if (q == null) {
				commander.setReturnCode(ErrNo.ENODATA, "No jobs quota found for user " + username);
				return;
			}

			commander.printOut("username", q.user);

			commander.printOut("nominalparallelJobs", String.valueOf(q.nominalparallelJobs));
			commander.printOut("running", String.valueOf(q.running));
			commander.printOut("waiting", String.valueOf(q.waiting));
			commander.printOut("totalCpuCostLast24h", String.valueOf(q.totalCpuCostLast24h));
			commander.printOut("totalRunningTimeLast24h", String.valueOf(q.totalRunningTimeLast24h));

			commander.printOut("maxparallelJobs", String.valueOf(q.maxparallelJobs));
			commander.printOut("maxUnfinishedJobs", String.valueOf(q.maxUnfinishedJobs));
			commander.printOut("maxTotalCpuCost", String.valueOf(q.maxTotalCpuCost));
			commander.printOut("maxTotalRunningTime", String.valueOf(q.maxTotalRunningTime));
			
			commander.printOutln(q.toString());
			return;
		}

		if (command.equals("set")) {
			if (this.param_to_set == null) {
				commander.setReturnCode(ErrNo.EINVAL, "Error in parameter name");
				return;
			}
			if (this.value_to_set == null || this.value_to_set.intValue() == 0) {
				commander.setReturnCode(ErrNo.EINVAL, "Error in value");
				printHelp();
				return;
			}
			// run the update
			if (TaskQueueApiUtils.setJobsQuota(user_to_set, this.param_to_set, this.value_to_set.toString()))
				commander.printOutln("Result: ok, " + this.param_to_set + "=" + this.value_to_set.toString() + " for user=" + username);
			else
				commander.setReturnCode(ErrNo.EREMOTEIO, "Result: failed to set " + this.param_to_set + "=" + this.value_to_set.toString() + " for user=" + username);
		}
	}

	@Override
	public void printHelp() {
		commander.printOutln();

		commander.printOutln(helpUsage("jquota", "Displays information about Job Quotas."));
		commander.printOutln(helpStartOptions());
		commander.printOutln(helpOption("list [username]", "get job quota information for the current account, or the indicated one"));
		commander.printOutln(helpOption("set <user> <field> <value>", "to set quota fileds (one of  maxUnfinishedJobs, maxTotalCpuCost, maxTotalRunningTime)"));
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
	public JAliEnCommandjquota(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);
		this.isAdmin = commander.getUser().canBecome("admin");
		if (alArguments.size() == 0)
			return;

		this.command = alArguments.get(0);

		this.user_to_set = commander.getUser();

		if (this.command.equals("set") && alArguments.size() == 4) {
			this.user_to_set = UserFactory.getByUsername(alArguments.get(1));

			final String param = alArguments.get(2);

			if (FileQuota.canUpdateField(param))
				return;

			this.param_to_set = param;
			try {
				this.value_to_set = Long.valueOf(alArguments.get(3));
			}
			catch (@SuppressWarnings("unused") final Exception e) {
				// FIXME invalid numeric values are ignored
			}
		}

		if (this.command.equals("list") && alArguments.size() > 1) {
			this.user_to_set = UserFactory.getByUsername(alArguments.get(1));

			if (this.user_to_set == null) {
				commander.printErrln("No such account name: " + alArguments.get(1));
				setArgumentsOk(false);
			}
		}
	}

}
