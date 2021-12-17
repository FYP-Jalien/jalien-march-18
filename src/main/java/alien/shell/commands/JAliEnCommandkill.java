package alien.shell.commands;

import java.util.ArrayList;
import java.util.List;

import alien.shell.ErrNo;
import alien.taskQueue.Job;
import alien.user.AuthorizationChecker;
import joptsimple.OptionException;

/**
 * @author ron
 * @since June 9, 2011
 */
public class JAliEnCommandkill extends JAliEnBaseCommand {

	private final List<Long> queueIds;

	@Override
	public void run() {
		final List<Job> jobs = commander.q_api.getJobs(queueIds);

		for (final Job job : jobs) {
			if (job == null)
				continue;

			commander.printOut("jobid", String.valueOf(job.queueId));
			if (AuthorizationChecker.canModifyJob(job, commander.user)) {
				if (!commander.q_api.killJob(job.queueId)) {
					commander.printOutln("Could not kill: " + job.queueId);
					commander.setReturnCode(ErrNo.EREMOTEIO, "Could not kill the job with id: [" + job.queueId + "]");
					commander.printOut("status", "failed_to_kill");
				}
				else {
					commander.printOutln("Killed: " + job.queueId);
					commander.printOut("status", "killed");
				}
			}
			else {
				commander.printOutln("You are not allowed to kill " + job.queueId);
				commander.printOut("status", "not_allowed");
			}

			commander.outNextResult();
		}
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("kill", "<jobId> [<jobId>[,<jobId>]]"));
		commander.printOutln();
	}

	/**
	 * cat cannot run without arguments
	 *
	 * @return <code>false</code>
	 */
	@Override
	public boolean canRunWithoutArguments() {
		return false;
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
	public JAliEnCommandkill(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);

		queueIds = new ArrayList<>(alArguments.size());

		for (final String arg : alArguments) {
			for (final String id : arg.split("\\D+")) {
				if (id.length() > 0)
					try {
						queueIds.add(Long.valueOf(id));
					}
					catch (@SuppressWarnings("unused") final NumberFormatException e) {
						commander.setReturnCode(ErrNo.EINVAL, "Invalid job ID: " + id);
						setArgumentsOk(false);
						return;
					}
			}
		}
	}

}