package alien.shell.commands;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import alien.api.taskQueue.ResubmitJob;
import alien.shell.ErrNo;
import joptsimple.OptionException;

/**
 * @author mmmartin
 * @since November 22, 2017
 */
public class JAliEnCommandresubmit extends JAliEnBaseCommand {

	private final List<Long> queueIds;

	@Override
	public void run() {
		for (final long queueId : queueIds) {
			final ResubmitJob rj = commander.q_api.resubmitJob(queueId);
			final Entry<Integer, String> rc = (rj != null ? rj.resubmitEntry() : null);

			if (rc == null) {
				commander.setReturnCode(ErrNo.EREMOTEIO, "Problem with the resubmit request of job ID " + queueId);
			}
			else {
				switch (rc.getKey().intValue()) {
					case 0:
						commander.printOutln(rc.getValue());
						break;
					default:
						commander.setReturnCode(rc.getKey().intValue(), rc.getValue());
						break;
				}
			}
		}
	}

	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln("resubmit: resubmits a job or a group of jobs by IDs");
		commander.printOutln("        Usage:");
		commander.printOutln("                resubmit <jobid1> [<jobid2>....]");
		commander.printOutln();
	}

	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * Constructor needed for the command factory in commander
	 *
	 * @param commander
	 * @param alArguments
	 * @throws OptionException
	 */
	public JAliEnCommandresubmit(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
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
