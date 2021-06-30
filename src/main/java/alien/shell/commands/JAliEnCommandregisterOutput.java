package alien.shell.commands;

import java.util.Collection;
import java.util.List;

import alien.catalogue.LFN;
import alien.shell.ErrNo;
import alien.taskQueue.Job;
import alien.taskQueue.JobStatus;

/**
 * @author ron
 * @since June 4, 2011
 */
public class JAliEnCommandregisterOutput extends JAliEnBaseCommand {

	private long jobId = -1;

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("registerOutput", "<job ID>"));
		commander.printOutln(helpStartOptions());
	}

	/**
	 * get cannot run without arguments
	 *
	 * @return <code>false</code>
	 */
	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * return RootPrintWriter.columnseparator + RootPrintWriter.fielddescriptor + lfn + RootPrintWriter.fieldseparator + "0";
	 *
	 * return RootPrintWriter.columnseparator + RootPrintWriter.fielddescriptor + lfn + RootPrintWriter.fieldseparator + "1"; }
	 *
	 * /** Constructor needed for the command factory in commander
	 *
	 * @param commander
	 *
	 * @param alArguments
	 *            the arguments of the command
	 */
	public JAliEnCommandregisterOutput(final JAliEnCOMMander commander, final List<String> alArguments) {
		super(commander, alArguments);

		for (final String arg : alArguments) {
			try {
				jobId = Long.parseLong(arg);
			}
			catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
				commander.setReturnCode(ErrNo.EINVAL, "Invalid job ID: " + arg);
				setArgumentsOk(false);
				return;
			}
		}
	}

	@Override
	public void run() {
		if (jobId < 0)
			return;

		final Job j = commander.q_api.getJob(jobId);

		if (j == null) {
			commander.setReturnCode(ErrNo.ESRCH, "Job ID " + jobId + " was not found in the task queue");
			return;
		}

		if (j.isMaster()) {
			commander.setReturnCode(ErrNo.EINVAL, "Job ID " + jobId + " is a masterjob, you can only register the output of a failed (sub)job");
			return;
		}

		if (!j.status().equals(JobStatus.ERROR_V) && !j.status().equals(JobStatus.ERROR_E)) {
			commander.setReturnCode(ErrNo.EINVAL, "Job ID " + jobId + " is in state " + j.getStatusName() + ". You can only register ERROR_V jobs (or ERROR_E when OutputErrorE exists).");
			return;
		}

		if (!commander.user.canBecome(j.user)) {
			commander.setReturnCode(ErrNo.EPERM, "You (" + commander.getUsername() + ") are not allowed to work on " + j.user + "'s jobs");
			return;
		}

		final Collection<LFN> bookedLFNs = commander.c_api.registerOutput(jobId);

		if (bookedLFNs == null || bookedLFNs.size() == 0) {
			commander.setReturnCode(ErrNo.ENODATA, "No files have been commited to the catalogue for job ID " + jobId);
			return;
		}

		for (final LFN l : bookedLFNs)
			commander.printOutln("  " + l.getCanonicalName());
	}
}
