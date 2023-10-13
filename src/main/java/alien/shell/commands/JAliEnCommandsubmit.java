package alien.shell.commands;

import java.io.File;
import java.io.IOException;
import java.util.List;

import alien.api.ServerException;
import alien.catalogue.FileSystemUtils;
import alien.io.protocols.TempFileManager;
import alien.shell.ErrNo;
import alien.shell.ShellColor;
import alien.taskQueue.JDL;
import alien.taskQueue.TaskQueueUtils;
import lazyj.Utils;

/**
 * @author ron
 * @since June 9, 2011
 */
public class JAliEnCommandsubmit extends JAliEnCommandcat {

	@Override
	public void run() {

		long queueId = 0;

		String content = null;

		String jdlArg = alArguments.get(0);

		String jdlPath = null;

		if (jdlArg.contains("=")) {
			commander.printOutln("Submitting the content that you passed to the command");
			content = jdlArg;
		}
		else {
			jdlPath = FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), jdlArg);

			commander.printOutln("Submitting " + jdlPath);

			final File fout = catFile(jdlPath);

			try {
				if (fout != null && fout.exists() && fout.isFile() && fout.canRead()) {
					content = Utils.readFile(fout.getAbsolutePath());
				}
				else {
					commander.setReturnCode(ErrNo.EREMOTEIO, "Not able to get the file " + jdlPath);
					return;
				}

				if (content == null) {
					commander.setReturnCode(ErrNo.EIO, "Could not read the contents of " + fout.getAbsolutePath());
					return;
				}
			}
			finally {
				TempFileManager.release(fout);
			}
		}

		try {
			final JDL jdl;
			final String[] args = getArgs();

			try {
				jdl = TaskQueueUtils.applyJDLArguments(content, args);
			}
			catch (final IOException ioe) {
				commander.setReturnCode(ErrNo.EINVAL, "Passing arguments to " + (jdlPath != null ? jdlPath : "your job") + " failed:\n  " + ioe.getMessage());
				return;
			}

			if (jdlPath != null)
				jdl.set("JDLPath", jdlPath);

			queueId = commander.q_api.submitJob(jdl);
			if (queueId > 0) {
				commander.printOutln("Your new job ID is " + optionalColor(ShellColor.blue()) + queueId + optionalColor(ShellColor.reset()));
				commander.printOut("jobId", String.valueOf(queueId));
			}
			else
				commander.setReturnCode(ErrNo.EREMOTEIO, "Cannot submit " + (jdlPath != null ? jdlPath : "your job"));
		}
		catch (final ServerException e) {
			commander.setReturnCode(ErrNo.EREMOTEIO, "Task queue rejected " + (jdlPath != null ? jdlPath : "your job") + " due to:\n  " + e.getMessage());
		}
	}

	/**
	 * @return the arguments as a String array
	 */
	public String[] getArgs() {
		return alArguments.size() > 1 ? alArguments.subList(1, alArguments.size()).toArray(new String[0]) : null;
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("submit", "<URL>"));
		commander.printOutln();
		commander.printOutln(helpParameter("<URL> => <LFN>"));
		commander.printOutln(helpParameter("<URL> => file:///<local path>"));
		commander.printOutln();
	}

	/**
	 * Constructor needed for the command factory in commander
	 *
	 * @param commander
	 *
	 * @param alArguments
	 *            the arguments of the command
	 */
	public JAliEnCommandsubmit(final JAliEnCOMMander commander, final List<String> alArguments) {
		super(commander, alArguments);
	}
}
