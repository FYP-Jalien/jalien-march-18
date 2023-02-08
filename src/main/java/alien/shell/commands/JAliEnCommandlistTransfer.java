package alien.shell.commands;

import java.util.List;

import alien.io.TransferDetails;
import alien.shell.ErrNo;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 *
 */
public class JAliEnCommandlistTransfer extends JAliEnBaseCommand {
	private String status;
	private String toSE;
	private String user;
	private Long id;
	private boolean verbose;
	private boolean jdl;
	private int count;
	private boolean sort_desc;

	@Override
	public void run() {
		commander
				.printOutln("TransferId\t   Status\t   User\t\tDestination      \tSize" + (this.jdl ? "\t\tSource" : "") + "\t\tAttempts" + (this.verbose ? "\t\tError reason" : "") + "\tFile name");
		if (this.count == 0) {
			commander.printOutln();
			commander.printOutln("Total: 0 transfers");
			return;
		}
		final List<TransferDetails> transfers = commander.c_api.listTransfer(this.toSE, this.user, this.status, this.id, this.count, this.sort_desc);
		if (transfers == null)
			return;
		for (final TransferDetails t : transfers)
			commander.printOutln(t.transferId + "\t   " + t.status + "\t   " + t.user + "\t" + t.destination + String.format("%14d", Long.valueOf(t.size)) + // t.size +
					(this.jdl && t.jdl != null ? "\t\t" + t.jdl : "\t\t") + "\t" + t.attempts + (this.verbose && t.reason != null ? "\t" + t.reason : "") + "\t" + t.lfn);

		commander.printOutln();
		commander.printOutln("Total: " + transfers.size() + " transfers");
	}

	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln("listTransfer: returns all the transfers that are waiting in the system");
		commander.printOutln("        Usage:");
		commander.printOutln(
				"                listTransfer [-status <status>] [-user <user>] [-id <queueId>] [-verbose] [-master] [-summary] [-all_status] [-jdl] [-destination <site>]  [-list=<number(all transfers by default)>] [-desc]");
		commander.printOutln();

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
	public JAliEnCommandlistTransfer(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);

		try {
			final OptionParser parser = new OptionParser();
			parser.accepts("list").withRequiredArg();
			parser.accepts("status").withRequiredArg();
			parser.accepts("user").withRequiredArg();
			parser.accepts("id").withRequiredArg().ofType(Long.class);
			parser.accepts("verbose");
			parser.accepts("summary");
			parser.accepts("all_status");
			parser.accepts("jdl");
			parser.accepts("desc");
			parser.accepts("destination").withRequiredArg();

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			if (options.has("list")) {
				final int cnt = Integer.parseInt((String) options.valueOf("list"));
				if (cnt < 0)
					throw new NumberFormatException();
				this.count = cnt;
			}
			else
				this.count = -1;

			this.status = (String) options.valueOf("status");
			this.user = (String) options.valueOf("user");
			if (options.has("id"))
				this.id = (Long) options.valueOf("id");
			this.verbose = options.has("verbose");
			this.jdl = options.has("jdl");
			this.sort_desc = options.has("desc");
			this.toSE = (String) options.valueOf("destination");
		}
		catch (final OptionException e) {
			commander.setReturnCode(ErrNo.EINVAL, e.getMessage());
			setArgumentsOk(false);
		}
		catch (@SuppressWarnings("unused") final NumberFormatException e) {
			commander.setReturnCode(ErrNo.EINVAL, "Please provide a valid number for -list argument");
			setArgumentsOk(false);
		}
	}
}
