package alien.shell.commands;

import java.util.ArrayList;
import java.util.List;

import alien.api.taskQueue.TaskQueueApiUtils;
import alien.quotas.FileQuota;
import alien.shell.ErrNo;
import alien.user.AliEnPrincipal;
import alien.user.UserFactory;
import joptsimple.OptionException;

/**
 *
 */
public class JAliEnCommandfquota extends JAliEnBaseCommand {
	private boolean isAdmin;
	private String command;

	private AliEnPrincipal user_to_set;
	private String param_to_set;
	private Long value_to_set;

	private List<String> usersToList = new ArrayList<>();

	@Override
	public void run() {
		if (command.equals("list")) {
			boolean first = true;

			for (final String username : usersToList) {
				final FileQuota q = TaskQueueApiUtils.getFileQuota(UserFactory.getByUsername(username));

				if (q == null) {
					commander.setReturnCode(ErrNo.ENODATA, "File quota information not available for " + username);
					continue;
				}

				commander.printOut("username", q.user);
				commander.printOut("totalSize", String.valueOf(q.totalSize));
				commander.printOut("maxTotalSize", String.valueOf(q.maxTotalSize));
				commander.printOut("nbFiles", String.valueOf(q.nbFiles));
				commander.printOut("maxNbFiles", String.valueOf(q.maxNbFiles));
				commander.printOut("maxNbFiles", String.valueOf(q.maxNbFiles));

				if (first)
					first = false;
				else
					commander.printOutln();

				commander.printOutln(q.toString());

				commander.outNextResult();
			}
		}

		if (command.equals("set")) {
			if (this.param_to_set == null) {
				commander.setReturnCode(ErrNo.EINVAL, "Parameter name cannot be null");
				return;
			}
			else if (this.value_to_set == null || this.value_to_set.longValue() == 0) {
				commander.setReturnCode(ErrNo.EINVAL, "Value cannot be null");
				printHelp();
				return;
			}

			// run the update
			if (TaskQueueApiUtils.setFileQuota(user_to_set, this.param_to_set, this.value_to_set.toString()))
				commander.printOutln("Result: ok, " + this.param_to_set + "=" + this.value_to_set.toString() + " for user=" + user_to_set);
			else
				commander.setReturnCode(ErrNo.EREMOTEIO, "Failed to set " + this.param_to_set + "=" + this.value_to_set.toString() + " for user=" + user_to_set);
		}
	}

	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln("fquota: Displays information about File Quotas.");
		commander.printOutln(helpUsage("fquota", "list [-<options>]"));
		commander.printOutln("Options:");
		commander.printOutln("  -unit = B|K|M|G: unit of file size");
		if (this.isAdmin) {
			commander.printOutln();
			commander.printOutln("fquota set <user> <field> <value> - set the user quota for file catalogue");
			commander.printOutln("  (maxNbFiles, maxTotalSize(Byte))");
			commander.printOutln("  use <user>=% for all users");
		}
		commander.printOutln();
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
	public JAliEnCommandfquota(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);
		this.isAdmin = commander.getUser().canBecome("admin");

		if (alArguments.size() == 0)
			return;

		this.command = alArguments.get(0);

		if (this.command.equals("set")) {
			if (alArguments.size() == 4) {
				this.user_to_set = UserFactory.getByUsername(alArguments.get(1));
				final String param = alArguments.get(2);

				if (!FileQuota.canUpdateField(param)) {
					commander.setReturnCode(ErrNo.ENOSYS, "Not a field you can set: " + param + ", only these ones can be modified: " + FileQuota.allowed_to_update);
					setArgumentsOk(false);
					return;
				}

				this.param_to_set = param;

				try {
					this.value_to_set = Long.valueOf(alArguments.get(3));
				}
				catch (@SuppressWarnings("unused") final Exception e) {
					commander.setReturnCode(ErrNo.EINVAL, "Illegal value for " + param_to_set + " : " + alArguments.get(3));
					setArgumentsOk(false);
					return;
				}
			}
			else {
				setArgumentsOk(false);
			}
		}
		else if (this.command.equals("list")) {
			for (int i = 1; i < alArguments.size(); i++)
				usersToList.add(alArguments.get(i));

			if (usersToList.size() == 0)
				usersToList.add(commander.getUsername());
		}
		else {
			setArgumentsOk(false);
		}
	}
}
