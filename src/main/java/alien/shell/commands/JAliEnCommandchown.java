package alien.shell.commands;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;
import alien.shell.ErrNo;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

// TODO check that the user has indeed permissions on the target entry, server side
// TODO validate that the user can become the target user name and the target role name (implicitly validating the strings)
// TODO recursive flag
// TODO check that groups/users exist

/**
 * chown
 */
public class JAliEnCommandchown extends JAliEnBaseCommand {
	private String user;
	private String group;
	private List<String> files;
	private boolean recursive;

	@Override
	public void run() {
		if (this.user == null || this.files == null || this.files.size() < 1) {
			commander.setReturnCode(ErrNo.EINVAL, "No user or file entered");
			return;
		}

		final LFN currentDir = commander.getCurrentDir();

		for (final String file : files) {
			final String absolutePath = FileSystemUtils.getAbsolutePath(commander.user.getName(),
					currentDir != null ? currentDir.getCanonicalName() : null, file);

			final List<String> chownTargets = FileSystemUtils.expandPathWildCards(absolutePath, commander.user);

			if (chownTargets == null || chownTargets.isEmpty()) {
				commander.setReturnCode(ErrNo.ENOENT, file);
				return;
			}

			for (final String path : chownTargets) {
				// run chown command
				final HashMap<String, Boolean> results = commander.c_api.chownLFN(path, this.user, this.group, this.recursive);

				if (results == null) {
					commander.setReturnCode(ErrNo.EIO, "Failed to chown file " + path);
					continue;
				}

				for (final Map.Entry<String, Boolean> entry : results.entrySet()) {
					final Boolean b = entry.getValue();

					if (b == null || !b.booleanValue()) {
						commander.setReturnCode(ErrNo.EIO, entry.getKey() + ": unable to chown");
					}
				}
			}
		}
	}

	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln("Usage: chown -R <user>[.<group>] <file>");
		commander.printOutln();
		commander.printOutln("Changes an owner or a group for a file");
		commander.printOutln("-R : do a recursive chown");
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
	public JAliEnCommandchown(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);
		try {
			final OptionParser parser = new OptionParser();
			parser.accepts("R");

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			final List<String> params = optionToString(options.nonOptionArguments());

			this.recursive = options.has("R");

			// check for at least 2 arguments
			if (params.size() < 2)
				return;

			// get user/group
			final String[] usergrp = params.get(0).split("\\.");

			this.user = usergrp[0];
			if (usergrp.length == 2)
				this.group = usergrp[1];

			// get file
			this.files = params.subList(1, params.size());
		}
		catch (final OptionException e) {
			commander.setReturnCode(ErrNo.EINVAL, e.getMessage());
			setArgumentsOk(false);
		}
	}

}
