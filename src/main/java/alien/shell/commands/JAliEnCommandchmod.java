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

/**
 * chmod
 */
public class JAliEnCommandchmod extends JAliEnBaseCommand {
	private String newMod;
	private List<String> files;
	private boolean recursive;

	@Override
	public void run() {
		if (this.newMod == null || this.files == null || this.files.size() < 1) {
			commander.setReturnCode(ErrNo.EINVAL, "No user or file entered");
			return;
		}

		final LFN currentDir = commander.getCurrentDir();

		for (final String file : files) {
			final String absolutePath = FileSystemUtils.getAbsolutePath(commander.user.getName(),
					currentDir != null ? currentDir.getCanonicalName() : null, file);

			final List<String> chmodTargets = FileSystemUtils.expandPathWildCards(absolutePath, commander.user);

			if (chmodTargets == null || chmodTargets.isEmpty()) {
				commander.setReturnCode(ErrNo.ENOENT, file);
				return;
			}

			for (final String path : chmodTargets) {
				// run chmod command
				final HashMap<String, Boolean> results = commander.c_api.chmodLFN(path, newMod, this.recursive);

				if (results == null) {
					commander.setReturnCode(ErrNo.EIO, "Failed to chmod file " + path);
					continue;
				}

				for (final Map.Entry<String, Boolean> entry : results.entrySet()) {
					final Boolean b = entry.getValue();

					if (b == null || !b.booleanValue()) {
						commander.setReturnCode(ErrNo.EIO, entry.getKey() + ": unable to chmod");
					}
				}
			}
		}
	}

	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln("Usage: chmod -R <octal mode> <path> [<path>...]");
		commander.printOutln();
		commander.printOutln("Changes the access mode for a catalogue path");
		commander.printOutln("-R : do a recursive chmod starting from the given path");
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
	public JAliEnCommandchmod(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
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

			newMod = params.get(0);

			// get file
			this.files = params.subList(1, params.size());
		}
		catch (final OptionException e) {
			commander.setReturnCode(ErrNo.EINVAL, e.getMessage());
			setArgumentsOk(false);
		}
	}

}
