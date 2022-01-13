package alien.shell.commands;

import java.util.List;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;
import alien.shell.ErrNo;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * @author ibrinzoi
 * @since 2021-12-08
 */
public class JAliEnCommandmoveDirectory extends JAliEnBaseCommand {

	String file;

	@Override
	public void run() {
		final LFN currentDir = commander.getCurrentDir();

		final String absolutePath = FileSystemUtils.getAbsolutePath(commander.user.getName(),
					currentDir != null ? currentDir.getCanonicalName() : null, file);

		commander.printOutln(commander.c_api.moveDirectory(absolutePath));
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("moveDirectory", "<directory>"));
		commander.printOutln();
	}

	/**
	 * This command must have one argument
	 *
	 * @return <code>false</code>
	 */
	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * Constructor needed for the command factory in JAliEnCOMMander
	 *
	 * @param commander
	 *
	 * @param alArguments
	 *            the arguments of the command
	 * @throws OptionException
	 */
	public JAliEnCommandmoveDirectory(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);
		try {
			final OptionParser parser = new OptionParser();
			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			this.file = optionToString(options.nonOptionArguments()).get(0);
		}
		catch (final OptionException e) {
			commander.setReturnCode(ErrNo.EINVAL, e.getMessage());
			setArgumentsOk(false);
		}
	}

}