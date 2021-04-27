package alien.shell.commands;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import alien.api.Dispatcher;
import alien.api.ServerException;
import alien.api.catalogue.RemoveLFNfromString;
import alien.catalogue.FileSystemUtils;
import alien.shell.ErrNo;
import alien.user.UsersHelper;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * @author ron
 * @since Oct 27, 2011
 * @author sraje (Shikhar Raje, IIIT Hyderabad)
 * @since June 25, 2012
 */
public class JAliEnCommandrm extends JAliEnBaseCommand {
	/**
	 * Variable for -f "Force" flag and -i "Interactive" flag. These 2 flags contradict each other, and hence only 1 variable for them. (Source: GNU Man pages for rm).
	 *
	 * @val True if interactive; False if forced.
	 */
	boolean bIF = false;

	/**
	 * Variable for -r "Recursive" flag
	 */
	boolean bR = false;

	/**
	 * Variable for -v "Verbose" flag
	 */
	boolean bV = false;

	private List<String> alPaths = null;

	@Override
	public void run() {
		final List<String> expandedPaths = new ArrayList<>(alPaths.size());

		final String username = commander.user.getName();

		final String cwd = commander.getCurrentDirName();

		for (final String path : alPaths) {
			if (path == null) {
				logger.log(Level.WARNING, "Could not get an LFN");
				return;
			}

			final String absolutePath = FileSystemUtils.getAbsolutePath(username, cwd, path);
			final List<String> sources = FileSystemUtils.expandPathWildCards(absolutePath, commander.user);

			if (sources != null && !sources.isEmpty())
				expandedPaths.addAll(sources);
			else
				commander.setReturnCode(ErrNo.ENOENT, path);
		}

		final String homeDir = UsersHelper.getHomeDir(username);

		for (final String sPath : expandedPaths) {
			final String fullPath = FileSystemUtils.getAbsolutePath(username, cwd, sPath);

			if (cwd.startsWith(fullPath + "/")) {
				commander.setReturnCode(ErrNo.EINVAL, "Cannot delete the directory you are in");
				continue;
			}

			if (homeDir.startsWith(fullPath + "/")) {
				commander.setReturnCode(ErrNo.EINVAL, "Cannot delete your home dir");
				continue;
			}

			final RemoveLFNfromString rlfn = new RemoveLFNfromString(commander.getUser(), fullPath, bR);

			try {
				final RemoveLFNfromString a = Dispatcher.execute(rlfn); // Remember, all checking is being done server side now.

				if (!a.wasRemoved())
					commander.setReturnCode(ErrNo.EIO, "Failed to remove [" + fullPath + "]");
			}
			catch (final ServerException e) {
				e.getCause().printStackTrace();
				commander.setReturnCode(ErrNo.EIO, "Failed to remove [" + fullPath + "]");
			}
		}
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("rm", " <LFN> [<LFN>[,<LFN>]]"));
		commander.printOutln(helpStartOptions());
		commander.printOutln(helpOption("-f", "ignore nonexistent files, never prompt"));
		commander.printOutln(helpOption("-r, -R", "remove directories and their contents recursively"));
		commander.printOutln(helpOption("-i", "prompt before every removal (for JSh clients)"));
		commander.printOutln();
	}

	/**
	 * rm cannot run without arguments
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
	public JAliEnCommandrm(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);
		try {
			final OptionParser parser = new OptionParser();
			parser.accepts("i");
			parser.accepts("f");
			parser.accepts("r");
			parser.accepts("R");
			parser.accepts("v");

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			bIF = options.has("i");
			bIF = !options.has("f");
			bR = options.has("r") || options.has("R");
			bV = options.has("v");

			alPaths = optionToString(options.nonOptionArguments());
		}
		catch (final OptionException e) {
			commander.setReturnCode(ErrNo.EINVAL, e.getMessage());
			setArgumentsOk(false);
		}
	}
}
