package alien.shell.commands;

import java.util.List;

import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * @author ron
 * @since Nov 24, 2011
 */
public class JAliEnCommandtype extends JAliEnBaseCommand {

	private String sPath = null;

	/**
	 * the LFN for path
	 */
	private LFN lfn = null;

	/**
	 * execute the type
	 */
	@Override
	public void run() {

		if (sPath != null)

			lfn = commander.c_api.getLFN(FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), sPath));

		if (lfn == null) {
			commander.printOutln("No such file or directory: [" + sPath + "]");
			return;
		}

		String ret = "";
		if (lfn.isFile()) {
			ret += "file";
			commander.printOut("type", "file");
		}
		else
			if (lfn.isDirectory()) {
				ret += "directory";
				commander.printOut("type", "directory");
			}
			else
				if (lfn.isCollection()) {
					ret += "collection";
					commander.printOut("type", "collection");
				}
		logger.info("Type line : " + ret);
		commander.printOutln(ret);
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		// ignore
	}

	/**
	 * ls can run without arguments
	 *
	 * @return <code>true</code>
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
	public JAliEnCommandtype(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);

		final OptionParser parser = new OptionParser();

		parser.accepts("z");
		parser.accepts("s");

		final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

		if (options.has("s"))
			silent();

		if (options.nonOptionArguments().size() != 1) {
			setArgumentsOk(false);
			return;
		}

		sPath = options.nonOptionArguments().get(0).toString();

	}

}
