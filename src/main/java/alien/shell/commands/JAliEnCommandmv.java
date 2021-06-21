package alien.shell.commands;

import java.util.List;

import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;
import alien.shell.ErrNo;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * @author ron
 * @since June 4, 2011
 * @author sraje (Shikhar Raje, IIIT Hyderabad)
 * @since Modified July 1, 2012
 */
public class JAliEnCommandmv extends JAliEnBaseCommand {

	private String[] sources = null;

	private String target = null;

	/**
	 * Size of the argument list.
	 */
	int size = 0;

	@Override
	public void run() {
		final String fullTarget = FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), target);
		LFN tLFN = commander.c_api.getLFN(fullTarget, false);

		if (size > 2) {
			if ((tLFN != null && tLFN.isDirectory()))
				for (int i = 0; i <= size - 2; i++) {
					final String fullSource = FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), sources[i]);
					final LFN sLFN = commander.c_api.getLFN(fullSource, false);

					if (sLFN.isFile() || sLFN.isDirectory()) {
						if (commander.c_api.moveLFN(sLFN.getCanonicalName(), fullTarget + "/" + sLFN.getFileName()) == null)
							commander.setReturnCode(ErrNo.EIO, "Failed to move " + sources[i] + " to " + fullTarget);
					}
				}
			else if (tLFN == null) {
				tLFN = commander.c_api.createCatalogueDirectory(fullTarget, true);
				for (int i = 0; i <= size - 2; i++) {
					final String fullSource = FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), sources[i]);
					final LFN sLFN = commander.c_api.getLFN(fullSource, false);

					if (sLFN.isFile() || sLFN.isDirectory()) {
						if (commander.c_api.moveLFN(sLFN.getCanonicalName(), fullTarget + "/" + sLFN.getFileName()) == null)
							commander.setReturnCode(ErrNo.EIO, "Failed to move " + sources[i] + " to " + fullTarget + "/" + sLFN.getFileName());
					}
				}
			}
			else {
				commander.setReturnCode(ErrNo.EINVAL,
						"If there are more than 2 arguments, then last one must be an existing direcetory OR a location that does not exist and can be made as new directory");
			}
		}
		else if (size == 2) {
			final String fullSource = FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), sources[0]);
			final LFN sLFN = commander.c_api.getLFN(fullSource, false);

			if (sLFN == null) {
				commander.setReturnCode(ErrNo.EINVAL, "Source cannot be found: " + fullSource);
			}
			else {
				if (tLFN != null) {
					if (sLFN.isFile() && tLFN.isFile()) {
						// TODO File overwrite mechanism
						tLFN = commander.c_api.moveLFN(sLFN.getCanonicalName(), fullTarget + "_backup");
					}
					else if ((sLFN.isDirectory() && tLFN.isDirectory()) || (sLFN.isFile() && tLFN.isDirectory())) {
						tLFN = commander.c_api.moveLFN(sLFN.getCanonicalName(), fullTarget + "/" + sLFN.getFileName());
					}
					else {
						commander.setReturnCode(ErrNo.EINVAL,
								"If there are 2 arguments then only:\n1. File to file\n2. File to directory\n3. Directory to Directory\n is supported\nMost probably a directory to file mv is being attempted");
					}
				}
				else {
					if (target.contains("/") && target.endsWith("/")) {
						tLFN = commander.c_api.createCatalogueDirectory(fullTarget, true);
						tLFN = commander.c_api.moveLFN(sLFN.getCanonicalName(), fullTarget + "/" + sLFN.getFileName());
					}
					else
						tLFN = commander.c_api.moveLFN(sLFN.getCanonicalName(), fullTarget);
				}

				if (tLFN == null)
					commander.setReturnCode(ErrNo.EIO, "Failed to move " + sources[0] + " to " + fullTarget);
			}
		}
		else if (size == 0 || size == 1)
			printHelp();
	}

	/**
	 * printout the help info, none for this command
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("mv", " <LFN>  <newLFN>"));
		commander.printOutln();
	}

	/**
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
	 */
	public JAliEnCommandmv(final JAliEnCOMMander commander, final List<String> alArguments) {
		super(commander, alArguments);

		try {
			final OptionParser parser = new OptionParser();

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			final List<String> nonOptionArguments = optionToString(options.nonOptionArguments());

			size = nonOptionArguments.size();

			// need at least one source and one target
			if (size > 1) {
				sources = new String[size - 1];
				for (int i = 0; i <= (size - 2); i++)
					sources[i] = nonOptionArguments.get(i);

				target = nonOptionArguments.get(size - 1);
			}
		}
		catch (final OptionException e) {
			commander.setReturnCode(ErrNo.EINVAL, e.getMessage());
			setArgumentsOk(false);
		}
	}
}