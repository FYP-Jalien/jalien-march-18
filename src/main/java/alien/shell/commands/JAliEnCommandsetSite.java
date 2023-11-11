package alien.shell.commands;

import java.io.IOException;
import java.util.List;

import alien.config.ConfigUtils;
import alien.shell.ErrNo;
import joptsimple.OptionException;
import lazyj.Format;
import lazyj.Utils;

/**
 * @author costing
 * @since 2019-07-04
 */
public class JAliEnCommandsetSite extends JAliEnBaseCommand {

	private String targetSiteName;

	/**
	 * execute the command
	 */
	@Override
	public void run() {
		if (targetSiteName == null || targetSiteName.length() == 0) {
			commander.printOutln("Keeping the current close site: " + commander.getSite());
			return;
		}

		if (targetSiteName.equalsIgnoreCase("auto")) {
			final String autoSiteName = ConfigUtils.getCloseSite();

			if (autoSiteName != null && autoSiteName.length() > 0)
				targetSiteName = autoSiteName.trim();
			else {
				commander.setReturnCode(ErrNo.EREMOTEIO, "Could not map you to a site name at the moment, keeping previous value of " + commander.getSite());
				return;
			}
		}
		else {
			if (targetSiteName.indexOf(':') >= 0 || targetSiteName.indexOf('.') >= 0) {
				try {
					final String autoSiteName = Utils.download("http://alimonitor.cern.ch/services/getClosestSite.jsp?ip=" + Format.encode(targetSiteName), null);

					if (autoSiteName != null && autoSiteName.length() > 0)
						targetSiteName = autoSiteName.trim();
					else {
						commander.setReturnCode(ErrNo.EREMOTEIO, "Could not map " + targetSiteName + " to a site name at the moment, keeping previous value of " + commander.getSite());
						return;
					}
				}
				catch (final IOException ioe) {
					commander.setReturnCode(ErrNo.EAGAIN, "Could not retrieve the site name for " + targetSiteName + " from the external service: " + ioe.getMessage());
					return;
				}
			}
		}

		commander.printOutln("Setting close site to " + targetSiteName + " (was " + commander.getSite() + ")");

		commander.setSite(targetSiteName);
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("setSite", "[site name]"));
		commander.printOutln(helpStartOptions());
		commander.printOutln();
	}

	@Override
	public boolean canRunWithoutArguments() {
		return true;
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
	public JAliEnCommandsetSite(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);

		if (alArguments.size() > 0)
			targetSiteName = alArguments.get(0).trim();
	}
}
