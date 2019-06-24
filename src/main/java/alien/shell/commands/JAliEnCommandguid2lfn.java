package alien.shell.commands;

import java.util.List;

import alien.catalogue.GUID;
import joptsimple.OptionException;

/**
 * @author ron
 * @since June 4, 2011
 */
public class JAliEnCommandguid2lfn extends JAliEnBaseCommand {

	/**
	 * entry the call is executed on, either representing a LFN
	 */
	private String guidName = null;

	/**
	 * execute the lfn2guid
	 */
	@Override
	public void run() {
		final GUID guid = commander.c_api.getGUID(guidName, false, true);

		commander.outNextResult();
		if (guid == null)
			commander.printOutln("Could not get the GUID [" + guidName + "].");
		else
			if (guid.getLFNs() != null && guid.getLFNs().iterator().hasNext())
				commander.printOutln(padRight(guid.guid + "", 40) + guid.getLFNs().iterator().next().getCanonicalName());
			else
				commander.printErrln("No LFNs are associated to this GUID [" + guid.guid + "].");
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("guid2lfn", "<GUID>"));
		commander.printOutln();
	}

	/**
	 * guid2lfn cannot run without arguments
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
	public JAliEnCommandguid2lfn(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);

		if (alArguments.size() != 1) {
			// help will be printed by the commander anyway since canRunWithoutArguments=false
			return;
		}

		guidName = alArguments.get(0);
	}

}
