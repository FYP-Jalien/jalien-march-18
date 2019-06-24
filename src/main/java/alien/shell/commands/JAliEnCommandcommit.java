package alien.shell.commands;

import java.util.Arrays;
import java.util.List;

import alien.catalogue.PFN;

/**
 * @author ron
 * @since June 4, 2011
 */
public class JAliEnCommandcommit extends JAliEnBaseCommand {

	/**
	 * commit request raw envelope
	 */
	private String rawenvelope = "";

	/**
	 * commit request lfn
	 */
	private String lfn = "";

	/**
	 * commit request size
	 */
	private int size = 0;

	/**
	 * commit request permissions
	 */
	private String perm = "";

	/**
	 * commit request expiration
	 */
	private String expire = "";

	/**
	 * commit request PFN
	 */
	private String pfn = "";

	/**
	 * commit request SE
	 */
	private String se = "";

	/**
	 * commit request GUID
	 */
	private String guid = "";

	/**
	 * commit request MD5
	 */
	private String md5 = "";

	/**
	 * execute the commit
	 */
	@Override
	public void run() {

		final List<PFN> pfns;

		if (rawenvelope.contains("signature="))
			pfns = commander.c_api.registerEnvelopes(Arrays.asList(rawenvelope));
		else
			pfns = commander.c_api.registerEncryptedEnvelope(rawenvelope, size, md5);

		String ret = "";
		if (pfns != null && pfns.size() > 0) {
			commander.printOut("lfn", "1");
			ret += lfn + padSpace(1) + "1";
		}
		else {
			commander.printOut("lfn", "0");
			ret += lfn + padSpace(1) + "0";
		}
		logger.info("Commit line : " + ret);

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
	 * get cannot run without arguments
	 *
	 * @return <code>false</code>
	 */
	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * serialize return values for gapi/root
	 *
	 * @param status
	 *
	 * @return serialized return
	 */
	public String deserializeForRoot(final boolean status) {
		final StringBuilder str = new StringBuilder();

		if (status)
			str.append("<").append("lfn").append(">").append(lfn).append("0").append("<").append("lfn").append(">");
		else
			str.append("<").append("lfn").append(">").append(lfn).append("1").append("<").append("lfn").append(">");
		return str.toString();
	}

	/**
	 * return RootPrintWriter.columnseparator + RootPrintWriter.fielddescriptor + lfn + RootPrintWriter.fieldseparator + "0";
	 *
	 * return RootPrintWriter.columnseparator + RootPrintWriter.fielddescriptor + lfn + RootPrintWriter.fieldseparator + "1"; }
	 *
	 * /** Constructor needed for the command factory in commander
	 *
	 * @param commander
	 *
	 * @param alArguments
	 *            the arguments of the command
	 */
	public JAliEnCommandcommit(final JAliEnCOMMander commander, final List<String> alArguments) {
		super(commander, alArguments);

		final java.util.ListIterator<String> arg = alArguments.listIterator();

		if (arg.hasNext()) {
			rawenvelope = arg.next();
			if (arg.hasNext())
				try {
					size = Integer.parseInt(arg.next());
				} catch (@SuppressWarnings("unused") final NumberFormatException e) {
					// ignore
				}
			if (arg.hasNext())
				lfn = arg.next();
			if (arg.hasNext())
				perm = arg.next();
			if (arg.hasNext())
				expire = arg.next();
			if (arg.hasNext())
				pfn = arg.next();
			if (arg.hasNext())
				se = arg.next();
			if (arg.hasNext())
				guid = arg.next();
			if (arg.hasNext())
				md5 = arg.next();

		}
	}
}
