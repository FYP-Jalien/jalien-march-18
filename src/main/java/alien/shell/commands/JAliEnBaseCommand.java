package alien.shell.commands;

import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import alien.api.DispatchSSLClient;
import alien.config.ConfigUtils;

/**
 * @author ron
 * @since June 4, 2011
 */
public abstract class JAliEnBaseCommand implements Runnable {

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(DispatchSSLClient.class.getCanonicalName());

	/**
	 * The JAliEnCOMMander
	 */
	protected JAliEnCOMMander commander;

	/**
	 *
	 */
	protected final List<String> alArguments;

	private final static int padHelpUsage = 20;

	private final static int padHelpOption = 21;

	private boolean argumentsOk = true;

	/**
	 * Constructor based on the array received from the request
	 *
	 * @param commander
	 * @param alArguments
	 */
	public JAliEnBaseCommand(final JAliEnCOMMander commander, final List<String> alArguments) {
		this.commander = commander;
		this.alArguments = alArguments;
	}

	/**
	 * Abstract class to execute the command / run the thread
	 *
	 */
	@Override
	public abstract void run();

	/**
	 * Abstract class to printout the help info of the command
	 *
	 */
	public abstract void printHelp();

	/**
	 * @param name
	 * @return usage tag for help
	 */
	public static String helpUsage(final String name) {
		return helpUsage(name, "");
	}

	/**
	 * @return <code>true</code> if the arguments are ok and the command can be executed
	 */
	public boolean areArgumentsOk() {
		return argumentsOk;
	}

	/**
	 * @param allOk set the flag
	 */
	protected void setArgumentsOk(boolean allOk) {
		argumentsOk = allOk;
	}

	/**
	 * @param name
	 * @param description
	 * @return usage tag for help
	 */
	public static String helpUsage(final String name, final String description) {
		String desc = description;

		if (desc != null && desc.length() > 0)
			desc = padSpace(3) + desc;
		else
			desc = "";

		return padRight("usage: " + name + desc, padHelpUsage);
	}

	/**
	 * @return options tag
	 */
	public static final String helpStartOptions() {
		return "\noptions:";
	}

	/**
	 * @param opt
	 * @return option tag for help
	 */
	public static String helpOption(final String opt) {
		return helpOption(opt, "");
	}

	/**
	 * @param opt
	 * @param description
	 * @return option tag for help
	 */
	public static final String helpOption(final String opt, final String description) {
		String desc = description;

		if (desc != null && desc.length() > 0)
			desc = "  :  " + desc;
		else
			desc = "";

		return padSpace(padHelpUsage) + padRight(opt, padHelpOption) + desc;
	}

	/**
	 * @param desc
	 * @return option tag for help
	 */
	public static final String helpParameter(final String desc) {
		return padSpace(padHelpUsage) + desc;
	}

	/**
	 * Abstract class to check if this command can run without arguments
	 *
	 * @return true if this command can run without arguments
	 */
	public abstract boolean canRunWithoutArguments();

	/**
	 * the command's silence trigger
	 */
	private boolean silent = false;

	/**
	 * @return <code>true</code> if the command was silenced
	 */
	public final boolean isSilent() {
		return silent;
	}

	/**
	 * set command's silence trigger
	 */
	public final void silent() {
		silent = true;
	}

	/**
	 * set command's silence trigger
	 */
	public final void verbose() {
		silent = false;
	}

	/**
	 * @param s
	 * @param n
	 * @return left-padded string
	 */
	public static final String padLeft(final String s, final int n) {
		if (n <= 0)
			return s;

		return String.format("%1$" + n + "s", s);
	}

	/**
	 * @param s
	 * @param n
	 * @return right-padded string
	 */
	public static final String padRight(final String s, final int n) {
		if (n <= 0)
			return s;

		return String.format("%1$-" + n + "s", s);
	}

	/**
	 * @param n
	 * @return n count spaces as String
	 */
	public static final String padSpace(final int n) {
		final char[] c = new char[n];

		for (int a = 0; a < n; a++)
			c[a] = ' ';

		return new String(c);
	}

	/**
	 * @param n
	 * @return n count tabs as String
	 */
	public static final String padTab(final int n) {
		final char[] c = new char[n];

		for (int a = 0; a < n; a++)
			c[a] = '\t';

		return new String(c);
	}

	/**
	 * For the new jopt library, convert the options from generic objects to Strings
	 *
	 * @param options
	 * @return options as strings
	 */
	public static final List<String> optionToString(final List<?> options) {
		if (options == null)
			return null;

		final LinkedList<String> ret = new LinkedList<>();

		for (final Object o : options)
			ret.add(o.toString());

		return ret;
	}
}
