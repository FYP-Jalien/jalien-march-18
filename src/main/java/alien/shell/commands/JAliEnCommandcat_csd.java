package alien.shell.commands;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import alien.shell.ErrNo;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import lazyj.Format;
import lazyj.Utils;

/**
 * @author mmmartin
 * @since November 27, 2018
 */
public class JAliEnCommandcat_csd extends JAliEnBaseCommand {

	private boolean bN = false;
	private boolean bE = false;
	private boolean bB = false;
	private boolean bT = false;
	private boolean bO = false;
	private ArrayList<String> alPaths = null;

	@Override
	public void run() {
		for (final String eachFileName : alPaths) {
			final File fout = catFile(eachFileName);
			int count = 0;
			if (fout != null && fout.exists() && fout.isFile() && fout.canRead()) {
				final String content = Utils.readFile(fout.getAbsolutePath());
				if (content != null) {
					final BufferedReader br = new BufferedReader(new StringReader(content));

					String line;

					try {
						while ((line = br.readLine()) != null) {
							if (bO)
								try (FileWriter fstream = new FileWriter(eachFileName); BufferedWriter o = new BufferedWriter(fstream)) {
									o.write(content);
								}

							if (bN) {
								commander.printOut("count", count + "");
								commander.printOut(++count + "  ");
							}
							else if (bB)
								if (line.trim().length() > 0) {
									commander.printOut("count", count + "");
									commander.printOut(++count + "  ");
								}
							if (bT)
								line = Format.replace(line, "\t", "^I");

							commander.printOut("value", line);
							commander.printOut(line);
							if (bE) {
								commander.printOut("value", "$");
								commander.printOut("$");
							}

							commander.printOutln();
						}

					}
					catch (@SuppressWarnings("unused") final IOException ioe) {
						// ignore, cannot happen
					}
				}
				else
					commander.setReturnCode(ErrNo.EIO, "Could not read the contents of " + fout.getAbsolutePath());
			}
			else
				commander.setReturnCode(ErrNo.EREMOTEIO, "Not able to get the file: " + eachFileName);
		}
	}

	/**
	 * @param fileName
	 *            catalogue file name to cat
	 * @return file handle for downloaded file
	 */
	public File catFile(final String fileName) {
		final ArrayList<String> args = new ArrayList<>(2);
		args.add("-t");
		args.add(fileName);

		JAliEnCommandcp_csd cp;
		try {
			cp = (JAliEnCommandcp_csd) JAliEnCOMMander.getCommand("cp_csd", new Object[] { commander, args });
		}
		catch (final Exception e) {
			e.printStackTrace();
			return null;
		}

		silent();

		try {
			final Thread backgroundCP = new Thread(cp);
			backgroundCP.start();
			while (backgroundCP.isAlive()) {
				Thread.sleep(500);
				commander.pending();
			}
		}
		catch (final Exception e) {
			e.printStackTrace();
			return null;
		}

		verbose();

		return cp.getOutputFile();
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("cat", "[-options] [<filename>]"));
		commander.printOutln(helpStartOptions());
		commander.printOutln(helpOption("-o", "outputfilename"));
		commander.printOutln(helpOption("-n", "number all output lines"));
		commander.printOutln(helpOption("-b", "number nonblank output lines"));
		commander.printOutln(helpOption("-E", "shows ends - display $ at end of each line number"));
		commander.printOutln(helpOption("-T", "show tabs -display TAB characters as ^I"));
		commander.printOutln();
	}

	/**
	 * cat cannot run without arguments
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
	public JAliEnCommandcat_csd(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);

		try {

			final OptionParser parser = new OptionParser();

			parser.accepts("o").withRequiredArg();
			parser.accepts("n");
			parser.accepts("b");
			parser.accepts("E");
			parser.accepts("T");

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			alPaths = new ArrayList<>(options.nonOptionArguments().size());
			alPaths.addAll(optionToString(options.nonOptionArguments()));

			bO = options.has("o");
			bN = options.has("n");
			bB = options.has("b");
			bE = options.has("E");
			bT = options.has("T");

		}
		catch (final OptionException e) {
			commander.setReturnCode(ErrNo.EINVAL, e.getMessage());
			setArgumentsOk(false);
		}
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();

		sb.append("\n { JAliEnCommandcat_csd received\n");
		sb.append("Arguments: ");

		if (bO)
			sb.append(" -o ");
		if (bN)
			sb.append(" -n ");
		if (bT)
			sb.append(" -T ");
		if (bB)
			sb.append(" -b ");
		if (bE)
			sb.append(" -E ");

		sb.append("}");

		return sb.toString();
	}
}
