package alien.shell.commands;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;

import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import lazyj.Log;

/**
 * @author ron
 * @since June 4, 2011 running ls command with possible options <br />
 *        -l : long format <br />
 *        -a : show hidden .* files <br />
 *        -F : add trailing / to directory names <br />
 *        -b : print in GUID format <br />
 *        -c : print canonical paths <br />
 */

// FIXME: freezes on passing incorrect arguments, I tried ls -R

public class JAliEnCommandls extends JAliEnBaseCommand {

	/**
	 * marker for -l argument : long format
	 */
	private boolean bL = false;

	/**
	 * marker for -a argument : show hidden .files
	 */
	private boolean bA = false;

	/**
	 * marker for -F argument : add trailing / to directory names
	 */
	private boolean bF = false;

	/**
	 * marker for -c argument : print canonical paths
	 */
	private boolean bC = false;

	/**
	 * marker for -b argument : print in GUID format
	 */
	private boolean bB = false;

	private List<String> alPaths = null;

	/**
	 * list of the LFNs that came up by the ls command
	 */
	private List<LFN> directory = null;

	/**
	 * execute the ls
	 */
	@Override
	public void run() {

		final int iDirs = alPaths.size();

		if (iDirs == 0)
			alPaths.add(commander.getCurrentDirName());

		final StringBuilder pathsNotFound = new StringBuilder();

		final List<String> expandedPaths = new ArrayList<>(alPaths.size());

		for (final String sPath : alPaths) {
			final String absolutePath = FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), sPath);

			final List<String> sources = FileSystemUtils.expandPathWildCards(absolutePath, commander.user);

			expandedPaths.addAll(sources);
		}

		for (final String sPath : expandedPaths) {
			Log.log(Log.INFO, "LS: listing for directory = \"" + sPath + "\"");

			final List<LFN> subdirectory = commander.c_api.getLFNs(sPath);

			if (subdirectory != null) {
				if (directory == null)
					directory = new ArrayList<>(subdirectory);
				else
					directory.addAll(subdirectory);

				for (final LFN localLFN : subdirectory) {

					logger.log(Level.FINE, localLFN.toString());

					if (!bA && localLFN.getFileName().startsWith("."))
						continue;

					if (bB && localLFN.isDirectory())
						continue;

					if (out.isRootPrinter()) {
						out.nextResult();

						out.setField("permissions", FileSystemUtils.getFormatedTypeAndPerm(localLFN));
						out.setField("user", localLFN.owner);
						out.setField("group", localLFN.gowner);
						out.setField("size", String.valueOf(localLFN.size));
						out.setField("ctime", String.valueOf(localLFN.ctime.getTime() / 1000));
						out.setField("name", localLFN.getFileName() + (bF && localLFN.isDirectory() ? "/" : ""));
						out.setField("path", localLFN.getCanonicalName() + (bF && localLFN.isDirectory() ? "/" : ""));

						if (localLFN.guid != null)
							out.setField("guid", localLFN.guid.toString().toUpperCase());
					}
					else {
						String ret = "";
						if (bB)
							ret += localLFN.guid.toString().toUpperCase() + padSpace(3) + localLFN.getName();
						else
							if (bC)
								ret += localLFN.getCanonicalName();
							else {
								if (bL)
									ret += FileSystemUtils.getFormatedTypeAndPerm(localLFN) + padSpace(3) + padLeft(localLFN.owner, 8) + padSpace(1) + padLeft(localLFN.gowner, 8) + padSpace(1)
											+ padLeft(String.valueOf(localLFN.size), 12) + padSpace(1) + format(localLFN.ctime) + padSpace(4) + localLFN.getFileName();

								else
									ret += localLFN.getFileName();

								if (bF && (localLFN.type == 'd'))
									ret += "/";
							}

						logger.info("LS line : " + ret);

						if (!isSilent())
							out.printOutln(ret);
					}
				}
			}
			else {
				if (pathsNotFound.length() > 0)
					pathsNotFound.append(", ");

				pathsNotFound.append(sPath);

				logger.log(Level.SEVERE, "No such file or directory: [" + sPath + "]");
				out.printOutln("No such file or directory: [" + sPath + "]");
			}

		}

		if (pathsNotFound.length() > 0)
			out.setReturnCode(1, "No such file or directory: [" + pathsNotFound + "]");

		// if (out.isRootPrinter())
		// out.setReturnArgs(deserializeForRoot());
	}

	private static final DateFormat formatter = new SimpleDateFormat("MMM dd HH:mm");

	private static synchronized String format(final Date d) {
		return formatter.format(d);
	}

	/**
	 * get the directory listing of the ls
	 *
	 * @return list of the LFNs
	 */
	protected List<LFN> getDirectoryListing() {
		return directory;
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		out.printOutln();
		out.printOutln(helpUsage("ls", "[-options] [<directory>]"));
		out.printOutln(helpStartOptions());
		out.printOutln(helpOption("-l", "long format"));
		out.printOutln(helpOption("-a", "show hidden .* files"));
		out.printOutln(helpOption("-F", "add trailing / to directory names"));
		out.printOutln(helpOption("-b", "print in guid format"));
		out.printOutln(helpOption("-c", "print canonical paths"));
		out.printOutln();
	}

	/**
	 * ls can run without arguments
	 *
	 * @return <code>true</code>
	 */
	@Override
	public boolean canRunWithoutArguments() {
		return true;
	}

	/**
	 * Constructor needed for the command factory in commander
	 *
	 * @param commander
	 * @param out
	 *
	 * @param alArguments
	 *            the arguments of the command
	 * @throws OptionException
	 */
	public JAliEnCommandls(final JAliEnCOMMander commander, final UIPrintWriter out, final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);
		try {

			final OptionParser parser = new OptionParser();

			parser.accepts("l");
			parser.accepts("bulk");
			parser.accepts("b");
			parser.accepts("a");
			parser.accepts("F");
			parser.accepts("c");

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			alPaths = optionToString(options.nonOptionArguments());

			bL = options.has("l");
			// bBulk = options.has("bulk");
			bB = options.has("b");
			bA = options.has("a");
			bF = options.has("F");
			bC = options.has("c");
		} catch (final OptionException e) {
			printHelp();
			throw e;
		}
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();

		sb.append("\n { JAliEnCommandls received\n");
		sb.append("Arguments: ");

		if (bL)
			sb.append(" -l ");
		if (bA)
			sb.append(" -a ");
		if (bF)
			sb.append(" -f ");
		if (bC)
			sb.append(" -c ");
		if (bB)
			sb.append(" -b ");

		sb.append("}");

		return sb.toString();
	}
}
