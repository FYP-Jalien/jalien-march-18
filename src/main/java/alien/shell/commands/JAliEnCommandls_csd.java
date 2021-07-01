package alien.shell.commands;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;

import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN_CSD;
import alien.shell.ErrNo;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import lazyj.Log;

/**
 * @author mmmartin
 * @since November 23, 2018 running ls command in Cassandra with possible options <br />
 *        -l : long format <br />
 *        -a : show hidden .* files <br />
 *        -F : add trailing / to directory names <br />
 *        -b : print in UUID format <br />
 *        -c : print canonical paths <br />
 */

// FIXME: freezes on passing incorrect arguments, I tried ls -R

public class JAliEnCommandls_csd extends JAliEnBaseCommand {

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
	 * marker for -b argument : print in UUID format
	 */
	private boolean bB = false;

	private List<String> alPaths = null;

	/**
	 * list of the LFNCSDs that came up by the ls command
	 */
	private Collection<LFN_CSD> directory = null;

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

			if (sources != null && !sources.isEmpty())
				expandedPaths.addAll(sources);
			else
				commander.setReturnCode(ErrNo.ENOENT, sPath);
		}

		for (final String sPath : expandedPaths) {
			Log.log(Log.INFO, "LS: listing for directory = \"" + sPath + "\"");

			final Collection<LFN_CSD> subdirectory = commander.c_api.getLFNCSDs(sPath);

			if (subdirectory != null) {
				if (directory == null)
					directory = new ArrayList<>(subdirectory);
				else
					directory.addAll(subdirectory);

				for (final LFN_CSD localLFN : subdirectory) {

					logger.log(Level.FINE, localLFN.toString());

					if (!bA && localLFN.getName().startsWith("."))
						continue;

					if (bB && localLFN.isDirectory())
						continue;

					commander.outNextResult();
					String ret = "";
					if (bB) {
						if (localLFN.id != null) {
							commander.printOut("guid", localLFN.id.toString().toUpperCase());
							ret += localLFN.id.toString().toUpperCase() + padSpace(3) + localLFN.getName();
						}
					}
					else if (bC)
						ret += localLFN.getCanonicalName();
					else {
						if (bL) {
							commander.printOut("permissions", FileSystemUtils.getFormatedTypeAndPerm(localLFN));
							commander.printOut("user", localLFN.owner);
							commander.printOut("group", localLFN.gowner);
							commander.printOut("size", String.valueOf(localLFN.size));
							commander.printOut("ctime", localLFN.ctime != null ? String.valueOf(localLFN.ctime.getTime()) : "");
							ret += FileSystemUtils.getFormatedTypeAndPerm(localLFN) + padSpace(3) + padLeft(localLFN.owner, 8) + padSpace(1) + padLeft(localLFN.gowner, 8) + padSpace(1)
									+ padLeft(String.valueOf(localLFN.size), 12) + padSpace(1) + format(localLFN.ctime) + padSpace(4) + localLFN.getName();
						}
						else {
							ret += localLFN.getName();
						}

						if (bF && (localLFN.type == 'd'))
							ret += "/";

						commander.printOut("name", localLFN.getName() + (bF && localLFN.isDirectory() ? "/" : ""));
						commander.printOut("path", localLFN.getCanonicalName() + (bF && localLFN.isDirectory() ? "/" : ""));
					}

					logger.info("LS line : " + ret);

					commander.printOutln(ret);
				}
			}
			else {
				if (pathsNotFound.length() > 0)
					pathsNotFound.append(", ");

				pathsNotFound.append(sPath);
			}
		}

		if (pathsNotFound.length() > 0) {
			logger.log(Level.SEVERE, "No such file or directory: [" + pathsNotFound + "]");
			commander.setReturnCode(ErrNo.ENOENT, pathsNotFound.toString());
		}
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
	protected Collection<LFN_CSD> getDirectoryListing() {
		return directory;
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("ls_csd", "[-options] [<directory>]"));
		commander.printOutln(helpStartOptions());
		commander.printOutln(helpOption("-l", "long format"));
		commander.printOutln(helpOption("-a", "show hidden .* files"));
		commander.printOutln(helpOption("-F", "add trailing / to directory names"));
		commander.printOutln(helpOption("-b", "print in guid format"));
		commander.printOutln(helpOption("-c", "print canonical paths"));
		commander.printOutln();
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
	 *
	 * @param alArguments
	 *            the arguments of the command
	 * @throws OptionException
	 */
	public JAliEnCommandls_csd(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);
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
		}
		catch (final OptionException e) {
			commander.setReturnCode(ErrNo.EINVAL, e.getMessage());
			setArgumentsOk(false);
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
