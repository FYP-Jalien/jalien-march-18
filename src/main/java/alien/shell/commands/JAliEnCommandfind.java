package alien.shell.commands;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.shell.ErrNo;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import lazyj.Format;

/**
 * @author ron
 * @since June 4, 2011
 */
public class JAliEnCommandfind extends JAliEnBaseCommand {

	/**
	 * marker for -x argument : return the LFN list through XmlCollection
	 */
	private boolean bX = false;
	private boolean bH = false;
	private boolean bC = false;

	private String xmlCollectionName = null;

	/**
	 * marker for -a argument : show hidden .files
	 */
	private boolean bA = false;

	/**
	 * marker for -s argument : no sorting
	 */
	private boolean bS = false;

	/**
	 * marker for -d argument : directory names
	 */
	private boolean bD = false;

	/**
	 * marker for -y: (FOR THE OCDB) return only the biggest version of each file
	 */
	private boolean bY = false;

	/**
	 * marker for -j argument : filter files for jobid
	 */
	private boolean bJ = false;

	/**
	 * marker for -r argument: regex pattern
	 */
	private boolean bR = false;

	/**
	 * marker for -w argument : wide format, optionally human readable file sizes
	 */
	private boolean bW = false;

	/**
	 * Full LFN details, an API option only as it simply makes sure all fields are returned as JSON data
	 */
	private boolean bF = false;

	private List<String> alPaths = null;

	private Collection<LFN> lfns = null;

	private Long queueid = Long.valueOf(0);

	private long limit = Long.MAX_VALUE;

	private long offset = 0;

	/**
	 * returns the LFNs that were the result of the find
	 *
	 * @return the output file
	 */

	public Collection<LFN> getLFNs() {
		return lfns;
	}

	/**
	 * execute the get
	 */
	@Override
	public void run() {
		int flags = 0;
		String query = "";

		if (bD)
			flags = flags | LFNUtils.FIND_INCLUDE_DIRS;

		if (bS)
			flags = flags | LFNUtils.FIND_NO_SORT;

		if (bY) {
			query = String.join(" ", alPaths.subList(2, alPaths.size()));

			query = query.replaceFirst("^\\s*['\"]\\s*", "");
			query = query.replaceFirst("\\s*['\"]\\s*$", "");

			flags = flags | LFNUtils.FIND_BIGGEST_VERSION;
		}

		if (bX)
			flags = flags | LFNUtils.FIND_SAVE_XML;

		if (bJ)
			flags = flags | LFNUtils.FIND_FILTER_JOBID;

		if (bR)
			flags = flags | LFNUtils.FIND_REGEXP;

		final String xmlCollectionPath = xmlCollectionName != null ? FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), xmlCollectionName) : null;

		String path = FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), alPaths.get(0));

		final LFN lPath = commander.c_api.getLFN(path);

		if (lPath != null && lPath.isDirectory()) {
			path = lPath.getCanonicalName();
		}
		else {
			commander.setReturnCode(ErrNo.ENOTDIR, alPaths.get(0));
			return;
		}

		lfns = commander.c_api.find(path, alPaths.get(1), query, flags, xmlCollectionPath, queueid, xmlCollectionPath != null && limit != Long.MAX_VALUE ? limit : -1);

		int count = 0;

		if (lfns != null) {
			if (offset >= lfns.size())
				return;

			if (bX) {
				return;
			}

			for (final LFN lfn : lfns) {
				if (--offset >= 0)
					continue;

				if (--limit < 0)
					break;

				count++;

				commander.outNextResult();
				commander.printOut("lfn", lfn.getCanonicalName());

				if (bW || bF) {
					commander.printOut("perm", lfn.perm);
					commander.printOut("owner", lfn.owner);
					commander.printOut("gowner", lfn.gowner);
					commander.printOut("size", String.valueOf(lfn.size));
					commander.printOut("ctime", lfn.ctime != null ? String.valueOf(lfn.ctime.getTime()) : "");
					commander.printOut("guid", lfn.guid != null ? lfn.guid.toString() : "");
				}

				if (bF) {
					// the remaining fields that are needed in particular for preparing XML collections
					commander.printOut("name", lfn.getFileName());
					commander.printOut("aclId", lfn.aclId >= 0 ? String.valueOf(lfn.aclId) : "");
					commander.printOut("broken", lfn.broken ? "1" : "0");
					commander.printOut("dir", String.valueOf(lfn.dir));
					commander.printOut("entryId", String.valueOf(lfn.entryId));
					commander.printOut("expiretime", lfn.expiretime != null ? String.valueOf(lfn.expiretime.getTime()) : "");
					commander.printOut("guidtime", lfn.guidtime);
					commander.printOut("md5", lfn.md5 != null ? lfn.md5 : "");
					commander.printOut("jobid", String.valueOf(lfn.jobid));
					commander.printOut("replicated", lfn.replicated ? "1" : "0");
					commander.printOut("type", String.valueOf(lfn.type));
				}

				if (bW) {
					// print long
					commander.printOutln(FileSystemUtils.getFormatedTypeAndPerm(lfn) + padSpace(3) + padLeft(lfn.owner, 8) + padSpace(1) + padLeft(lfn.gowner, 8) + padSpace(1)
							+ padLeft(bH ? Format.size(lfn.size) : String.valueOf(lfn.size), 12) + padSpace(1) + format(lfn.ctime) + padSpace(1) + padSpace(4) + lfn.getCanonicalName());
				}
				else
					commander.printOutln(lfn.getCanonicalName());
			}
		}

		if (bC)
			commander.printOutln("Found " + count + " maching entries");
	}

	private static final DateFormat formatter = new SimpleDateFormat("MMM dd HH:mm");

	private static synchronized String format(final Date d) {
		return formatter.format(d);
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("find", "[flags] <path> <pattern>"));
		commander.printOutln();

		commander.printOutln(helpStartOptions());

		commander.printOutln(helpOption("-a", "show hidden .* files"));
		commander.printOutln(helpOption("-s", "no sorting"));
		commander.printOutln(helpOption("-c", "print the number of matching files"));
		commander.printOutln(helpOption("-x <target LFN>", "create the indicated XML collection with the results of the find operation"));
		commander.printOutln(helpOption("-d", "return also the directories"));
		commander.printOutln(helpOption("-w[h]", "long format, optionally human readable file sizes"));
		commander.printOutln(helpOption("-j <queueid>", "filter files created by a certain job ID"));
		commander.printOutln(helpOption("-l <count>", "limit the number of returned entries to at most the indicated value"));
		commander.printOutln(helpOption("-o <offset>", "skip over the first /offset/ results"));
		commander.printOutln(helpOption("-r", "pattern is a regular expression"));
		commander.printOutln(helpOption("-f", "return all LFN data as JSON fields (API flag only)"));
		commander.printOutln(helpOption("-y", "(FOR THE OCDB) return only the biggest version of each file"));
		commander.printOutln();
	}

	/**
	 * find cannot run without arguments
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
	// public JAliEnCommandfind(JAliEnCOMMander commander, UIPrintWriter out, final ArrayList<String> alArguments){
	// super(commander, out, alArguments);
	public JAliEnCommandfind(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);
		try {

			final OptionParser parser = new OptionParser();

			parser.accepts("w");
			parser.accepts("s");
			parser.accepts("c");
			parser.accepts("x").withRequiredArg();
			parser.accepts("a");
			parser.accepts("h");
			parser.accepts("d");
			parser.accepts("y");
			parser.accepts("r");
			parser.accepts("j").withRequiredArg().ofType(Long.class);
			parser.accepts("l").withRequiredArg().ofType(Long.class);
			parser.accepts("o").withRequiredArg().ofType(Long.class);
			parser.accepts("z"); // ignored option, just to maintain compatibility with AliEn
			parser.accepts("f"); // full LFN details, API only

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			if (options.has("x") && options.hasArgument("x")) {
				bX = true;
				xmlCollectionName = (String) options.valueOf("x");
			}

			if (options.has("j") && options.hasArgument("j")) {
				bJ = true;
				queueid = (Long) options.valueOf("j");
			}

			alPaths = optionToString(options.nonOptionArguments());

			bF = options.has("f");
			bW = options.has("w");
			bS = options.has("s");
			bA = options.has("a");
			bD = options.has("d");
			bH = options.has("h");
			bY = options.has("y");
			bR = options.has("r");
			bC = options.has("c");

			if (options.has("l")) {
				limit = ((Long) options.valueOf("l")).longValue();

				if (limit <= 0) {
					commander.printErrln("Limit value has to be strictly positive, ignoring indicated value (" + limit + ")");
					limit = Long.MAX_VALUE;
				}
			}

			if (options.has("o")) {
				offset = ((Long) options.valueOf("o")).longValue();

				if (offset < 0) {
					commander.printErrln("Offset value cannot be negative, ignoring indicated value (" + offset + ")");
					offset = 0;
				}
			}
			
			if (alPaths.size()==0) {
				setArgumentsOk(false);
				return;
			}

			if (alPaths.size() == 1) {
				// can we infer the path and the pattern from the only argument we have?
				String path = alPaths.get(0);

				int idx = path.indexOf('*');

				if (idx > 0) {
					idx = path.lastIndexOf('/', idx);

					if (idx > 0) {
						alPaths.clear();
						alPaths.add(path.substring(0, idx));
						alPaths.add(path.substring(idx + 1));
					}
				}

				if (alPaths.size() == 1) {
					setArgumentsOk(false);
					commander.setReturnCode(ErrNo.EINVAL, "I can't infer the search pattern from `" + path + "`, please pass the base directory and the search pattern separately");
					printHelp();
				}
			}
		}
		catch (final OptionException e) {
			commander.setReturnCode(ErrNo.EINVAL, e.getMessage());
			setArgumentsOk(false);
		}
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();

		sb.append("\n { JAliEnCommandfind received\n");
		sb.append("Arguments:");

		if (bW)
			sb.append(" -w");
		if (bA)
			sb.append(" -a");
		if (bS)
			sb.append(" -s");
		if (bX)
			sb.append(" -x");
		if (bD)
			sb.append(" -d");
		if (bJ)
			sb.append(" -j");
		if (bR)
			sb.append(" -r");
		if (bC)
			sb.append(" -c");

		sb.append("}");

		return sb.toString();
	}

}
