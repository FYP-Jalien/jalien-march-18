package alien.shell.commands;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;

import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.XmlCollection;
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

	private String xmlCollectionName = null;
	private String fileName = null;

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
	 * marker for -c argument :files in the catalogue
	 */
	private boolean bY = false;
	// "-x collectionname" - should take the returned LFN list through an XmlCollection instance and print the generated XML instead of simply the list of returned files
	private boolean bC = false;

	/**
	 * marker for -l argument : long format, optionally human readable file sizes
	 */
	private boolean bL = false;

	private List<String> alPaths = null;

	private Collection<LFN> lfns = null;
	private final List<LFN> directory = null;

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
		if (alPaths.size() < 2) {
			printHelp();
			return;
		}

		int flags = 0;
		/*
		 * try { if (alArguments.size() == 3) flags = Integer.parseInt(alArguments.get(2)); } catch (NumberFormatException e) { // ignore }
		 */

		if (bD)
			flags = flags | LFNUtils.FIND_INCLUDE_DIRS;
		if (bS)
			flags = flags | LFNUtils.FIND_NO_SORT;
		if (bY)
			flags = flags | LFNUtils.FIND_BIGGEST_VERSION;

		lfns = commander.c_api.find(FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), alPaths.get(0)), alPaths.get(1), flags);

		if (lfns != null && !isSilent())
			if (bX) {
				// display the xml collection

				final XmlCollection c = new XmlCollection();
				c.addAll(lfns);
				c.setName(xmlCollectionName);
				c.setOwner(commander.user.getName());

				final StringBuilder str = new StringBuilder("find");

				for (final String arg : alArguments)
					str.append(' ').append(arg);

				c.setCommand(str.toString());

				if (bC)
					try {
						final File f = File.createTempFile("collection", ".xml");

						if (f != null) {

							out.printOutln("Temp file is : " + f.getAbsolutePath());

							final String content = c.toString();
							try (BufferedWriter o = new BufferedWriter(new FileWriter(f))) {
								o.write(content);
							}

							final ArrayList<String> args = new ArrayList<>(2);
							args.add("file://" + f.getAbsolutePath());
							args.add(fileName);

							final JAliEnCommandcp cp = (JAliEnCommandcp) JAliEnCOMMander.getCommand("cp", new Object[] { commander, out, args });
							cp.silent();

							cp.start();
							while (cp.isAlive()) {
								Thread.sleep(500);
								if (!isSilent())
									out.pending();
							}

							out.printOutln(fileName);
						}
						else
							out.printErrln("Could not create a temporary file");

					} catch (final Exception e) {
						out.printErrln("Could not upload the XML collection because " + e.getMessage());
					}
				else
					out.printOutln(c.toString());
			}
			else
				for (final LFN lfn : lfns)
					if (out.isRootPrinter()) {
						out.nextResult();

						if (bL) {
							out.setField("permissions", FileSystemUtils.getFormatedTypeAndPerm(lfn));
							out.setField("user", lfn.owner);
							out.setField("group", lfn.gowner);
							out.setField("size", (bH ? Format.size(lfn.size) : String.valueOf(lfn.size)));
							out.setField("ctime", " " + lfn.ctime);
							out.setField("lfn", lfn.getCanonicalName());

						}
						else
							out.setField("lfn", lfn.getCanonicalName());
					}
					else
						if (bL)
							// print long
							out.printOutln(FileSystemUtils.getFormatedTypeAndPerm(lfn) + padSpace(3) + padLeft(lfn.owner, 8) + padSpace(1) + padLeft(lfn.gowner, 8) + padSpace(1)
									+ padLeft(bH ? Format.size(lfn.size) : String.valueOf(lfn.size), 12) + padSpace(1) + format(lfn.ctime) + padSpace(1) + padSpace(4) + lfn.getCanonicalName());
						else
							out.printOutln(lfn.getCanonicalName());

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
		out.printOutln();
		out.printOutln(helpUsage("find", "<path>  <pattern> flags"));
		out.printOutln();

		out.printOutln(helpStartOptions());

		out.printOutln(helpOption("-a", "show hidden .* files"));
		out.printOutln(helpOption("-s", "no sorting"));
		out.printOutln(helpOption("-c", "collection filename (put the output in a collection)"));
		out.printOutln(helpOption("-y", "(FOR THE OCDB) return only the biggest version of each file"));
		out.printOutln(helpOption("-x", "xml collection name (return the LFN list through XmlCollection)"));
		out.printOutln(helpOption("-d", "return also the directories"));
		out.printOutln(helpOption("-l[h]", "long format, optionally human readable file sizes"));
		out.printOutln();
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

	@Override
	public String deserializeForRoot() {
		logger.log(Level.INFO, toString());

		final StringBuilder ret = new StringBuilder();

		if (directory != null) {
			final String col = RootPrintWriter.columnseparator;
			final String desc = RootPrintWriter.fielddescriptor;
			final String sep = RootPrintWriter.fieldseparator;

			for (final LFN lfn : directory) {
				if (!bA && lfn.getFileName().startsWith("."))
					continue;

				if (bD) {
					if (lfn.type != 'd') {
						ret.append(col);
						ret.append(desc).append("path").append(sep).append(lfn.getCanonicalName());
						ret.append(desc).append("guid").append(sep).append(lfn.guid);
					}
				}
				else
					if (bC) {
						ret.append(col);
						ret.append(desc).append("name").append(sep).append(lfn.getCanonicalName());
					}

					else {
						ret.append(col);
						ret.append(desc).append("name").append(sep).append(lfn.getFileName());

					}
			}

			return ret.toString();

		}
		return super.deserializeForRoot();

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
	// public JAliEnCommandfind(JAliEnCOMMander commander, UIPrintWriter out, final ArrayList<String> alArguments){
	// super(commander, out, alArguments);
	public JAliEnCommandfind(final JAliEnCOMMander commander, final UIPrintWriter out, final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);
		try {

			final OptionParser parser = new OptionParser();

			parser.accepts("l");
			parser.accepts("s");
			parser.accepts("x").withRequiredArg();
			parser.accepts("a");
			parser.accepts("h");
			parser.accepts("d");
			parser.accepts("c").withRequiredArg();
			parser.accepts("y");

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			if (options.has("x") && options.hasArgument("x")) {
				bX = true;

				xmlCollectionName = (String) options.valueOf("x");
			}
			if (options.has("c") && options.hasArgument("c")) {
				bC = true;
				fileName = (String) options.valueOf("c");

			}

			alPaths = optionToString(options.nonOptionArguments());

			bL = options.has("l");
			bS = options.has("s");
			bA = options.has("a");
			bD = options.has("d");
			bH = options.has("h");
			bY = options.has("y");
			bC = options.has("c");
		} catch (final OptionException e) {
			printHelp();
			throw e;
		}
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();

		sb.append("\n { JAliEnCommandfind received\n");
		sb.append("Arguments: ");

		if (bL)
			sb.append(" -l ");
		if (bA)
			sb.append(" -a ");
		if (bC)
			sb.append(" -c ");
		if (bS)
			sb.append(" -s ");
		if (bX)
			sb.append(" -x ");
		if (bD)
			sb.append(" -d ");

		sb.append("}");

		return sb.toString();
	}

}
