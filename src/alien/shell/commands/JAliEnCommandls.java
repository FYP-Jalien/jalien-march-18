package alien.shell.commands;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import lazyj.Log;
import alien.api.catalogue.CatalogueApiUtils;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;
import alien.perl.commands.AlienTime;

/**
 * @author ron
 * @since June 4, 2011
 */
public class JAliEnCommandls extends JAliEnBaseCommand {

	/**
	 * marker for -l argument
	 */
	private boolean bL = false;

	/**
	 * marker for -a argument
	 */
	private boolean bA = false;

	/**
	 * marker for -F argument
	 */
	private boolean bF = false;

	/**
	 * marker for -b argument
	 */
	private boolean bB = false;

	private final ArrayList<String> alPaths;

	
	/**
	 * list of the LFNs that came up by the ls command
	 */
	private List<LFN> directory = null;

	/**
	 * execute the ls
	 */
	public void execute() {

		int iDirs = alPaths.size();

		if (iDirs == 0)
			alPaths.add(commander.getCurrentDir().getCanonicalName());

		for (String sPath : alPaths) {
			// listing current directory
			if (!sPath.startsWith("/"))
				sPath = commander.getCurrentDir().getCanonicalName()
						+ sPath;

			Log.log(Log.INFO, "Spath = \"" + sPath + "\"");

				directory = CatalogueApiUtils.getLFNs(sPath);


			if (directory != null) {
				for (LFN localLFN : directory) {

					if (!bA && localLFN.getFileName().startsWith("."))
						continue;

					String ret = "";
					if (bB) {
						if (localLFN.type == 'd')
							continue;
						ret += localLFN.guid.toString().toUpperCase() + "	"
								+ localLFN.getName();
					} else {

						if (bL)
							ret += FileSystemUtils
									.getFormatedTypeAndPerm(localLFN)
									+ "   "
									+ localLFN.owner
									+ " "
									+ localLFN.gowner
									+ " "
									+ padLeft(String.valueOf(localLFN.size), 12)
									+ " "
									+ format(localLFN.ctime)
									+ "            " + localLFN.getFileName();
						else
							ret += localLFN.getFileName();

						if (bF && (localLFN.type == 'd'))
							ret += "/";
					}
					if (!silent)
						out.printOutln(ret);
				}
			}

		}
		if(out.isRootPrinter())
			out.setReturnArgs(deserializeForRoot());
	}

	private static final DateFormat formatter = new SimpleDateFormat(
			"MMM dd HH:mm");

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
	public void printHelp() {
		out.printOutln(AlienTime.getStamp()
				+ "Usage: ls [-laFn|b|h] [<directory>]\n"
				+"		-l : long format\n"
				+"		-a : show hidden .* files\n"
				+"		-F : add trailing / to directory names\n"
				+"		-b : print in guid format\n"
				+"		-h : print the help text");
	}

	/**
	 * ls can run without arguments
	 * 
	 * @return <code>true</code>
	 */
	public boolean canRunWithoutArguments() {
		return true;
	}

	/**
	 * the command's silence trigger
	 */
	private boolean silent = false;

	/**
	 * set command's silence trigger
	 */
	public void silent() {
		silent = true;
	}

	
	/**
	 * serialize return values for gapi/root 
	 * @return serialized return
	 */
	public String deserializeForRoot(){
		String ret = RootPrintWriter.columnseparator;

		if (directory != null) {
			String desc = RootPrintWriter.fielddescriptor;
			String sep = RootPrintWriter.fieldseparator;
			for (LFN lfn : directory) {
				ret+= desc+ "aclId"+sep+ lfn.aclId;
				ret+= desc+ "lfn"+sep+ lfn.lfn;
				ret+= desc+ "broken"+sep+ lfn.broken;
				ret+= desc+ "dir"+sep+ lfn.dir;
				ret+= desc+ "size"+sep+ lfn.size;
				ret+= desc+ "gowner"+sep+ lfn.gowner;
				ret+= desc+ "guid"+sep+ lfn.guid;
				ret+= desc+ "owner"+sep+ lfn.owner;
				ret+= desc+ "replicated"+sep+ lfn.replicated;
				ret+= desc+ "ctime"+sep+ lfn.ctime;
				ret+= desc+ "entryId"+sep+ lfn.entryId;
				ret+= desc+ "guidtime"+sep+ lfn.guidtime;
				ret+= desc+ "expiretime"+sep+ lfn.expiretime;
				ret+= desc+ "type"+sep+ lfn.type;
				ret+= desc+ "md5"+sep+ lfn.md5;
				ret+= desc+ "perm"+sep+ lfn.perm;
			}
			return ret;
		}
		else 
			return super.deserializeForRoot();
		
	}
	
	
	
	/**
	 * Constructor needed for the command factory in commander
	 * @param commander 
	 * @param out 
	 * 
	 * @param alArguments
	 *            the arguments of the command
	 */
	public JAliEnCommandls(JAliEnCOMMander commander, UIPrintWriter out, final ArrayList<String> alArguments){
		super(commander, out, alArguments);
		final OptionParser parser = new OptionParser();
		
		parser.accepts("l");
		parser.accepts("bulk");
		parser.accepts("b");
		parser.accepts("a");
		parser.accepts("F");
		
		final OptionSet options = parser.parse(alArguments.toArray(new String[]{}));
		
		alPaths = new ArrayList<String>(options.nonOptionArguments().size());
		alPaths.addAll(options.nonOptionArguments());
		
				bL = options.has("l");
//				bBulk = options.has("bulk");
				bB = options.has("b");
				bA = options.has("a");
				bF = options.has("F");
	}

}
