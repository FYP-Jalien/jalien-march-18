package alien.shell.commands;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import alien.catalogue.FileSystemUtils;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.se.SE;
import alien.se.SEUtils;
import alien.shell.ErrNo;
import alien.shell.ShellColor;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import lazyj.Format;

/**
 * @author costing
 * @since 2018-08-15
 */
public class JAliEnCommandstat extends JAliEnBaseCommand {
	private ArrayList<String> alPaths = null;

	/**
	 * Verbose
	 */
	private boolean bV = false;

	private boolean printGUIDInfo(final String guidName, final boolean indexed) {
		final GUID g = commander.c_api.getGUID(guidName);

		if (g == null) {
			commander.printErrln("This GUID does not exist in the catalogue: " + guidName);
			return false;
		}

		final String prefix = indexed ? "    " : "";

		commander.printOutln(prefix + "GUID: " + guidName);
		commander.printOutln(prefix + "Owner: " + g.owner + ":" + g.gowner);
		commander.printOutln(prefix + "Permissions: " + g.perm);
		commander.printOutln(prefix + "Size: " + g.size + " (" + Format.size(g.size) + ")");
		commander.printOut("guid", guidName);
		commander.printOut("owner", g.owner);
		commander.printOut("gowner", g.gowner);
		commander.printOut("perm", g.perm);
		commander.printOut("size", String.valueOf(g.size));

		if (bV) {
			commander.printOut("host", String.valueOf(g.host));
			commander.printOut("tableName", String.valueOf(g.tableName));
			commander.printOutln(prefix + "GUID shard: " + g.host + " / " + g.tableName);
		}

		if (g.md5 != null && g.md5.length() > 0) {
			commander.printOutln(prefix + "MD5: " + g.md5);
			commander.printOut("md5", g.md5);
		}

		final long gTime = GUIDUtils.epochTime(g.guid);

		commander.printOutln(prefix + "Created: " + (new Date(gTime)) + " (" + gTime + ") by " + GUIDUtils.getMacAddr(g.guid));
		commander.printOut("mtime", String.valueOf(gTime));

		if (g.ctime != null) {
			commander.printOutln(prefix + "Last change: " + g.ctime + " (" + g.ctime.getTime() + ")");
			commander.printOut("ctime", String.valueOf(g.ctime.getTime()));
		}
		else
			commander.printOut("ctime", "");

		final Set<PFN> pfns = g.getPFNs();

		if (pfns == null || pfns.size() == 0)
			commander.printOutln(prefix + "No physical replicas");
		else {
			commander.printOutln(prefix + "Replicas:");

			final Map<String, String> replicas = new LinkedHashMap<>();

			for (final PFN p : pfns) {
				String seName;

				if (p.seNumber > 0) {
					final SE se = SEUtils.getSE(p.seNumber);

					if (se == null) {
						seName = "SE #" + p.seNumber + " no longer exists";
						replicas.put(String.valueOf(p.seNumber), p.pfn);
					}
					else {
						seName = "SE => " + se.seName;
						replicas.put(se.seName, p.pfn);
					}
				}
				else {
					seName = "ZIP archive member";
					replicas.put("ZIP", p.pfn);
				}

				commander.printOutln("\t " + padRight(seName, 30) + " pfn => " + p.pfn + "\n");
			}

			if (replicas.size() > 0)
				commander.printOut("replicas", replicas);
		}

		return true;
	}

	@Override
	public void run() {
		for (final String lfnName : this.alPaths) {
			final LFN lfn = commander.c_api.getLFN(FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), lfnName));

			if (lfn == null) {
				if (GUIDUtils.isValidGUID(lfnName)) {
					if (!printGUIDInfo(lfnName, false))
						continue;
				}
				else {
					commander.setReturnCode(ErrNo.ENOENT, lfnName);
					continue;
				}
			}
			else {
				commander.printOutln("File: " + lfn.getCanonicalName());
				commander.printOutln("Type: " + lfn.type);
				commander.printOutln("Owner: " + lfn.owner + ":" + lfn.gowner);
				commander.printOutln("Permissions: " + lfn.perm);
				commander.printOutln("Last change: " + lfn.ctime + " (" + lfn.ctime.getTime() + ")");

				if (lfn.expiretime != null) {
					commander.printOut("expiretime", String.valueOf(lfn.expiretime.getTime() / 1000));
					commander.printOut("Expires: " + lfn.expiretime + " (" + lfn.expiretime.getTime() + ")");

					if (lfn.expiretime.getTime() <= System.currentTimeMillis())
						commander.printOutln(ShellColor.jobStateRed() + " EXPIRED" + ShellColor.reset());
					else
						commander.printOutln(" which is in " + Format.toInterval(lfn.expiretime.getTime() - System.currentTimeMillis()));
				}

				if (bV && lfn.indexTableEntry != null) {
					commander.printOutln("LFN shard: " + lfn.indexTableEntry.hostIndex + " / " + lfn.indexTableEntry.tableName);
					commander.printOut("hostIndex", String.valueOf(lfn.indexTableEntry.hostIndex));
					commander.printOut("tableName", String.valueOf(lfn.indexTableEntry.tableName));
				}

				commander.printOut("lfn", lfn.getCanonicalName());
				commander.printOut("type", String.valueOf(lfn.type));
				commander.printOut("owner", lfn.owner);
				commander.printOut("gowner", lfn.gowner);
				commander.printOut("perm", lfn.perm);
				commander.printOut("ctime", lfn.ctime != null ? String.valueOf(lfn.ctime.getTime()) : "");

				if (lfn.jobid > 0) {
					commander.printOutln("Job ID: " + lfn.jobid);
					commander.printOut("jobid", String.valueOf(lfn.jobid));
				}

				if (!lfn.isDirectory()) {
					commander.printOutln("Size: " + lfn.size + " (" + Format.size(lfn.size) + ")");
					commander.printOutln("MD5: " + lfn.md5);
					commander.printOut("size", String.valueOf(lfn.size));
					commander.printOut("md5", lfn.md5);

					if (lfn.guid != null) {
						if (bV) {
							commander.printOutln("GUID detailed information:");
							printGUIDInfo(lfn.guid.toString(), true);
						}
						else {
							final long gTime = GUIDUtils.epochTime(lfn.guid);

							commander.printOutln("GUID: " + lfn.guid);
							commander.printOutln("\tGUID created on " + (new Date(gTime)) + " (" + gTime + ") by " + GUIDUtils.getMacAddr(lfn.guid));
							commander.printOut("guid", String.valueOf(lfn.guid));
							commander.printOut("mtime", String.valueOf(gTime));
						}
					}
				}
			}

			commander.printOutln();

			commander.outNextResult();
		}
	}

	private static final OptionParser parser = new OptionParser();

	static {
		parser.accepts("v");
	}

	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("stat", "[-v] <filename1> [<or uuid>] ..."));
		commander.printOutln(helpOption("-v", "More details on the status."));
		commander.printOutln();
	}

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
	public JAliEnCommandstat(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);
		try {
			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			bV = options.has("v");

			alPaths = new ArrayList<>(options.nonOptionArguments().size());
			alPaths.addAll(optionToString(options.nonOptionArguments()));
		}
		catch (final OptionException e) {
			commander.setReturnCode(ErrNo.EINVAL, e.getMessage());
			setArgumentsOk(false);
		}
	}
}
