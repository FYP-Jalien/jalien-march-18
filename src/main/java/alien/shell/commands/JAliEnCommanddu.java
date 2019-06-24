package alien.shell.commands;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.PFN;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import lazyj.Format;

/**
 *
 */
public class JAliEnCommanddu extends JAliEnBaseCommand {

	private boolean bH = false;
	private boolean bC = false;
	private boolean bS = false;

	private List<String> paths = null;

	private static class DUStats {
		long physicalFiles = 0;
		long physicalOneCopy = 0;
		long physicalTotalSize = 0;
		long physicalReplicas = 0;

		long logicalFiles = 0;
		long logicalSize = 0;

		long subfolders = 0;

		public DUStats() {

		}

		public void addPhysicalFile(final long size, final long replicas) {
			physicalFiles++;
			physicalOneCopy += size;
			physicalTotalSize += size * replicas;
			physicalReplicas += replicas;
		}

		public void addLogicalFile(final long size) {
			logicalFiles++;
			logicalSize += size;
		}

		public void addSubfolder() {
			subfolders++;
		}

		public void addStats(final DUStats other) {
			physicalFiles += other.physicalFiles;
			physicalOneCopy += other.physicalOneCopy;
			physicalTotalSize += other.physicalTotalSize;
			physicalReplicas += other.physicalReplicas;

			logicalFiles += other.logicalFiles;
			logicalSize += other.logicalSize;

			subfolders += other.subfolders;
		}
	}

	private String getSize(final long value) {
		return (bH ? Format.size(value) : String.valueOf(value));
	}

	private void printStats(final String firstLine, final DUStats stats) {
		commander.printOutln(firstLine);
		commander.printOutln("  Logical files : " + stats.logicalFiles + " of " + getSize(stats.logicalSize));
		commander.printOut("  Physical files: " + stats.physicalFiles + " of " + getSize(stats.physicalTotalSize));
		commander.printOut(" in " + stats.physicalReplicas + " replicas ");

		if (stats.physicalFiles > 0)
			commander.printOut(" (avg " + Format.point((double) stats.physicalReplicas / stats.physicalFiles) + " replicas/file)");

		commander.printOutln(", size of one replica: " + getSize(stats.physicalOneCopy));

		commander.printOutln("  Subfolders: " + stats.subfolders);
	}

	@Override
	public void run() {
		if (paths == null || paths.size() == 0)
			return;

		final DUStats summary = new DUStats();

		final List<String> pathsToRunOn = new ArrayList<>();

		for (String path : paths) {
			final String absolutePath = FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), path);

			final List<String> expandedPaths = FileSystemUtils.expandPathWildCards(absolutePath, commander.user);

			for (String expandedPath : expandedPaths)
				if (!pathsToRunOn.contains(expandedPath))
					pathsToRunOn.add(expandedPath);
		}

		for (final String path : pathsToRunOn) {
			final DUStats stats = new DUStats();

			final Collection<LFN> lfns = commander.c_api.find(path, "*", null, LFNUtils.FIND_INCLUDE_DIRS | LFNUtils.FIND_NO_SORT, null, null);

			if (lfns != null) {
				for (final LFN l : lfns) {
					if (l.isDirectory())
						stats.addSubfolder();

					if (l.isCollection() && bC)
						stats.addLogicalFile(l.getSize());

					if (l.isFile()) {
						final Set<PFN> pfns = commander.c_api.getPFNs(l.guid.toString());

						if (pfns != null && pfns.size() > 0) {
							boolean logicalFile = false;
							int physicalReplicas = 0;

							for (final PFN p : pfns) {
								if (p.pfn.startsWith("guid://"))
									logicalFile = true;
								else
									physicalReplicas++;
							}

							if (logicalFile)
								stats.addLogicalFile(l.getSize());

							if (physicalReplicas > 0)
								stats.addPhysicalFile(l.getSize(), physicalReplicas);
						}
					}
				}

				if (!bS)
					printStats(path, stats);
				else
					summary.addStats(stats);
			}
			else
				commander.printErrln("Could not get the usage of " + path);
		}

		if (bS)
			printStats("Summary of " + paths.size() + " paths", summary);
	}

	@Override
	public void printHelp() {
		commander.printOutln("Gives the disk space usge of one or more directories");
		commander.printOutln("Usage: du [-hc] <dir> ...");
		commander.printOutln();
		commander.printOutln("Options:");
		commander.printOutln("	-h: Give the output in human readable format");
		commander.printOutln("	-c: Include collections in the summary information");
		commander.printOutln("	-s: Print a summary of all parameters");
		commander.printOutln();
	}

	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * @param commander
	 * @param alArguments
	 * @throws OptionException
	 */
	public JAliEnCommanddu(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);

		try {
			final OptionParser parser = new OptionParser();
			parser.accepts("h");
			parser.accepts("c");
			parser.accepts("s");

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			paths = optionToString(options.nonOptionArguments());

			if (paths.size() < 1)
				return;

			bH = options.has("h");
			bC = options.has("c");
			bS = options.has("s");
		}
		catch (final OptionException e) {
			// printHelp();
			throw e;
		}
	}
}
