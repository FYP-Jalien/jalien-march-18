package alien.shell.commands;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import alien.api.Dispatcher;
import alien.api.catalogue.ArchiveContent;
import alien.catalogue.CatalogEntity;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.shell.ErrNo;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import lazyj.Format;

/**
 * @author costing
 * @since 2023-10-21
 */
public class JAliEnCommandarchiveList extends JAliEnBaseCommand {
	private ArrayList<String> alPaths = null;

	@Override
	public void run() {
		final Map<UUID, CatalogEntity> uuids = new LinkedHashMap<>();

		for (final String lfnName : this.alPaths) {
			final LFN lfn = commander.c_api.getLFN(FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), lfnName));

			if (lfn == null) {
				if (GUIDUtils.isValidGUID(lfnName)) {
					final GUID g = commander.c_api.getGUID(lfnName);

					if (g != null)
						uuids.put(UUID.fromString(lfnName), g);
					else {
						commander.setReturnCode(ErrNo.ENOENT, lfnName + " does not exist in the catalogue");
					}
				}
				else {
					commander.printErrln("LFN " + lfnName + " doesn't exist in the catalogue");
					commander.setReturnCode(ErrNo.ENOENT, lfnName + " does not exist in the catalogue");
				}
			}
			else
				uuids.put(lfn.guid, lfn);
		}

		ArchiveContent ac = new ArchiveContent(commander.getUser(), new HashSet<>(uuids.keySet()));

		try {
			ac = Dispatcher.execute(ac);
		}
		catch (final Exception e) {
			commander.setReturnCode(ErrNo.EREMOTEIO, "Server threw an exception: " + e.getMessage());
			return;
		}

		final Map<UUID, Map<GUID, String>> content = ac.getContents();

		for (final Map.Entry<UUID, CatalogEntity> e : uuids.entrySet()) {
			final CatalogEntity entity = e.getValue();

			commander.printOutln("Content of: " + entity.getName() + " (" + entity.getSize() + " bytes = " + Format.size(entity.getSize()) + ")");

			final Map<GUID, String> members = content.get(e.getKey());

			if (members == null || members.size() == 0) {
				commander.printOutln("  there are no files pointing to this entry, looks like this is not a ZIP archive");
				continue;
			}

			for (final Map.Entry<GUID, String> member : members.entrySet()) {
				final GUID memberGuid = member.getKey();

				commander.printOutln("archive_guid", e.getKey().toString());
				commander.printOutln("archive_name", entity.getName());
				commander.printOutln("member_guid", memberGuid.toString());
				commander.printOutln("member_size", String.valueOf(memberGuid.getSize()));

				commander.printOutln("  " + member.getValue() + "\t , guid " + memberGuid.guid + " (" + memberGuid.getSize() + " bytes = " + Format.size(memberGuid.getSize()) + " = "
						+ Format.point(memberGuid.getSize() * 100. / entity.getSize()) + "% of the archive)");

				commander.outNextResult();
			}
		}
	}

	private static final OptionParser parser = new OptionParser();

	static {
		parser.accepts("v");
	}

	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("archiveList", "<filename1> [<or uuid>] ..."));
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
	public JAliEnCommandarchiveList(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);
		try {
			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			alPaths = new ArrayList<>(options.nonOptionArguments().size());
			alPaths.addAll(optionToString(options.nonOptionArguments()));
		}
		catch (final OptionException e) {
			commander.setReturnCode(ErrNo.EINVAL, e.getMessage());
			setArgumentsOk(false);
		}
	}
}
