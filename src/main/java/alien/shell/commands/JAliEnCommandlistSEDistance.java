package alien.shell.commands;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import alien.catalogue.FileSystemUtils;
import alien.se.SE;
import alien.shell.ErrNo;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * @author costing
 *
 */
public class JAliEnCommandlistSEDistance extends JAliEnBaseCommand {
	private boolean useWriteMetrics;
	private String site;
	private String lfn_name;
	private String qos;

	@Override
	public void run() {
		/*
		 * if (!this.useWriteMetrics && (this.lfn_name == null || this.lfn_name.length() == 0)) {
		 * commander.printErrln("No LFN specified for read metrics");
		 * return;
		 * }
		 */

		if (lfn_name != null && lfn_name.length() != 0)
			this.lfn_name = FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), this.lfn_name);

		final List<HashMap<SE, Double>> results = commander.c_api.listSEDistance(site, this.useWriteMetrics, this.lfn_name, this.qos);

		if (results == null) {
			commander.setReturnCode(ErrNo.ENODATA, "No results from server");
			return;
		}

		for (final HashMap<SE, Double> smap : results) {
			for (final Map.Entry<SE, Double> entry : smap.entrySet()) {
				final SE s = entry.getKey();

				if (!s.seName.contains("::"))
					continue;

				commander.printOutln(String.format("%1$" + 40 + "s", s.seName) + "\t(read: " + String.format("% .3f", Double.valueOf(s.demoteRead)) + ",  write: "
						+ String.format("% .3f", Double.valueOf(s.demoteWrite)) + ",  distance: " + String.format("% .3f", entry.getValue()) + ")");
			}
		}

		commander.printOutln();
	}

	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln("listSEDistance: Returns the closest working SE for a particular site. Usage");
		commander.printOutln(helpStartOptions());
		commander.printOutln(helpOption("-site", "site to base the results to, instead of using the default mapping of this client to a site"));
		commander.printOutln(helpOption("-read", "use the read metrics, optionally with an LFN for which to sort the replicas. Default is to print the write metrics."));
		commander.printOutln(helpOption("-qos", "restrict the returned SEs to this particular tag"));
		commander.printOutln();
	}

	@Override
	public boolean canRunWithoutArguments() {
		return true;
	}

	/**
	 * @param commander
	 * @param alArguments
	 * @throws OptionException
	 */
	public JAliEnCommandlistSEDistance(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);

		final OptionParser parser = new OptionParser();
		parser.accepts("qos").withRequiredArg();
		parser.accepts("read").withOptionalArg();
		parser.accepts("site").withRequiredArg();

		final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

		if (options.hasArgument("qos"))
			qos = options.valueOf("qos").toString();
		else
			qos = null;

		if (options.has("read")) {
			this.useWriteMetrics = false;

			if (options.hasArgument("read"))
				this.lfn_name = options.valueOf("read").toString();
		}
		else
			this.useWriteMetrics = true;

		if (options.has("site"))
			this.site = options.valueOf("site").toString();
	}
}
