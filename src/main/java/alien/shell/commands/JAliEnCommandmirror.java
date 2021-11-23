package alien.shell.commands;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import alien.catalogue.FileSystemUtils;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.shell.ErrNo;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * @author psvirin
 *
 */
public class JAliEnCommandmirror extends JAliEnBaseCommand {
	private boolean useLFNasGuid;
	private Integer attempts;
	private String lfn;
	private String dstSE;
	private String removeSourceSE;

	private int referenceCount = 0;
	private final List<String> ses = new ArrayList<>();
	private final List<String> exses = new ArrayList<>();
	private final HashMap<String, Integer> qos = new HashMap<>();

	/**
	 * @param commander
	 * @param alArguments
	 */
	public JAliEnCommandmirror(final JAliEnCOMMander commander, final List<String> alArguments) {
		super(commander, alArguments);

		try {
			final OptionParser parser = new OptionParser();
			parser.accepts("try").withRequiredArg().ofType(Integer.class);
			parser.accepts("S").withRequiredArg();
			parser.accepts("r").withRequiredArg();
			parser.accepts("g");

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			final List<String> lfns = optionToString(options.nonOptionArguments());
			if (lfns == null || lfns.size() == 0)
				return;
			this.lfn = lfns.get(0);

			useLFNasGuid = options.has("g");

			if (options.has("try"))
				attempts = (Integer) options.valueOf("try");
			else
				attempts = Integer.valueOf(5);

			removeSourceSE = options.has("r") ? options.valueOf("r").toString() : null;

			if (options.has("S") && options.hasArgument("S")) {
				if ((String) options.valueOf("S") != null) {
					final StringTokenizer st = new StringTokenizer((String) options.valueOf("S"), ",");
					while (st.hasMoreElements()) {
						final String spec = st.nextToken();
						if (spec.contains("::")) {
							if (spec.indexOf("::") != spec.lastIndexOf("::"))
								// SE
								// spec
								if (spec.startsWith("!"))
									exses.add(spec.substring(1).toUpperCase());
								else {// an SE spec
									ses.add(spec.toUpperCase());
									referenceCount++;
								}
						}
						else if (spec.contains(":"))
							try {
								final int c = Integer.parseInt(spec.substring(spec.indexOf(':') + 1));
								if (c > 0) {
									qos.put(spec.substring(0, spec.indexOf(':')), Integer.valueOf(c));
									referenceCount = referenceCount + c;
								}
								else
									throw new JAliEnCommandException("Number of replicas cannot be negative, in QoS string " + spec);
							}
							catch (final Exception e) {
								throw new JAliEnCommandException("Exception parsing QoS string " + spec, e);
							}
						else if (!spec.equals(""))
							throw new JAliEnCommandException();
					}
				}
			}
			else {
				if (lfns.size() != 2)
					throw new JAliEnCommandException();
				this.dstSE = lfns.get(1);
			}
		}
		catch (final OptionException e) {
			commander.setReturnCode(ErrNo.EINVAL, e.getMessage());
			setArgumentsOk(false);
		}
	}

	@Override
	public void run() {
		if (this.useLFNasGuid && !GUIDUtils.isValidGUID(this.lfn)) {
			commander.setReturnCode(ErrNo.EINVAL, "Invalid GUID value: " + this.lfn);
			return;
		}

		if (this.ses.size() == 0 && this.dstSE != null && this.dstSE.length() != 0)
			this.ses.add(this.dstSE);

		if (this.ses.size() != 0 || this.qos.size() != 0) {
			HashMap<String, Long> results;
			try {
				final List<String> toMirrorEntries;

				if (!this.useLFNasGuid) {
					final LFN currentDir = commander.getCurrentDir();

					final String absolutePath = FileSystemUtils.getAbsolutePath(commander.user.getName(), currentDir != null ? currentDir.getCanonicalName() : null, this.lfn);

					toMirrorEntries = FileSystemUtils.expandPathWildCards(absolutePath, commander.user);

					if (toMirrorEntries == null || toMirrorEntries.isEmpty()) {
						commander.setReturnCode(ErrNo.ENOENT, this.lfn);
						return;
					}
				}
				else {
					toMirrorEntries = Arrays.asList(this.lfn);
				}

				for (final String toMirror : toMirrorEntries) {
					results = commander.c_api.mirrorLFN(toMirror, this.ses, this.exses, this.qos, this.useLFNasGuid, this.attempts, this.removeSourceSE);

					if (results == null && !this.useLFNasGuid && GUIDUtils.isValidGUID(this.lfn))
						results = commander.c_api.mirrorLFN(this.lfn, this.ses, this.exses, this.qos, true, this.attempts, this.removeSourceSE);

					if (results != null) {
						for (final Map.Entry<String, Long> entry : results.entrySet()) {
							String result_string;
							final String s = entry.getKey();
							final Long result = entry.getValue();

							if (result != null) {
								if (result.longValue() > 0)
									commander.printOutln(s + ": queued transfer ID " + result.longValue());
								else {
									result_string = JAliEnCommandmirror.errcode2Text(result.intValue());
									commander.printErrln(s + ": " + result_string);
								}
							}
							else
								commander.printErrln(s + ": unexpected error");
						}
					}
					else {
						commander.printErrln("Failed to mirror " + toMirror);
					}
				}
			}
			catch (final IllegalArgumentException e) {
				commander.setReturnCode(ErrNo.EINVAL, e.getMessage());
			}
		}
	}

	/**
	 * @param error
	 * @return string representation of the error code
	 */
	protected static String errcode2Text(final int error) {
		String text = null;
		switch (error) {
			case 0:
				text = "file already exists on SE";
				break;
			case -256:
				text = "problem getting LFN";
				break;
			case -320:
				text = "LFN name empty";
				break;
			case -330:
				text = "LFN name empty";
				break;
			case -350:
				text = "other problem";
				break;
			case -255:
				text = "no destination SE name";
				break;
			case -254:
				text = "unable to connect to SE";
				break;
			case -253:
				text = "empty SE list";
				break;
			case -1:
				text = "wrong mirror parameters";
				break;
			case -2:
				text = "database connection missing";
				break;
			case -3:
				text = "cannot locate real pfns";
				break;
			case -4:
				text = "DB query failed";
				break;
			case -5:
				text = "DB query didn't generate a transfer ID";
				break;
			case -6:
				text = "cannot locate the archive LFN to mirror";
				break;
			default:
				text = "Unknown error code: " + error;
				break;
		}
		return text;
	}

	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln("mirror Copies/moves a file to one or more other SEs");
		commander.printOutln(" Usage:");
		commander.printOutln("	mirror [-g] [-try <number>] [-r SE] [-S [se[,se2[,!se3[,qos:count]]]]] <lfn> [<SE>]");
		commander.printOutln("                 -g:     Use the lfn as a guid");
		commander.printOutln("                 -S:     specifies the destination SEs/tags to be used");
		commander.printOutln("                 -r:     remove this source replica after a successful transfer (a `move` operation)");
		commander.printOutln("                 -try <attempts>     Specifies the number of attempts to try and mirror the file (default 5)");
		commander.printOutln();
	}

	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}
}