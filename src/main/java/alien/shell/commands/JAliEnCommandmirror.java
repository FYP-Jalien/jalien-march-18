package alien.shell.commands;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

import alien.catalogue.FileSystemUtils;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
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
						else
							if (spec.contains(":"))
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
							else
								if (!spec.equals(""))
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
			printHelp();
			throw e;
		}
	}

	@Override
	public void run() {
		if (this.useLFNasGuid && !GUIDUtils.isValidGUID(this.lfn)) {
			commander.printErrln("Invalid GUID was specified");
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

					if (toMirrorEntries.size() == 0) {
						commander.printErrln("No such file: " + this.lfn);

						return;
					}
				}
				else {
					toMirrorEntries = Arrays.asList(this.lfn);
				}

				for (final String toMirror : toMirrorEntries) {
					results = commander.c_api.mirrorLFN(toMirror, this.ses, this.exses, this.qos, this.useLFNasGuid, this.attempts);

					if (results == null && !this.useLFNasGuid && GUIDUtils.isValidGUID(this.lfn))
						results = commander.c_api.mirrorLFN(this.lfn, this.ses, this.exses, this.qos, true, this.attempts);

					if (results != null) {
						for (final String s : results.keySet()) {
							String result_string;
							final Long result = results.get(s);

							if (result != null) {
								if (result.longValue() > 0)
									commander.printOutln(s + ": queued transfer ID " + result.longValue());
								else {
									result_string = JAliEnCommandmirror.Errcode2Text(result.intValue());
									commander.printErrln(s + ": " + result_string);
								}
							}
							else
								commander.printErrln(s + ": unexpected error");
						}
					}
					else {
						commander.printErrln("Couldn't execute the mirrror command, argument not found");
					}
				}
			}
			catch (final IllegalArgumentException e) {
				commander.printErrln(e.getMessage());
			}
		}
	}

	/**
	 * @param error
	 * @return string representation of the error code
	 */
	protected static String Errcode2Text(final int error) {
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
		commander.printOutln("mirror Copies a file into another SE");
		commander.printOutln(" Usage:");
		commander.printOutln("	mirror [-g] [-try <number>] [-S [se[,se2[,!se3[,qos:count]]]]] <lfn> [<SE>]");
		commander.printOutln("                 -g:      Use the lfn as a guid");
		commander.printOutln("                 -S:     specifies the destination SEs to be used");
		commander.printOutln("                 -try <NumOfAttempts>     Specifies the number of attempts to try and mirror the file");
		commander.printOutln();
	}

	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}
}