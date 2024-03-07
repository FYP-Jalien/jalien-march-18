package alien.shell.commands;

import java.util.ArrayList;
import java.util.List;

/**
 * @author ron
 * @since June 4, 2011
 */
public class JAliEnCommandtime extends JAliEnBaseCommand {

	@Override
	public void run() {

		if (alArguments.size() < 2) {
			printHelp();
			return;
		}

		final ArrayList<String> args = new ArrayList<>();
		args.addAll(alArguments);
		int times;
		try {
			times = Integer.parseInt(alArguments.get(0));
			args.remove(alArguments.get(0));
		}
		catch (@SuppressWarnings("unused") final NumberFormatException e) {
			printHelp();
			return;
		}
		
		if (times<=0) {
			printHelp();
			return;
		}

		final StringBuilder command = new StringBuilder(alArguments.get(1));
		args.remove(alArguments.get(1));

		JAliEnBaseCommand comm = null;
		try {
			comm = JAliEnCOMMander.getCommand(command.toString(), new Object[] { commander, args });
		}
		catch (final Exception e) {
			e.printStackTrace();
			return;
		}
		// comm.silent();

		final ArrayList<Long> timings = new ArrayList<>(times);

		for (int t = 0; t < times; t++) {
			final long lStart = System.currentTimeMillis();

			comm.run();
			timings.add(Long.valueOf(System.currentTimeMillis() - lStart));
		}

		long total = 0;

		for (final Long t : timings)
			total += t.longValue();

		for (final String s : args)
			command.append(' ').append(s);

		commander.printOutln("\"" + command + "\" with #" + times + " tries,\tavr/total msec:\t\t" + (total / times) + "/" + total);
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln("Usage: time <times>  <command> [command_arguments] ");
	}

	/**
	 * cat cannot run without arguments
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
	 */
	public JAliEnCommandtime(final JAliEnCOMMander commander, final List<String> alArguments) {
		super(commander, alArguments);
	}
}
