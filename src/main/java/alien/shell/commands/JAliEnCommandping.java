package alien.shell.commands;

import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import alien.api.Dispatcher;
import alien.api.Ping;
import alien.api.ServerException;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import lazyj.Format;

/**
 * @author costing
 * @since 2019-06-07
 */
public class JAliEnCommandping extends JAliEnBaseCommand {

	private int count = 3;

	private long sleep = 1000;

	private static final double NANO_TO_MS = 1000000.;

	/**
	 * Send a simple object upstream and measure how long it takes to have it back
	 */
	@Override
	public void run() {
		commander.printOut("Sending " + count + " messages");

		if (sleep > 0)
			commander.printOut(" with a pause of " + sleep + " ms between them");

		commander.printOutln();

		try {
			final long absStartTime = System.nanoTime();

			Ping p = null;

			long min = -1;
			long max = -1;
			long sum = 0;
			long sum2 = 0;

			for (int i = 0; i < count; i++) {
				final long startTime = System.nanoTime();

				p = Dispatcher.execute(new Ping());

				final long endTime = System.nanoTime();

				final long delta = endTime - startTime;

				if (min < 0 || delta < min)
					min = delta;

				max = Math.max(max, delta);

				sum += delta;
				sum2 += delta * delta;

				commander.printOutln("reply from " + p.getPartnerAddress() + ": time=" + Format.point(delta / NANO_TO_MS) + " ms");

				if (i < count && sleep > 0)
					try {
						Thread.sleep(sleep);
					}
					catch (@SuppressWarnings("unused") InterruptedException e) {
						return;
					}
			}

			final long absEndTime = System.nanoTime();

			commander.printOutln(count + " packets transmitted, time " + Format.point((absEndTime - absStartTime) / NANO_TO_MS) + " ms");

			double mdev = Math.sqrt(sum2 / count - (sum * sum) / (count * count));

			commander.printOut("rtt min/avg/max/mdev = ");
			commander.printOut(Format.point(min / NANO_TO_MS) + "/");
			commander.printOut(Format.point((sum / count) / NANO_TO_MS) + "/");
			commander.printOut(Format.point(max / NANO_TO_MS) + "/");
			commander.printOutln(Format.point(mdev / NANO_TO_MS) + " ms");

			if (p != null) {
				commander.printOutln();
				commander.printOutln("Central service endpoint information:");
				for (final Map.Entry<String, String> entry : p.getServerInfo().entrySet())
					commander.printOutln("  " + entry.getKey() + " : " + entry.getValue());
			}
		}
		catch (ServerException e) {
			logger.log(Level.WARNING, "Could not execute the ping command");
			e.getCause().printStackTrace();
		}
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("ping"));
		commander.printOutln(helpOption("-c", "Number of iterations"));
		commander.printOutln(helpOption("-s", "Sleep between iterations, default " + sleep + " (milliseconds)"));
		commander.printOutln();
	}

	/**
	 * There are no arguments to this command
	 *
	 * @return <code>true</code>
	 */
	@Override
	public boolean canRunWithoutArguments() {
		return true;
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
	public JAliEnCommandping(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);

		try {
			final OptionParser parser = new OptionParser();

			parser.accepts("c").withRequiredArg().describedAs("Number of iterations").ofType(Integer.class);
			parser.accepts("s").withRequiredArg().describedAs("Sleep between iterations").ofType(Long.class);

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			if (options.hasArgument("c"))
				count = ((Integer) options.valueOf("c")).intValue();

			if (count < 0 || count > 100) {
				commander.printOutln("Ignoring count value of " + count);
				count = 3;
			}

			if (options.hasArgument("s"))
				sleep = ((Long) options.valueOf("s")).intValue();

			if (sleep < 0 || sleep > 60000) {
				commander.printOutln("Ignoring sleep value of " + sleep);
				sleep = 1000;
			}
		}
		catch (final OptionException | IllegalArgumentException e) {
			printHelp();
			throw e;
		}
	}

}
