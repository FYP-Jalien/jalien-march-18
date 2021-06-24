package alien.optimizers;

import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;

/**
 * @author Miguel
 * @since Aug 9, 2016
 */
public class Optimizer extends Thread {

	/**
	 * Logging facility
	 */
	static final Logger logger = ConfigUtils.getLogger(Optimizer.class.getCanonicalName());

	private long sleep_period = 60 * 1000L; // 1min

	private static String[] catalogue_optimizers = { "alien.optimizers.catalogue.LTables", "alien.optimizers.catalogue.GuidTable", "alien.optimizers.catalogue.ResyncLDAP", "utils.lfncrawler.LFNCrawler" };

	@Override
	public void run() {
		this.run("all");
	}

	/**
	 * @param type
	 *            which optimizer to start, can be one of "catalogue", "job", "transfer" or "all"
	 */
	public void run(final String type) {
		logger.log(Level.INFO, "Starting optimizers: " + type);

		if (!ConfigUtils.isCentralService()) {
			logger.log(Level.INFO, "We are not a central service :-( !");
			return;
		}

		switch (type) {
			case "catalogue":
				startCatalogueOptimizers();
				break;
			case "job":
				startJobOptimizers();
				break;
			case "transfer":
				startTransferOptimizers();
				break;
			default:
				startAllOptimizers();
		}
	}

	private void startAllOptimizers() {
		startCatalogueOptimizers();
		startJobOptimizers();
		startTransferOptimizers();
	}

	private static void startCatalogueOptimizers() {
		for (final String opt : catalogue_optimizers)
			try {
				final Optimizer optclass = (Optimizer) Class.forName(opt).getConstructor().newInstance();
				logger.log(Level.INFO, "New catalogue optimizer: " + opt);
				optclass.start();
			}
			catch (final ReflectiveOperationException e) {
				logger.log(Level.SEVERE, "Can't instantiate optimizer " + opt + "! " + e);
			}
	}

	private void startJobOptimizers() {
		// TODO
	}

	private void startTransferOptimizers() {
		// TODO
	}

	/**
	 * @return how often to check
	 */
	public long getSleepPeriod() {
		return this.sleep_period;
	}

	/**
	 * @param newSleepPeriod
	 *            period
	 */
	public void setSleepPeriod(final long newSleepPeriod) {
		this.sleep_period = newSleepPeriod;
	}
}
