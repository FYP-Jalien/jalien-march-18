package alien.monitoring;

import java.util.Vector;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Access counters
 *
 * @author costing
 */
public final class CacheMonitor implements MonitoringObject, DerivedDataProducer {
	private final String name;

	private final AtomicLong hits = new AtomicLong(0);
	private final AtomicLong misses = new AtomicLong(0);

	private long oldHits = 0;
	private long oldMisses = 0;

	private final long started = System.currentTimeMillis();

	private long lastFill = System.currentTimeMillis();

	/**
	 * Default constructor
	 *
	 * @param name
	 */
	public CacheMonitor(final String name) {
		this.name = name;
	}

	/**
	 * Increment the number of hits
	 *
	 * @return new absolute value of hits
	 */
	public long incrementHits() {
		final long newValue = hits.incrementAndGet();

		if (newValue == Long.MAX_VALUE) {
			hits.set(1);
			oldHits = 0;

			return 1;
		}

		return newValue;
	}

	/**
	 * Increment the number of misses
	 *
	 * @return new absolute value of misses
	 */
	public long incrementMisses() {
		final long newValue = misses.incrementAndGet();

		if (newValue == Long.MAX_VALUE) {
			misses.set(1);
			oldMisses = 0;

			return 1;
		}

		return newValue;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see alien.monitoring.MonitoringObject#fillValues(java.util.Vector, java.util.Vector)
	 */
	@Override
	public void fillValues(final Vector<String> paramNames, final Vector<Object> paramValues) {
		final long now = System.currentTimeMillis();

		final long diff = now - lastFill;

		lastFill = now;

		if (diff <= 0) {
			return;
		}

		final double diffSeconds = diff / 1000d;

		final long absHits = hits.get();
		final long absMisses = misses.get();
		final long absTotal = absHits + absMisses;

		final double absDiffSeconds = (now - started) / 1000d;

		paramNames.add(name + "_abs_misses");
		paramValues.add(Double.valueOf(absMisses));

		paramNames.add(name + "_abs_hits");
		paramValues.add(Double.valueOf(absHits));

		paramNames.add(name + "_abs_total");
		paramValues.add(Double.valueOf(absTotal));

		if (absDiffSeconds > 0) {
			paramNames.add(name + "_abs_misses_R");
			paramValues.add(Double.valueOf(absMisses / absDiffSeconds));

			paramNames.add(name + "_abs_hits_R");
			paramValues.add(Double.valueOf(absHits / absDiffSeconds));

			paramNames.add(name + "_abs_total_R");
			paramValues.add(Double.valueOf(absTotal / absDiffSeconds));
		}

		if (absTotal > 0) {
			paramNames.add(name + "_abs_hit_percent");
			paramValues.add(Double.valueOf(absHits * 100d / absTotal));

			paramNames.add(name + "_abs_miss_percent");
			paramValues.add(Double.valueOf(absMisses * 100d / absTotal));
		}

		final long diffHits = absHits - oldHits;
		final long diffMisses = absMisses - oldMisses;

		if (diffHits >= 0 && diffMisses >= 0) {
			final long diffTotal = diffHits + diffMisses;

			paramNames.add(name + "_hits");
			paramValues.add(Double.valueOf(diffHits));

			paramNames.add(name + "_misses");
			paramValues.add(Double.valueOf(diffMisses));

			paramNames.add(name + "_total");
			paramValues.add(Double.valueOf(diffTotal));

			paramNames.add(name + "_hits_R");
			paramValues.add(Double.valueOf(diffHits / diffSeconds));

			paramNames.add(name + "_misses_R");
			paramValues.add(Double.valueOf(diffMisses / diffSeconds));

			paramNames.add(name + "_total_R");
			paramValues.add(Double.valueOf(diffTotal / diffSeconds));

			if (diffTotal > 0) {
				paramNames.add(name + "_hit_percent");
				paramValues.add(Double.valueOf(diffHits * 100d / diffTotal));

				paramNames.add(name + "_miss_percent");
				paramValues.add(Double.valueOf(diffMisses * 100d / diffTotal));
			}
		}

		oldHits = absHits;
		oldMisses = absMisses;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return name + ": hits: " + hits + ", misses: " + misses;
	}
}
