package alien.monitoring;

import java.util.Vector;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Access counters
 *
 * @author costing
 */
public final class Counter implements MonitoringObject, DerivedDataProducer {
	private final AtomicLong counter = new AtomicLong(0);

	private long oldValue = 0;

	private final String name;

	/**
	 * Default constructor
	 *
	 * @param name
	 */
	public Counter(final String name) {
		this.name = name;
	}

	/**
	 * Increment the counter with a positive value. When overflowing the counter will be reset to the given value.
	 *
	 * @param incrementCount
	 *
	 * @return the incremented value
	 */
	public long increment(final long incrementCount) {
		long value = counter.addAndGet(incrementCount);

		if (value == Long.MAX_VALUE || value < 0) {
			// reset counters when overflowing
			value = incrementCount;
			counter.set(value);
			oldValue = 0;
		}

		return value;
	}

	/**
	 * @return current absolute value of the counter
	 */
	public long longValue() {
		return counter.get();
	}

	private long lastRate = System.currentTimeMillis();

	/**
	 * Get the rate, number of changes since last call
	 *
	 * @return the rate
	 */
	double getRate() {
		final long now = System.currentTimeMillis();

		final long diff = now - lastRate;

		if (diff <= 0)
			return Double.NaN;

		lastRate = now;

		return getRate(diff / 1000d);
	}

	/**
	 * Get the increment rate in changes/second, since the last call
	 *
	 * @param timeInterval
	 *            interval in seconds
	 * @return changes/second
	 */
	double getRate(final double timeInterval) {
		if (timeInterval <= 0)
			return Double.NaN;

		final long value = longValue();

		double ret = Double.NaN;

		if (value >= oldValue)
			ret = (value - oldValue) / timeInterval;

		oldValue = value;

		return ret;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see alien.monitoring.MonitoringObject#fillValues(java.util.Vector, java.util.Vector)
	 */
	@Override
	public void fillValues(final Vector<String> paramNames, final Vector<Object> paramValues) {
		paramNames.add(name);
		paramValues.add(Double.valueOf(longValue()));

		final double rate = getRate();

		if (!Double.isNaN(rate)) {
			paramNames.add(name + "_R");
			paramValues.add(Double.valueOf(rate));
		}
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return name + ": " + counter.longValue();
	}
}
