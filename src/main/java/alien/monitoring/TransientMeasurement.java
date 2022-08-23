/**
 *
 */
package alien.monitoring;

import java.util.Vector;

/**
 * @author costing
 * @since Aug 19, 2022
 */
public class TransientMeasurement extends Measurement implements TransientMonitor {
	private boolean noData = false;

	/**
	 * @param name A string that prefixes all derived metrics
	 */
	public TransientMeasurement(final String name) {
		super(name);
	}

	@Override
	public synchronized void fillValues(final Vector<String> paramNames, final Vector<Object> paramValues) {
		noData = count == 0;
		super.fillValues(paramNames, paramValues);
	}

	@Override
	public boolean shouldStop() {
		return noData;
	}
}
