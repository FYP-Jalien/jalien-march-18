package utils;

/**
 * Hide warnings from other parts of the code
 * 
 * @author costing
 * @since Jan 10 2024
 */
public class Signals {
	/**
	 * @param signal
	 * @param r 
	 */
	public static void addHandler(final String signal, final Runnable r) {
		sun.misc.Signal.handle(new sun.misc.Signal(signal), sig -> r.run());
	}
}
