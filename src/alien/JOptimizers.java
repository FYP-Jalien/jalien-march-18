package alien;

import alien.config.Context;
import alien.optimizers.Optimizer;

/**
 * @author mmmartin
 * @since Aug 9, 2016
 */
public class JOptimizers {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(final String[] args) throws Exception {
		Context.addToLoggingContext("JOptimizer");
		Optimizer opt = new Optimizer();
		opt.start();
	}
}
