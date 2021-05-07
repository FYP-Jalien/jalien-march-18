package alien;

import java.net.Socket;

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
	public JOptimizers() throws Exception {
		final Optimizer opt = new Optimizer();
		opt.start();
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	/*public static void main(final String[] args) throws Exception {
		final Optimizer opt = new Optimizer();
		opt.start();
	}*/
}
