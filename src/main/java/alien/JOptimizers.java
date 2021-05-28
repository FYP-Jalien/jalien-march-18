package alien;

import alien.optimizers.Optimizer;

/**
 * @author mmmartin
 * @since Aug 9, 2016
 */
public class JOptimizers {

	private static JOptimizers _instance;
	/**
	 * @param args
	 * @throws Exception
	 */
	private JOptimizers() {
		final Optimizer opt = new Optimizer();
		opt.start();
	}

	public static JOptimizers getInstance() throws Exception {
		if (_instance == null)
			_instance = new JOptimizers();
		return _instance;
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
