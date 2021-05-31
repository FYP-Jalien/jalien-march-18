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

	/**
	 * @return the only intance of optimizers for this JVM
	 */
	public static synchronized JOptimizers getInstance() {
		if (_instance == null)
			_instance = new JOptimizers();

		return _instance;
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	/*
	 * public static void main(final String[] args) throws Exception {
	 * final Optimizer opt = new Optimizer();
	 * opt.start();
	 * }
	 */
}
