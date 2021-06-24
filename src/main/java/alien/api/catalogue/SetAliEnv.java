/**
 *
 */
package alien.api.catalogue;

import java.util.Arrays;
import java.util.List;

import alien.api.Request;
import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import lazyj.DBFunctions;

/**
 * @author costing
 * @since Jun 22, 2021
 */
public class SetAliEnv extends Request {
	private static final long serialVersionUID = 3114356348162956273L;

	private static final Monitor monitor = MonitorFactory.getMonitor(SetAliEnv.class.getCanonicalName());

	private final String packageNames;
	private final String keyModifier;
	private String cachedAliEnvOutput;

	/**
	 * @param packageNames list of package names
	 * @param keyModifier operating system or other factors that impact execution of the `alienv printenv` command
	 * @param alienvOutput `alienv printenv` output on the client platform
	 */
	public SetAliEnv(final List<String> packageNames, final String keyModifier, final String alienvOutput) {
		this(String.join(",", packageNames), keyModifier, alienvOutput);
	}

	/**
	 * @param packageNames comma-separated package names
	 * @param keyModifier operating system or other factors that impact execution of the `alienv printenv` command
	 * @param alienvOutput alienvOutput `alienv printenv` output on the client platform
	 */
	public SetAliEnv(final String packageNames, final String keyModifier, final String alienvOutput) {
		this.packageNames = packageNames;
		this.keyModifier = keyModifier;
		this.cachedAliEnvOutput = alienvOutput;
	}

	@Override
	public void run() {
		try (DBFunctions db = ConfigUtils.getDB("admin")) {
			db.query("replace into alienv_cache (packageNames, keyModifier, expires, cachedOutput) values (?, ?, UNIX_TIMESTAMP()+60*60*12, ?);", false, packageNames, keyModifier, cachedAliEnvOutput);
		}

		monitor.incrementCounter("alienv_cache_set");

		// drop the largest blob of text from the reply
		this.cachedAliEnvOutput = null;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(packageNames, keyModifier, cachedAliEnvOutput);
	}
}
