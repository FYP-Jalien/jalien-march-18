/**
 *
 */
package alien.api.catalogue;

import java.util.Arrays;
import java.util.List;

import alien.api.Cacheable;
import alien.api.Request;
import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import lazyj.DBFunctions;

/**
 * @author costing
 * @since Jun 22, 2021
 */
public class GetAliEnv extends Request implements Cacheable {
	private static final long serialVersionUID = -291036303440798251L;

	private static final Monitor monitor = MonitorFactory.getMonitor(GetAliEnv.class.getCanonicalName());

	private final String packageNames;
	private final String keyModifier;

	private String cachedAliEnvOutput;

	/**
	 * @param packageNames list of package names
	 * @param keyModifier operating system or other factors that impact execution of the `alienv printenv` command
	 */
	public GetAliEnv(final List<String> packageNames, final String keyModifier) {
		this(String.join(",", packageNames), keyModifier);
	}

	/**
	 * @param packageNames comma-separated package names
	 * @param keyModifier operating system or other factors that impact execution of the `alienv printenv` command
	 */
	public GetAliEnv(final String packageNames, final String keyModifier) {
		this.packageNames = packageNames;
		this.keyModifier = keyModifier;
	}

	@Override
	public void run() {
		try (DBFunctions db = ConfigUtils.getDB("admin")) {
			if (db != null) {
				db.query("SELECT cachedOutput FROM alienv_cache WHERE packageNames=? AND keyModifier=? AND expires>UNIX_TIMESTAMP();", false, packageNames, keyModifier);

				if (db.moveNext()) {
					cachedAliEnvOutput = db.gets(1);

					if (monitor != null)
						monitor.incrementCacheHits("alienv");

					return;
				}
			}
		}

		if (monitor != null)
			monitor.incrementCacheMisses("alienv");
	}

	@Override
	public String getKey() {
		return packageNames + "#" + keyModifier;
	}

	@Override
	public long getTimeout() {
		return 1000 * 60 * 60;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(packageNames, keyModifier);
	}

	/**
	 * @return the cached value, if any
	 */
	public String getCachedAliEnOutput() {
		return cachedAliEnvOutput;
	}
}
