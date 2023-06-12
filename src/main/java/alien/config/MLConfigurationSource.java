package alien.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import lazyj.ExtProperties;
import lia.Monitor.monitor.AppConfig;

/**
 * @author nhardi
 *
 *         Configuration coming from MonaLisa. Used only when ML is configured.
 *
 */
public class MLConfigurationSource implements ConfigSource {
	@Override
	public Map<String, ExtProperties> getConfiguration() {
		final Map<String, ExtProperties> tmp = new HashMap<>();
		tmp.put("config", getConfigFromML());
		tmp.put("logging", getConfigFromML());
		return tmp;
	}

	private static ExtProperties getConfigFromML() {
		// TODO: remove duplicated code here and in ConfigUtils!
		final String mlConfigURL = System.getProperty("lia.Monitor.ConfigURL");
		final boolean hasMLConfig = mlConfigURL != null && mlConfigURL.trim().length() > 0;

		final ExtProperties tmp = new ExtProperties();

		if (hasMLConfig) {
			// assume running as a library inside ML code, inherit the configuration keys from its main config file
			final Properties mlConfigProperties = AppConfig.getPropertiesConfigApp();

			for (final String key : mlConfigProperties.stringPropertyNames())
				tmp.set(key, mlConfigProperties.getProperty(key));
		}

		return tmp;
	}

}
