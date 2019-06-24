package alien.config;

import java.util.HashMap;
import java.util.Map;

import lazyj.ExtProperties;

/**
 * @author nhardi
 *
 *         Load the system properties (command line flags) as they were defined in
 *         a properties file named "config".
 *
 */
public class SystemConfiguration implements ConfigSource {
	@Override
	public Map<String, ExtProperties> getConfiguration() {
		final Map<String, ExtProperties> tmp = new HashMap<>();

		final ExtProperties systemValues = new ExtProperties();

		for (final Map.Entry<String, String> entry : System.getenv().entrySet())
			systemValues.set(entry.getKey(), entry.getValue());

		for (final Map.Entry<Object, Object> entry : System.getProperties().entrySet())
			systemValues.set(entry.getKey().toString(), entry.getValue().toString());

		tmp.put("config", systemValues);

		return tmp;
	}
}
