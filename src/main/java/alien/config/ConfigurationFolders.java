package alien.config;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import alien.user.UserFactory;
import lazyj.ExtProperties;

/**
 * Configuration is loaded from files found in a set (via the AliEnConfig java parameter), or default ($HOME/.alien/config), configuration folder
 */
class ConfigurationFolders implements ConfigSource {
	private Map<String, ExtProperties> oldConfig;

	/**
	 * @param oldConfig to inherit
	 */
	public ConfigurationFolders(final Map<String, ExtProperties> oldConfig) {
		this.oldConfig = oldConfig;
	}

	@Override
	public Map<String, ExtProperties> getConfiguration() {
		return getFromConfigFolders(oldConfig);
	}

	private static Map<String, ExtProperties> getFromConfigFolders(final Map<String, ExtProperties> oldConfigFiles) {
		Map<String, ExtProperties> tmp = new HashMap<>();

		// configuration files in the indicated config folder overwrite the defaults from classpath
		// TODO: extract into a method, return a map, merge with the otheronfigFiles
		// NOTE: this method extends previously found properties!
		final String defaultConfigLocation = UserFactory.getUserHome() + System.getProperty("file.separator") + ".alien" + System.getProperty("file.separator") + "config";
		final String configOption = System.getProperty("AliEnConfig", "config");

		final List<String> configFolders = Arrays.asList(defaultConfigLocation, configOption);

		for (final String path : configFolders) {
			final File f = new File(path);

			if (f.exists() && f.isDirectory() && f.canRead()) {
				final File[] list = f.listFiles();

				if (list != null)
					for (final File sub : list)
						if (sub.isFile() && sub.canRead() && sub.getName().endsWith(".properties")) {
							String sName = sub.getName();
							sName = sName.substring(0, sName.lastIndexOf('.'));

							ExtProperties oldProperties = oldConfigFiles.get(sName.toLowerCase());

							if (oldProperties == null)
								oldProperties = new ExtProperties();

							final ExtProperties prop = new ExtProperties(path, sName, oldProperties, true);
							prop.setAutoReload(1000 * 60);

							tmp.put(sName.toLowerCase(), prop);
						}
			}
		}

		return tmp;
	}

}
