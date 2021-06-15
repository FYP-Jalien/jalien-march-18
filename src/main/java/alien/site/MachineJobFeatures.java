package alien.site;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;

/**
 * @author sweisz
 */
public class MachineJobFeatures {

	static transient final Logger logger = ConfigUtils.getLogger(MachineJobFeatures.class.getCanonicalName());
	private static String MachineFeaturesDir = System.getenv().getOrDefault("MACHINEFEATURES", "");
	private static String JobFeaturesDir = System.getenv().getOrDefault("JOBFEATURES", "");

	/**
	 * @author sweisz
	 */
	public enum FeatureType {
		/**
		 * Machine feature
		 */
		MACHINEFEATURE,
		/**
		 * Job featre
		 */
		JOBFEATURE;
	}

	/**
	 * @param fullPath
	 * @return the MJF parameters from files
	 */
	public static String getValueFromFile(final String fullPath) {
		String output = null;

		if (fullPath == null) {
			logger.log(Level.WARNING, "Can't pass null to getValueFromFile()");
			return null;
		}

		final File f = new File(fullPath);

		if (!f.exists() || !f.isFile() || !f.canRead()) {
			if (logger.isLoggable(Level.FINE))
				logger.log(Level.FINE, "Cannot access MJF file: " + fullPath);

			return null;
		}

		try (FileInputStream fis = new FileInputStream(f)) {
			output = String.valueOf(fis.readAllBytes());
		}
		catch (final IOException e) {
			logger.log(Level.WARNING, "Cannot read content of " + fullPath, e);
			return null;
		}

		return output;
	}

	private static String resolvePath(final String featureString, final FeatureType type) {
		String resolvedPath = null;

		switch (type) {
			case JOBFEATURE:
				resolvedPath = JobFeaturesDir + "/" + featureString;
				break;
			case MACHINEFEATURE:
				resolvedPath = MachineFeaturesDir + "/" + featureString;
				break;
			default:
				logger.log(Level.WARNING, "No matching feature type");
		}

		return resolvedPath;
	}

	private static String getFeature(final String featureString, final FeatureType type) {
		final String resolvedPath = resolvePath(featureString, type);

		return getValueFromFile(resolvedPath);
	}

	/**
	 * @param featureString
	 * @param type
	 * @param defaultString
	 * @return feature value or default if that is missing
	 */
	public static String getFeatureOrDefault(final String featureString, final FeatureType type, final String defaultString) {
		final String output = getFeature(featureString, type);

		return output != null ? output : defaultString;
	}

	/**
	 * @param featureString
	 * @param type
	 * @return feature value as number
	 */
	public static Long getFeatureNumber(final String featureString, final FeatureType type) {
		final String resolvedPath = resolvePath(featureString, type);

		final String output = getValueFromFile(resolvedPath);

		if (logger.isLoggable(Level.FINER))
			logger.log(Level.FINER, "Got value for " + featureString + " = " + output);

		return output != null ? Long.valueOf(output) : null;
	}

	/**
	 * @param featureString
	 * @param type
	 * @param defaultNumber
	 * @return feature value as number, of the default one if missing
	 */
	public static Long getFeatureNumberOrDefault(final String featureString, final FeatureType type, final Long defaultNumber) {
		final Long output = getFeatureNumber(featureString, type);

		return output != null ? output : defaultNumber;
	}
}
