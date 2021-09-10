package alien.site.containers;

import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;
import alien.site.JobAgent;

/**
 * @author mstoretv
 */
public class ContainerizerFactory {
	private static final Logger logger = ConfigUtils.getLogger(JobAgent.class.getCanonicalName());

	/**
	 * @author mstoretv
	 */
	enum Containerizers {
		/**
		 * Entry for Singularity (CVMFS)
		 */
		SingularityCVMFS,
		/**
		 * Entry for Singularity (local)
		 */
		Singularity,
		/**
		 * Entry for Docker
		 */
		Docker
	}

	/**
	 * @return the first supported container from the (Singularity, Docker) list, or <code>null</code> if none is supported at runtime
	 */
	public static Containerizer getContainerizer() {
		for (final Containerizers c : Containerizers.values()) {
			try {
				final Containerizer containerizerCandidate = (Containerizer) getClassFromName(c.name()).getConstructor().newInstance();
				if (containerizerCandidate.isSupported())
					return containerizerCandidate;
			}
			catch (final Exception e) {
				logger.log(Level.WARNING, "Invalid containerizer: " + e);
			}
		}
		return null;
	}

	private static Class<?> getClassFromName(final String name) throws ClassNotFoundException {
		final String pkg = Containerizer.class.getPackageName();
		return Class.forName(pkg + "." + name);
	}
}