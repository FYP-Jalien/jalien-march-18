package alien.site.containers;

import java.io.File;
import java.util.List;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import alien.config.ConfigUtils;
import alien.site.JobAgent;
import alien.site.packman.CVMFS;

/**
 * @author mstoretv
 */
public abstract class Containerizer {

	private static final String DEFAULT_JOB_CONTAINER_PATH = CVMFS.getContainerPath();
	
	/**
	 * Sandbox location
	 */
	protected static final String CONTAINER_JOBDIR = "/workdir";

	private static final Logger logger = ConfigUtils.getLogger(JobAgent.class.getCanonicalName());
	
	/**
	 * Location of the container
	 */
	protected final String containerImgPath;

	/**
	 * Working directory
	 */
	String workdir = null;
	
	/**
	 * Command to set the environment for container
	 */
	protected static final String envSetup = "source <( " + CVMFS.getAlienvPrint() + " && echo export APMON_CONFIG=" + System.getenv("APMON_CONFIG") + " && echo export CUDA_VISIBLE_DEVICES=" +
			System.getenv().get("CUDA_VISIBLE_DEVICES") + " && echo export ROCR_VISIBLE_DEVICES=" + System.getenv().get("ROCR_VISIBLE_DEVICES") + " ); ";

	/**
	 * Simple constructor, initializing the container path from default location or from the environment (DEFAULT_JOB_CONTAINER_PATH key)
	 */
	public Containerizer() {
		containerImgPath = System.getenv().getOrDefault("JOB_CONTAINER_PATH", DEFAULT_JOB_CONTAINER_PATH);
		if (containerImgPath.equals(DEFAULT_JOB_CONTAINER_PATH)) {
			logger.log(Level.INFO, "Environment variable JOB_CONTAINER_PATH not set. Using default path instead: " + DEFAULT_JOB_CONTAINER_PATH);
		}
	}

	/**
	 * @return <code>true</code> if running a simple command (java -version) is possible under the given container implementation
	 */
	public boolean isSupported() {
		final String javaTest = "java -version";

		boolean supported = false;
		try {
			final ProcessBuilder pb = new ProcessBuilder(containerize(javaTest));
			final Process probe = pb.start();
			probe.waitFor();

			try (Scanner cmdScanner = new Scanner(probe.getErrorStream())) {
				while (cmdScanner.hasNext()) {
					if (cmdScanner.next().contains("Runtime")) {
						supported = true;
					}
				}
			}
		}
		catch (final Exception e) {
			logger.log(Level.WARNING, "Failed to start container: " + e.toString());
		}
		return supported;
	}

	/**
	 * @return String representing supported GPUs by the system. Will contain either 'nvidia[0-9]' (Nvidia), 'kfd' (AMD), or none.
	 */
	public final String getGPUString() {
		final Pattern p = Pattern.compile("^nvidia\\d+$");
		String[] names = new File("/dev").list((dir, name) -> name.equals("kfd") || p.matcher(name).matches());

		return String.join(",", names);
	}

	/**
	 * Decorating arguments to run the given command under a container. Returns a
	 * list for use with ProcessBuilders
	 *
	 * @param cmd
	 * @return parameter
	 */
	public abstract List<String> containerize(String cmd);
	
	/**
	 * Decorating arguments to run the given command under a container. Returns a
	 * string for use within scripts
	 *
	 * @param cmd
	 * @return parameter
	 */
	public String containerizeAsString(String cmd) {
		String contCmd = String.join(" ", containerize(cmd));
		contCmd = contCmd.replaceAll(" \\$", " \\\\\\$"); //Prevent startup path from being expanded prematurely
		contCmd = contCmd.replaceAll("-c ", "-c \"") + "\""; //Wrap the command to be executed as a string 
		return contCmd;
	}

	/**
	 * @param newWorkdir
	 */
	public void setWorkdir(final String newWorkdir) {
		workdir = newWorkdir;
	}

	/**
	 * @return working directory
	 */
	public String getWorkdir() {
		return workdir;
	}

	/**
	 * @return Class name of the container wrapping code
	 */
	public String getContainerizerName() {
		return this.getClass().getSimpleName();
	}
}
