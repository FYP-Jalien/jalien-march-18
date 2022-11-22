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
	protected String containerImgPath;

	/**
	 * Working directory
	 */
	String workdir = null;

	/**
	 * Debug cmd to run
	 * 
	 */
	protected String debugCmd = "";

	/**
	 * For resource constraints
	 */
	protected int memLimit = 0;

	/**
	 * GPU
	 */
	private static final String cudaDevices = System.getenv().containsKey("CUDA_VISIBLE_DEVICES") ? " && echo export CUDA_VISIBLE_DEVICES=" + System.getenv().get("CUDA_VISIBLE_DEVICES") : "";
	private static final String rocrDevices = System.getenv().containsKey("ROCR_VISIBLE_DEVICES") ? " && echo export ROCR_VISIBLE_DEVICES=" + System.getenv().get("ROCR_VISIBLE_DEVICES") : "";

	/**
	 * ApMon
	 */
	private static final String apmonConfig = System.getenv().containsKey("APMON_CONFIG") ? " && echo export APMON_CONFIG=" + System.getenv().get("APMON_CONFIG") : "";

	/**
	 * Command to set the environment for container
	 */
	protected static final String envSetup = "source <( " + CVMFS.getAlienvPrint() + apmonConfig + cudaDevices + rocrDevices + " ); ";

	/**
	 * Simple constructor, initializing the container path from default location or from config/environment (DEFAULT_JOB_CONTAINER_PATH key)
	 */
	public Containerizer() {
		containerImgPath = System.getenv().getOrDefault("JOB_CONTAINER_PATH", DEFAULT_JOB_CONTAINER_PATH);
		if (!containerImgPath.equals(DEFAULT_JOB_CONTAINER_PATH)) {
			logger.log(Level.INFO, "Custom JOB_CONTAINER_PATH set. Will use the following image instead: " + containerImgPath);
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
	public final static String getGPUString() {
		final Pattern p = Pattern.compile("^nvidia\\d+$");
		String[] names = new File("/dev").list((dir, name) -> name.equals("kfd") || p.matcher(name).matches());

		return String.join(",", names);
	}

	/**
	 * @return String for binding a number of predefined directories in a container, as required for GPU
	 */
	public static final String getGPUdirs() {
		String toBind = "";

		if (new File("/etc/alternatives").exists())
			toBind += "/etc/alternatives:/etc/alternatives,";

		if (new File("/opt").exists())
			toBind += "/opt:/opt,";

		return toBind;
	}

	public static final String getCustomBinds() {
		return System.getenv().getOrDefault("ADDITIONAL_BINDS", "").isBlank() ? "" : System.getenv().get("ADDITIONAL_BINDS") + ",";
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
	 * Override the container to be used for job
	 * 
	 * @param newContainerPath
	 */
	public void setContainerPath(final String newContainerPath) {
		containerImgPath = newContainerPath;
	}

	/**
	 * Workdir to be mounted in the container as CONTAINER_JOBDIR
	 * 
	 * @param newWorkdir
	 */
	public void setWorkdir(final String newWorkdir) {
		workdir = newWorkdir;
	}

	/**
	 * Provide JDL to use with cgroups v2
	 * 
	 * @param memLimit
	 */
	public void setMemLimit(final int newMemLimit) {
		memLimit = newMemLimit;
	}

	/**
	 * @return working directory
	 */
	public String getWorkdir() {
		return workdir;
	}

	/**
	 * 
	 * Applies options from debugtag to job container
	 * 
	 * @param debugTag
	 */
	public void enableDebug(final String debugTag) {
		try {
			if (debugTag.contains("@")) {
				final String[] debugParams = debugTag.split("@");

				debugCmd = !debugParams[0].isBlank() ? debugParams[0] : debugCmd;
				containerImgPath = !debugParams[1].isBlank() ? debugParams[1] : containerImgPath;
			}
		}
		catch (final Exception e) {
			logger.log(Level.WARNING, "Unable to parse debugTag: " + debugTag);
		}
	}

	/**
	 * @return Class name of the container wrapping code
	 */
	public String getContainerizerName() {
		return this.getClass().getSimpleName();
	}
}
