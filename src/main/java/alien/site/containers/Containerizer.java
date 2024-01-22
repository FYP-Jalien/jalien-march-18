package alien.site.containers;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import alien.config.ConfigUtils;
import alien.site.JobAgent;
import alien.site.packman.CVMFS;
import lazyj.commands.CommandOutput;
import lazyj.commands.SystemCommand;

/**
 * @author mstoretv
 */
public abstract class Containerizer {

	private static final String DEFAULT_JOB_CONTAINER_PATH = CVMFS.getDefaultContainerPath();

	/**
	 * Sandbox location
	 */
	protected static final String CONTAINER_JOBDIR = "/workdir";

	private static final Logger logger = ConfigUtils.getLogger(JobAgent.class.getCanonicalName());

	/**
	 * Location of the container
	 */
	protected static String containerImgPath = System.getenv().getOrDefault("JOB_CONTAINER_PATH", DEFAULT_JOB_CONTAINER_PATH);

	/**
	 * Working directory
	 */
	protected String workdir = null;

	/**
	 * Directories to bind-mount for GPU
	 */
	protected String gpuDirs = "";

	/**
	 * Debug cmd to run
	 * 
	 */
	protected String debugCmd = "";

	/**
	 * Set to false if mounting GPU libraries breaks container or no GPU strings found
	 */
	static boolean useGpu = true;

	/**
	 * GPU
	 */
	private static final String cudaDevices = System.getenv().containsKey("CUDA_VISIBLE_DEVICES") ? " && echo export CUDA_VISIBLE_DEVICES=" + System.getenv().get("CUDA_VISIBLE_DEVICES") : "";
	private static final String rocrDevices = System.getenv().containsKey("ROCR_VISIBLE_DEVICES") ? " && echo export ROCR_VISIBLE_DEVICES=" + System.getenv().get("ROCR_VISIBLE_DEVICES") : "";

	/**
	 * ApMon
	 */
	private static final String apmonConfig = System.getenv().containsKey("APMON_CONFIG") ? " && echo export APMON_CONFIG=" + System.getenv().get("APMON_CONFIG") : "";
	private static final String alienSite = System.getenv().containsKey("ALIEN_SITE") ? " && echo export ALIEN_SITE=" + System.getenv().get("ALIEN_SITE") : "";

	/**
	 * Simple constructor, initializing the container path from default location or from config/environment (DEFAULT_JOB_CONTAINER_PATH key)
	 */
	public Containerizer() {
		if (!containerImgPath.equals(DEFAULT_JOB_CONTAINER_PATH)) {
			logger.log(Level.INFO, "Custom JOB_CONTAINER_PATH set. Will use the following image instead: " + containerImgPath);
		}
	}

	/**
	 * Checks if running a simple command (java -version) is possible under the given container implementation.
	 * Also contains a sanity check (ps --version) to see if bind-mounted GPU libraries are incompatible with container
	 * 
	 * @return <code>true</code> if running a simple command (java -version) is possible under the given container implementation
	 */
	public boolean isSupported() {
		final String javaTest = "java -version && ps --version";
		try {
			CommandOutput output = SystemCommand.executeCommand(containerize(javaTest), true);
			try (BufferedReader br = output.reader()) {
				final String outputString = br.lines().collect(Collectors.joining());
				if (outputString != null && !outputString.isBlank()) {
					if (!outputString.contains("ps from"))
						useGpu = false;
					if (!outputString.contains("Runtime"))
						throw new EOFException("Unknown string: " + outputString);
				}
				else
					throw new EOFException("Returned container probe is null or blank: " + outputString);
			}
		}
		catch (final Exception e) {
			logger.log(Level.FINE, "The following containers appear to be unsupported: " + this.getClass().getSimpleName() + ". Reason: ", e);
			return false;
		}
		return true;
	}

	/**
	 * @return String representing supported GPUs by the system. Will contain either 'nvidia[0-9]' (Nvidia), 'kfd' (AMD), or none.
	 */
	public final static String getGPUString() {
		final Pattern p = Pattern.compile("^nvidia\\d+$");
		String[] names = new File("/dev").list((dir, name) -> name.equals("kfd") || p.matcher(name).matches());

		if (names == null || names.length == 0)
			useGpu = false;

		return String.join(",", names);
	}

	/**
	 * @return String for binding a number of predefined directories in a container, as required for GPU
	 */
	public static final String getGPUdirs() {
		String toBind = "";

		if (useGpu) {
			if (new File("/etc/alternatives").exists())
				toBind += "/etc/alternatives:/etc/alternatives,";
			if (new File("/opt").exists())
				toBind += "/opt:/opt,";
		}

		return toBind;
	}

	/**
	 * @return the value of the $ADDITIONAL_BINDS env variable
	 */
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
		contCmd = contCmd.replaceAll(" \\$", " \\\\\\$"); // Prevent startup path from being expanded prematurely
		contCmd = contCmd.replaceAll("-c ", "-c \"") + "\""; // Wrap the command to be executed as a string
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
	 * @return working directory
	 */
	public String getWorkdir() {
		return workdir;
	}

	/**
	 * 
	 * @return Command to set the environment for container as string
	 */
	protected static final String sourceEnvCmd(){
		final String containerImgPathExport = "echo export JOB_CONTAINER_PATH=" + containerImgPath;
		return "source <( " + containerImgPathExport + apmonConfig + alienSite + cudaDevices + rocrDevices + " ); ";
	}

	/**
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
			logger.log(Level.WARNING, "Unable to parse debugTag: " + debugTag, e);
		}
	}

	/**
	 * @return Class name of the container wrapping code
	 */
	public String getContainerizerName() {
		return this.getClass().getSimpleName();
	}
}
