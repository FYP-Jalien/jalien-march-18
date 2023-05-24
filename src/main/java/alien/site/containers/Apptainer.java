package alien.site.containers;

import java.util.ArrayList;
import java.util.List;

/**
 * @author mstoretv
 */
public class Apptainer extends Containerizer {

	@Override
	public List<String> containerize(final String cmd) {
		final List<String> apptainerCmd = new ArrayList<>();
		apptainerCmd.add(getBinPath());
		apptainerCmd.add("exec");
		apptainerCmd.add("-C");

		if (useGpu) {
			final String gpuString = getGPUString();
			if (gpuString.contains("nvidia"))
				apptainerCmd.add("--nv");
			else if (gpuString.contains("kfd"))
				apptainerCmd.add("--rocm");
		}

		apptainerCmd.add("-B");
		if (workdir != null) {
			apptainerCmd.add(getCustomBinds() + getGPUdirs() + "/cvmfs:/cvmfs," + workdir + ":" + CONTAINER_JOBDIR + "," + workdir + "/tmp:/tmp");
			apptainerCmd.add("--pwd");
			apptainerCmd.add(CONTAINER_JOBDIR);
		}
		else
			apptainerCmd.add("/cvmfs:/cvmfs");

		apptainerCmd.add("--no-mount");
		apptainerCmd.add("tmp");

		if (useCgroupsv2) {
			apptainerCmd.add("--memory");
			apptainerCmd.add(Integer.toString(memLimit) + "M");
			apptainerCmd.add("--memory-swap");
			apptainerCmd.add("0");
		}

		apptainerCmd.add(containerImgPath);
		apptainerCmd.add("/bin/bash");
		apptainerCmd.add("-c");
		apptainerCmd.add(envSetup + debugCmd + cmd);

		return apptainerCmd;
	}

	/**
	 * @return apptainer command to execute, default simply "apptainer" but can be overriden with the $FORCE_BINPATH environment variable
	 */
	@SuppressWarnings("static-method")
	protected String getBinPath() {
		return System.getenv().getOrDefault("FORCE_BINPATH", "apptainer");
	}
}
