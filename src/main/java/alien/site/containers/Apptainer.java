package alien.site.containers;

import java.util.ArrayList;
import java.util.List;

/**
 * @author mstoretv
 */
public class Apptainer extends Containerizer {

	@Override
	public List<String> containerize(final String cmd, boolean containall) {
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
		if(workdir != null) {
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
		if (containall) {
			apptainerCmd.add(envSetup + debugCmd + cmd);
		}
		else
			apptainerCmd.add(debugCmd + cmd);
	
		return apptainerCmd;
	}

	public String getBinPath() {
		return System.getenv().getOrDefault("FORCE_BINPATH", "apptainer");
	}
}
