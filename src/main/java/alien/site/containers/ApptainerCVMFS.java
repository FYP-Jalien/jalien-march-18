package alien.site.containers;

import java.util.ArrayList;
import java.util.List;

import alien.site.packman.CVMFS;

/**
 * @author mstoretv
 */
public class ApptainerCVMFS extends Containerizer {

	@Override
	public List<String> containerize(final String cmd) {
		final List<String> apptainerCmd = new ArrayList<>();
		apptainerCmd.add(CVMFS.getApptainerPath() + "/" + "apptainer");
		apptainerCmd.add("exec");
		apptainerCmd.add("-C");

		final String gpuString = getGPUString();
		String gpuDirs = getGPUdirs();

		if (gpuString.contains("nvidia"))
			apptainerCmd.add("--nv");
		else if (gpuString.contains("kfd"))
			apptainerCmd.add("--rocm");
		else
			gpuDirs = "";

		apptainerCmd.add("-B");
		if(workdir != null) {
			apptainerCmd.add(getCustomBinds() + gpuDirs + "/cvmfs:/cvmfs," + workdir + ":" + CONTAINER_JOBDIR + "," + "/tmp:/tmp");
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
}
