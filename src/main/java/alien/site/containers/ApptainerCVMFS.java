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
		if (gpuString.contains("nvidia"))
			apptainerCmd.add("--nv");
		if (gpuString.contains("kfd"))
			apptainerCmd.add("--rocm");

		apptainerCmd.add("-B");
		if(workdir != null) {
			apptainerCmd.add("/cvmfs:/cvmfs,/tmp:/tmp," + workdir + ":" + CONTAINER_JOBDIR); //TODO: remove /tmp after testing (not needed)
			apptainerCmd.add("--pwd");
			apptainerCmd.add(CONTAINER_JOBDIR);
		}
		else
			apptainerCmd.add("/cvmfs:/cvmfs");

		apptainerCmd.add(containerImgPath);
		apptainerCmd.add("/bin/bash");
		apptainerCmd.add("-c");
		apptainerCmd.add(envSetup + cmd);

		return apptainerCmd;
	}
}
