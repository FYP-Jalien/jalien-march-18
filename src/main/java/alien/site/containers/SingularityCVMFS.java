package alien.site.containers;

import java.util.ArrayList;
import java.util.List;

import alien.site.packman.CVMFS;

/**
 * @author mstoretv
 */
public class SingularityCVMFS extends Containerizer {

	private static String dirsToMountForGPU = "/opt:/opt,/etc/alternatives:/etc/alternatives,";
	
	@Override
	public List<String> containerize(final String cmd) {
		final List<String> singularityCmd = new ArrayList<>();
		singularityCmd.add(CVMFS.getSingularityPath() + "/" + "singularity");
		singularityCmd.add("exec");
		singularityCmd.add("-C");

		final String gpuString = getGPUString();
		if (gpuString.contains("nvidia"))
			singularityCmd.add("--nv");
		else if (gpuString.contains("kfd"))
			singularityCmd.add("--rocm");
		else
			dirsToMountForGPU = "";

		singularityCmd.add("-B");
		if(workdir != null) {
			singularityCmd.add(dirsToMountForGPU + "/cvmfs:/cvmfs," + workdir + ":" + CONTAINER_JOBDIR + "," + workdir + "/tmp:/tmp");
			singularityCmd.add("--pwd");
			singularityCmd.add(CONTAINER_JOBDIR);
		}
		else
			singularityCmd.add("/cvmfs:/cvmfs");

		singularityCmd.add("--no-mount");
		singularityCmd.add("tmp");			

		singularityCmd.add(containerImgPath);
		singularityCmd.add("/bin/bash");
		singularityCmd.add("-c");
		singularityCmd.add(envSetup + cmd);

		return singularityCmd;
	}
}
