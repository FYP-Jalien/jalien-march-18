package alien.site.containers;

import java.util.ArrayList;
import java.util.List;

import alien.site.packman.CVMFS;

/**
 * @author mstoretv
 */
public class SingularityCVMFS extends Containerizer {

	@Override
	public List<String> containerize(final String cmd) {
		final List<String> singularityCmd = new ArrayList<>();
		singularityCmd.add(CVMFS.getSingularityPath() + "/" + "singularity");
		singularityCmd.add("exec");
		singularityCmd.add("-C");
		singularityCmd.add("-B");

		if(workdir != null) {
			singularityCmd.add("/cvmfs:/cvmfs,/tmp:/tmp," + workdir + ":" + CONTAINER_JOBDIR); //TODO: remove /tmp after testing (not needed)
			singularityCmd.add("--pwd");
			singularityCmd.add(CONTAINER_JOBDIR);
		}
		else
			singularityCmd.add("/cvmfs:/cvmfs");

		singularityCmd.add(containerImgPath);
		singularityCmd.add("/bin/bash");
		singularityCmd.add("-c");
		singularityCmd.add(envSetup + cmd);

		return singularityCmd;
	}
}