package alien.site.containers;

import java.util.ArrayList;
import java.util.List;

/**
 * @author mstoretv
 */
public class Singularity extends Containerizer {

	@Override
	public List<String> containerize(final String cmd) {
		final List<String> singularityCmd = new ArrayList<>();
		singularityCmd.add("singularity");
		singularityCmd.add("exec");
		singularityCmd.add("-C");
		singularityCmd.add("-B");

		if(workdir != null) {
			singularityCmd.add("/cvmfs:/cvmfs," + workdir + ":" + CONTAINER_JOBDIR + "," + workdir + "/tmp:/tmp");
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