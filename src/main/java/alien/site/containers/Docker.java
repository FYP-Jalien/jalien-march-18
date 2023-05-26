package alien.site.containers;

import java.util.ArrayList;
import java.util.List;

/**
 * @author mstoretv
 */
public class Docker extends Containerizer {

	@Override
	public List<String> containerize(final String cmd) {
		final List<String> dockerCmd = new ArrayList<>();
		dockerCmd.add("docker");
		dockerCmd.add("run");
		dockerCmd.add("-v");
		dockerCmd.add("/cvmfs:/cvmfs");
		dockerCmd.add("-v");
		dockerCmd.add("/tmp:/tmp"); // TODO: remove /tmp after testing (not needed)

		if (workdir != null) {
			dockerCmd.add("-v");
			dockerCmd.add(workdir + ":" + CONTAINER_JOBDIR);
			dockerCmd.add("-w");
			dockerCmd.add(CONTAINER_JOBDIR);
		}

		dockerCmd.add("-i");
		dockerCmd.add(containerImgPath);
		dockerCmd.add("/bin/bash");
		dockerCmd.add("-c");
		dockerCmd.add(envSetup + cmd);

		return dockerCmd;
	}
}