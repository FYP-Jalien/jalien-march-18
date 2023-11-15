package alien.site.containers;

import java.util.ArrayList;
import java.util.List;

/**
 * @author mstoretv
 */
public class Podman extends Containerizer {

	@Override
	public List<String> containerize(final String cmd) {
		final List<String> podmanCmd = new ArrayList<>();
		podmanCmd.add("podman");
		podmanCmd.add("run");
		podmanCmd.add("-v");
		podmanCmd.add("/cvmfs:/cvmfs");
		podmanCmd.add("-v");
		podmanCmd.add("/tmp:/tmp"); // TODO: remove /tmp after testing (not needed)

		if (workdir != null) {
			podmanCmd.add("-v");
			podmanCmd.add(workdir + ":" + CONTAINER_JOBDIR);
			podmanCmd.add("-w");
			podmanCmd.add(CONTAINER_JOBDIR);
		}

		podmanCmd.add("-i");
		podmanCmd.add(containerImgPath);
		podmanCmd.add("/bin/bash");
		podmanCmd.add("-c");
		podmanCmd.add(getEnvSetup() + cmd);

		return podmanCmd;
	}
}