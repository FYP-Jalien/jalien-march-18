package alien.site.containers;

import java.util.ArrayList;
import java.util.List;

import alien.site.packman.CVMFS;

/**
 * @author mstoretv
 */
public class PodmanCVMFS extends Containerizer {

	@Override
	public List<String> containerize(final String cmd, boolean containall) {
		final List<String> podmanCmd = new ArrayList<>();
		podmanCmd.add(CVMFS.getPodmanPath() + "/" + "podman");
		podmanCmd.add("run");
		podmanCmd.add("--security-opt=seccomp=unconfined");
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

		podmanCmd.add("-it");
		podmanCmd.add("--read-only");
		podmanCmd.add("--rootfs");
		podmanCmd.add(containerImgPath);
		podmanCmd.add("/bin/bash");
		podmanCmd.add("-c");
		podmanCmd.add(envSetup + cmd);

		return podmanCmd;
	}
}