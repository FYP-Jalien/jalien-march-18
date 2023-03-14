package alien.site.containers;

import alien.site.packman.CVMFS;

/**
 * @author mstoretv
 */
public class ApptainerCVMFS extends Apptainer {

	@Override
	public String getBinPath(){
		return System.getenv().getOrDefault("FORCE_BINPATH", CVMFS.getApptainerPath() + "/" + "apptainer");
	}
}
