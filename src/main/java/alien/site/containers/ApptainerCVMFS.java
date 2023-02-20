package alien.site.containers;

import alien.site.packman.CVMFS;

/**
 * @author mstoretv
 */
public class ApptainerCVMFS extends Apptainer {

	@Override
	public String getBinPath(){
		return CVMFS.getApptainerPath() + "/" + "apptainer";
	}
}
