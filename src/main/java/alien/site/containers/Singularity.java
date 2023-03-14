package alien.site.containers;

/**
 * @author mstoretv
 */
public class Singularity extends Apptainer {

	@Override
	public String getBinPath(){
		return System.getenv().getOrDefault("FORCE_BINPATH", "singularity");
	}
}
