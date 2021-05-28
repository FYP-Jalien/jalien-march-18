package alien.api.catalogue;

import java.util.ArrayList;
import java.util.List;

import alien.api.Request;
import alien.optimizers.DBSyncUtils;
import alien.optimizers.catalogue.ResyncLDAP;

/**
 * Outputs the logs stored in the optimizers db
 *
 * @author Marta
 * @since 2021-05-28
 */
public class OptimiserLogs extends Request {
	private static final long serialVersionUID = -8097151852196189205L;

	public OptimiserLogs() {
	}


	/**
	 * Modifies the frequency for the classes in the db
	 *
	 * @param frequency
	 * @param classes
	 */
	public static void modifyFrequency(int frequency, List<String> classes) {
		DBSyncUtils.modifyFrequency(frequency, classes);
	}

	/**
	 * Gets the recorded log in the db
	 *
	 * @param classname
	 * @return
	 */
	public static String getLastLogFromDB(String classname, boolean verbose, boolean exactMatch) {
		return DBSyncUtils.getLastLog(classname, verbose, exactMatch);
	}

	/**
	 * Creates string with the classnames contained in the db.
	 *
	 * @return
	 */
	public static ArrayList<String> getRegisteredClasses() {
		return DBSyncUtils.getRegisteredClasses();
	}

	/**
	 * Gets the full classname for a given keyword
	 *
	 * @param className
	 * @return
	 */
	public static String getFullClassName(String className) {
		return DBSyncUtils.getFullClassName(className);
	}

	@Override
	public String toString() {
		return "Asked for a manual resyncLDAP";
	}

	@Override
	public List<String> getArguments() {
		return null;
	}

	@Override
	public void run() {
	}
}
