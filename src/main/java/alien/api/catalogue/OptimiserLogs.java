package alien.api.catalogue;

import java.util.ArrayList;
import java.util.List;

import alien.api.Request;
import alien.optimizers.DBSyncUtils;

/**
 * Outputs the logs stored in the optimizers db
 *
 * @author Marta
 * @since 2021-05-28
 */
public class OptimiserLogs extends Request {
	private static final long serialVersionUID = -8097151852196189205L;

	private List<String> classes;
	private boolean verbose;
	private boolean listClasses;
	private int frequency;
	private String logOutput;

	public OptimiserLogs(List<String> classes, int frequency, boolean verbose, boolean listClasses) {
		this.classes = classes;
		this.frequency = frequency;
		this.verbose = verbose;
		this.listClasses = listClasses;
		this.logOutput = "";
	}

	@Override
	public void run() {

		if (frequency != 0)
			OptimiserLogs.modifyFrequency(frequency, classes);

		if (listClasses) {
			logOutput = logOutput + "Classnames matching query : \n";
			for (String className : classes) {
				logOutput = logOutput + "\t" + OptimiserLogs.getFullClassName(className) + "\n";
			}
			logOutput = logOutput + "\n";
		}
		else {
			for (String className : classes) {
				String classLog = OptimiserLogs.getLastLogFromDB(className, verbose, false);
				if (classLog != "") {
					logOutput = logOutput + classLog + "\n";
				}
				else {
					logOutput = logOutput + "The introduced classname/keyword (" + className + ") is not registered. The classes in the database are : \n";
					for (String classname : OptimiserLogs.getRegisteredClasses())
						logOutput = logOutput + "\t" + classname + "\n";
				}
			}
		}
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

	/**
	 * Gets the log to output to the user
	 *
	 * @return
	 */
	public String getLogOutput() {
		return logOutput;
	}
}
