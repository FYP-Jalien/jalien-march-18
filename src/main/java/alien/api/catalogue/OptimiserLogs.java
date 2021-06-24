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
	private final boolean verbose;
	private final boolean listClasses;
	private final int frequency;
	private String logOutput;

	/**
	 * @param classes optimizers to get the log of
	 * @param frequency new execution interval to set for the optimizers
	 * @param verbose be verbose on the details of last execution
	 * @param listClasses return just the list of matching classes
	 */
	public OptimiserLogs(final List<String> classes, final int frequency, final boolean verbose, final boolean listClasses) {
		this.classes = classes;
		this.frequency = frequency;
		this.verbose = verbose;
		this.listClasses = listClasses;
		this.logOutput = "";
	}

	@Override
	public void run() {

		if (classes == null || classes.isEmpty()) {
			classes = getRegisteredClasses();
		}

		if (frequency != 0)
			if (getEffectiveRequester().canBecome("admin")) {
				modifyFrequency(frequency, classes);
			}
			else {
				logOutput = "Only users with role admin can execute this call";
			}

		if (listClasses) {
			logOutput = logOutput + "Classnames matching query : \n";
			for (final String className : classes) {
				logOutput = logOutput + "\t" + getFullClassName(className) + "\n";
			}
			logOutput = logOutput + "\n";
		}
		else {
			for (final String className : classes) {
				final String classLog = getLastLogFromDB(className, verbose, false);
				if (!classLog.isBlank()) {
					logOutput = logOutput + classLog + "\n";
				}
				else {
					logOutput = logOutput + "The introduced classname/keyword (" + className + ") is not registered. The classes in the database are : \n";
					for (final String classname : getRegisteredClasses())
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
	private static void modifyFrequency(final int frequency, final List<String> classes) {
		DBSyncUtils.modifyFrequency(frequency, classes);
	}

	/**
	 * Gets the recorded log in the db
	 *
	 * @param classname
	 * @return
	 */
	private static String getLastLogFromDB(final String classname, final boolean verbose, final boolean exactMatch) {
		return DBSyncUtils.getLastLog(classname, verbose, exactMatch);
	}

	/**
	 * Creates string with the classnames contained in the db.
	 *
	 * @return
	 */
	private static ArrayList<String> getRegisteredClasses() {
		return DBSyncUtils.getRegisteredClasses();
	}

	/**
	 * Gets the full classname for a given keyword
	 *
	 * @param className
	 * @return
	 */
	private static String getFullClassName(final String className) {
		return DBSyncUtils.getFullClassName(className);
	}

	@Override
	public String toString() {
		return "Asked for a optimiserLogs";
	}

	@Override
	public List<String> getArguments() {
		return null;
	}

	/**
	 * Gets the log to output to the user
	 *
	 * @return the log of the periodic execution of the requested classes
	 */
	public String getLogOutput() {
		return logOutput;
	}
}
