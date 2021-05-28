package alien.api.catalogue;

import java.util.List;

import alien.api.Request;
import alien.optimizers.DBSyncUtils;
import alien.optimizers.catalogue.ResyncLDAP;

/**
 * Execution of the manual resyncLDAP command
 *
 * @author Marta
 * @since 2021-05-28
 */
public class ManualResyncLDAP extends Request {
	private static final long serialVersionUID = -8097151852196189205L;

	private String logOutput;

	public ManualResyncLDAP() {
		logOutput = "";
	}

	@Override
	public void run() {
		if (getEffectiveRequester().canBecome("admin"))
			logOutput = ResyncLDAP.manualResyncLDAP();
		else
			logOutput = "Only users with role admin can execute this call";
	}

	/**
	 * Gets output from the executed command
	 *
	 * @return
	 */
	public String getLogOutput() {
		return this.logOutput;
	}

	/**
	 * Gets the recorded log in the db
	 *
	 * @param classname
	 * @return
	 */
	public static String getLastLogFromDB(String classname) {
		return DBSyncUtils.getLastLog(classname, false, true);
	}

	@Override
	public String toString() {
		return "Asked for a manual resyncLDAP";
	}

	@Override
	public List<String> getArguments() {
		return null;
	}
}
