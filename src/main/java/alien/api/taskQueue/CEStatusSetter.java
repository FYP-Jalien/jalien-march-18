package alien.api.taskQueue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.api.Request;
import alien.config.ConfigUtils;
import alien.taskQueue.TaskQueueUtils;
import alien.user.AliEnPrincipal;
import lazyj.DBFunctions;

/**
 * Sets the status of a Computing Element in the database
 *
 * @author marta
 */
public class CEStatusSetter extends Request {

	/**
	 *
	 */
	private static final long serialVersionUID = -3575992501982425989L;

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(CEStatusSetter.class.getCanonicalName());

	private final String status;

	List<String> ceNames;

	ArrayList<String> updatedCEs = new ArrayList<>();

	/**
	 * @param user
	 * @param status
	 * @param ceNames
	 */
	public CEStatusSetter(final AliEnPrincipal user, final String status, final List<String> ceNames) {
		setRequestUser(user);
		this.status = status;
		this.ceNames = ceNames;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(String.valueOf(this.status));
	}

	@Override
	public void run() throws SecurityException {
		if (!getEffectiveRequester().canBecome("admin"))
			throw new SecurityException("Only administrators can do it");

		try (DBFunctions db = TaskQueueUtils.getQueueDB()) {
			if (db == null)
				return;
			db.query("SELECT site FROM SITEQUEUES;", false);

			while (db.moveNext()) {
				if (ceNames != null && ceNames.size() > 0) {
					for (final String ceName : ceNames) {
						if (db.gets("site").toUpperCase().contains(ceName.toUpperCase())) {
							updatedCEs.add("\'" + db.gets("site") + "\'");
							continue;
						}
					}
				}
			}
			final String ceList = String.join(",", updatedCEs);
			logger.log(Level.INFO, "Updating CEs " + ceList + " with status " + status);

			db.query("UPDATE SITEQUEUES SET blocked=\'" + status + "\' WHERE site IN (" + ceList + ")", false);
		}
	}

	/**
	 * @return successfully updated ces
	 */
	public List<String> getUpdatedCEs() {
		return this.updatedCEs;
	}

	@Override
	public String toString() {
		return "Setting the CEs " + String.join(",", ceNames) + " to status " + status;
	}
}
