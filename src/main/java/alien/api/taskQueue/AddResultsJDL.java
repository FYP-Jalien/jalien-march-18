package alien.api.taskQueue;

import java.util.Arrays;
import java.util.List;

import alien.api.Request;
import alien.taskQueue.JDL;
import alien.taskQueue.TaskQueueUtils;

/**
 * Add a results JDL to DB
 *
 * @author mstoretv
 * @since Jul 15, 2019
 */
public class AddResultsJDL extends Request {
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	/**
	 *
	 */
	private JDL jdl;
	private final long queueId;

	private boolean successfulUpdate = false;

	/**
	 * @param jdl resulting JDL to set
	 * @param queueId job ID to associate it with
	 */
	public AddResultsJDL(final JDL jdl, final long queueId) {
		this.jdl = jdl;
		this.queueId = queueId;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(jdl != null ? jdl.toString() : null, String.valueOf(queueId));
	}

	@Override
	public void run() {
		successfulUpdate = TaskQueueUtils.addResultsJdl(this.jdl, Long.valueOf(this.queueId));

		this.jdl = null;
	}

	@Override
	public String toString() {
		return "Adding the following results JDL for queueID: [" + this.queueId + "] :" + jdl.toString();
	}

	/**
	 * @return <code>true</code> if the update was successful in the database
	 */
	public boolean wasSuccessfullyUpdated() {
		return successfulUpdate;
	}
}