package alien.api.taskQueue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import alien.api.Request;
import alien.taskQueue.JDL;
import alien.taskQueue.TaskQueueUtils;
import alien.user.AliEnPrincipal;

/**
 * Get a JDL object
 *
 * @author ron
 * @since Jun 05, 2011
 */
public class SubmitJob extends Request {

	/**
	 *
	 */
	private static final long serialVersionUID = 7349968366381661013L;

	private JDL jdl;
	private long jobID = 0;

	/**
	 * @param user
	 * @param jdl
	 */
	public SubmitJob(final AliEnPrincipal user, final JDL jdl) {
		setRequestUser(user);
		this.jdl = jdl;
	}

	@Override
	public List<String> getArguments() {
		if (jdl != null)
			return Arrays.asList(jdl.toString());

		return null;
	}

	@Override
	public void run() {
		try {
			jobID = TaskQueueUtils.submit(jdl, getEffectiveRequester());
		}
		catch (final IOException ioe) {
			throw new IllegalArgumentException(ioe.getMessage());
		}
		finally {
			jdl = null;
		}
	}

	/**
	 * @return jobID
	 */
	public long getJobID() {
		return this.jobID;
	}

	@Override
	public String toString() {
		return "Asked to submit JDL: " + this.jdl;
	}
}
