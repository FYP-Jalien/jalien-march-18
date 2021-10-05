package alien.api.taskQueue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import alien.api.Request;
import alien.taskQueue.JobBroker;
import alien.user.AliEnPrincipal;

/**
 * Get a list of CE matching job requirements
 *
 * @author marta
 */
public class GetMatchingCEs extends Request {

	/**
	 *
	 */
	private static final long serialVersionUID = -3575992501982425989L;

	private HashMap<CE,Object> matchingCEs;

	private final long jobId;

	/**
	 * @param user
	 * @param jobId
	 */
	public GetMatchingCEs(final AliEnPrincipal user, final long jobId) {
		setRequestUser(user);
		this.jobId = jobId;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(String.valueOf(this.jobId));
	}

	@Override
	public void run() {
		this.matchingCEs = JobBroker.getMatchingCEs(jobId);
	}

	/**
	 * @return the list of CEs
	 */
	public HashMap<CE,Object> getMatchingCEs () {
		return this.matchingCEs;
	}

	@Override
	public String toString() {
		return "Asked for Job's " + jobId + " matching CEs. The reply is: " + this.matchingCEs.keySet();
	}
}
