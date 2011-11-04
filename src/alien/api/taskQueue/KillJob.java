package alien.api.taskQueue;

import alien.api.Request;
import alien.taskQueue.TaskQueueUtils;
import alien.user.AliEnPrincipal;

/**
 * Get a JDL object
 * 
 * @author ron
 * @since Jun 05, 2011
 */
public class KillJob extends Request {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7349968366381661013L;
	
	
	private final int queueId;
	
	private boolean wasKilled = false;

	/**
	 * @param user 
	 * @param role 
	 * @param queueId
	 */
	public KillJob(final AliEnPrincipal user, final String role, final int queueId) {
		setRequestUser(user);
		setRoleRequest(role);
		this.queueId = queueId;
	}


	@Override
	public void run() {
		this.wasKilled = TaskQueueUtils.killJob(getEffectiveRequester(),getEffectiveRequesterRole(),queueId);
	}

	/**
	 * @return success of the kill
	 */
	public boolean wasKilled(){
		return this.wasKilled;
	}

	@Override
	public String toString() {
		return "Asked to kill job: " + this.queueId;
	}
}
