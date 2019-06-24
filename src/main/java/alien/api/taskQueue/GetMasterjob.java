package alien.api.taskQueue;

import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import alien.api.Request;
import alien.config.ConfigUtils;
import alien.taskQueue.Job;
import alien.taskQueue.JobStatus;
import alien.taskQueue.TaskQueueUtils;
import alien.user.AliEnPrincipal;

/**
 * Get a JDL object
 *
 * @author ron
 * @since Jun 05, 2011
 */
public class GetMasterjob extends Request {

	/**
	 *
	 */
	private static final long serialVersionUID = -2299330738741897654L;

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(TaskQueueUtils.class.getCanonicalName());

	/**
	 *
	 */
	private List<Job> subJobs = null;

	private Job masterJob = null;

	private final long jobId;

	private final Set<JobStatus> status;

	private final List<Long> id;

	private final List<String> site;

	/**
	 * @param user
	 * @param jobId
	 * @param status
	 * @param id
	 * @param site
	 */
	public GetMasterjob(final AliEnPrincipal user, final long jobId, final Set<JobStatus> status, final List<Long> id, final List<String> site) {
		setRequestUser(user);
		this.jobId = jobId;
		this.status = status;
		this.id = id;
		this.site = site;
	}

	@Override
	public void run() {

		System.out.println("running with GetMasterJob");

		subJobs = TaskQueueUtils.getMasterJobStat(jobId, status, id, site, 10000);

		System.out.println("got subjosb, in GetMasterJob");

		masterJob = TaskQueueUtils.getJob(jobId);

		System.out.println("done with GetMasterJob");

	}

	/**
	 *
	 * @return the masterjob
	 */
	public List<Job> subJobStatus() {
		return this.subJobs;
	}

	/**
	 *
	 * @return the masterjob
	 */
	public HashMap<Job, List<Job>> masterJobStatus() {

		final HashMap<Job, List<Job>> masterjobstatus = new HashMap<>();

		masterjobstatus.put(this.masterJob, this.subJobs);

		return masterjobstatus;
	}

	@Override
	public String toString() {
		return "Asked for Masterjob status :  reply is: " + this.masterJob;
	}
}
