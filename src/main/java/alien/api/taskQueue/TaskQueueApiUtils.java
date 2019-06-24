package alien.api.taskQueue;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import alien.api.Dispatcher;
import alien.api.ServerException;
import alien.quotas.FileQuota;
import alien.quotas.Quota;
import alien.shell.commands.JAliEnCOMMander;
import alien.taskQueue.JDL;
import alien.taskQueue.Job;
import alien.taskQueue.JobStatus;

/**
 * Get the JDL object
 *
 * @author ron
 * @since Jun 05, 2011
 */
public class TaskQueueApiUtils {

	private final JAliEnCOMMander commander;

	/**
	 * @param commander
	 */
	public TaskQueueApiUtils(final JAliEnCOMMander commander) {
		this.commander = commander;
	}

	/**
	 * @return the uptime / w statistics
	 */
	public static Map<String, GetUptime.UserStats> getUptime() {
		try {
			final GetUptime uptime = Dispatcher.execute(new GetUptime());

			return uptime.getStats();
		} catch (final ServerException e) {
			System.out.println("Could not get an uptime stats: " + e.getMessage());
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * @param states
	 * @param users
	 * @param sites
	 * @param nodes
	 * @param mjobs
	 * @param jobid
	 * @param orderByKey
	 * @param limit
	 * @return a PS listing
	 */
	public List<Job> getPS(final Collection<JobStatus> states, final Collection<String> users, final Collection<String> sites, final Collection<String> nodes, final Collection<Integer> mjobs,
			final Collection<Integer> jobid, final String orderByKey, final int limit) {

		try {
			final GetPS ps = Dispatcher.execute(new GetPS(commander.getUser(), states, users, sites, nodes, mjobs, jobid, orderByKey, limit));

			return ps.returnPS();
		} catch (final ServerException e) {
			System.out.println("Could not get a PS listing: " + e.getMessage());
			e.getCause().printStackTrace();
		}
		return null;
	}

	/**
	 * @param status
	 * @param id
	 * @param jobId
	 * @param site
	 * @return a PS listing
	 */
	public List<Job> getMasterJobStatus(final long jobId, final Set<JobStatus> status, final List<Long> id, final List<String> site) {

		try {
			final GetMasterjob mj = Dispatcher.execute(new GetMasterjob(commander.getUser(), jobId, status, id, site));

			// return mj.masterJobStatus();
			return mj.subJobStatus();

		} catch (final ServerException e) {
			System.out.println("Could get a PS listing: ");
			e.getCause().printStackTrace();
		}
		return null;

	}

	/**
	 * @param queueId
	 * @return a JDL as String
	 */
	public String getTraceLog(final long queueId) {

		try {
			final GetTraceLog trace = Dispatcher.execute(new GetTraceLog(commander.getUser(), queueId));

			return trace.getTraceLog();
		} catch (final ServerException e) {
			System.out.println("Could get not a TraceLog: ");
			e.getCause().printStackTrace();
		}
		return null;

	}

	/**
	 * @param queueId
	 * @return a JDL as String
	 */
	public String getJDL(final long queueId) {

		try {
			final GetJDL jdl = Dispatcher.execute(new GetJDL(commander.getUser(), queueId));

			return jdl.getJDL();
		} catch (final ServerException e) {
			System.out.println("Could get not a JDL: " + e.getMessage());
			e.getCause().printStackTrace();
		}
		return null;

	}

	/**
	 * @param queueId
	 * @return a Job
	 */
	public Job getJob(final long queueId) {

		try {
			final GetJob job = Dispatcher.execute(new GetJob(commander.getUser(), queueId));

			return job.getJob();
		} catch (final ServerException e) {
			System.out.println("Could get not the Job: " + e.getMessage());
			e.getCause().printStackTrace();
		}
		return null;

	}

	/**
	 * @param queueIds
	 * @return a Job
	 */
	public List<Job> getJobs(final List<Long> queueIds) {

		try {
			final GetJobs job = Dispatcher.execute(new GetJobs(commander.getUser(), queueIds));

			return job.getJobs();
		} catch (final ServerException e) {
			System.out.println("Could get not the Jobs: " + e.getMessage());
			e.getCause().printStackTrace();
		}
		return null;

	}

	/**
	 * Set a job's status
	 *
	 * @param jobnumber
	 * @param status
	 */
	public static void setJobStatus(final long jobnumber, final JobStatus status) {
		try {
			Dispatcher.execute(new SetJobStatus(jobnumber, status));

		} catch (final ServerException e) {
			System.out.println("Could get not a Job's status: " + e.getMessage());
			e.getCause().printStackTrace();
		}
	}

	/**
	 * Set a job's status and sets extra fields on the DB
	 *
	 * @param jobnumber
	 * @param status
	 * @param extrafields
	 */
	public static void setJobStatus(final long jobnumber, final JobStatus status, final HashMap<String, Object> extrafields) {
		try {
			Dispatcher.execute(new SetJobStatus(jobnumber, status, extrafields));

		} catch (final ServerException e) {
			System.out.println("Could get not a Job's status: " + e.getMessage());
			e.getCause().printStackTrace();
		}
	}

	/**
	 * Submit a job
	 *
	 * @param jdl
	 * @return queueId
	 * @throws ServerException
	 */
	public long submitJob(final JDL jdl) throws ServerException {

		// final JDL signedJDL = JobSigner.signJob(JAKeyStore.clientCert, "User.cert", JAKeyStore.pass,
		// commander.getUser().getName(), ojdl);

		final SubmitJob j = new SubmitJob(commander.getUser(), jdl);

		final SubmitJob response = Dispatcher.execute(j);

		return response.getJobID();

	}

	/**
	 * Kill a job
	 *
	 * @param queueId
	 *
	 * @return status of the kill
	 */
	public boolean killJob(final long queueId) {

		try {
			final KillJob j = new KillJob(commander.getUser(), queueId);

			Dispatcher.execute(j);
			return j.wasKilled();

		} catch (final Exception e) {
			System.out.println("Could not kill the job  with id: [" + queueId + "]");
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * @return jobs quota for the current user
	 */
	public Quota getJobsQuota() {
		try {
			final GetQuota gq = new GetQuota(commander.getUser());
			final GetQuota gqres = Dispatcher.execute(gq);
			return gqres.getQuota();
		} catch (final Exception e) {
			System.out.println("Exception in GetQuota: " + e.getMessage());
		}
		return null;
	}

	/**
	 * @return file quota for the current user
	 */
	public FileQuota getFileQuota() {
		try {
			final GetFileQuota gq = new GetFileQuota(commander.getUser());
			final GetFileQuota gqres = Dispatcher.execute(gq);

			return gqres.getFileQuota();
		} catch (final Exception e) {
			System.out.println("Exception in getFileQuota: " + e.getMessage());
		}
		return null;
	}

	/**
	 * @param jobid
	 * @param tag
	 * @param message
	 */
	@SuppressWarnings("static-method")
	public void putJobLog(final long jobid, final String tag, final String message) {
		try {
			final PutJobLog sq = new PutJobLog(jobid, tag, message);
			Dispatcher.execute(sq);
		} catch (final Exception e) {
			System.out.println("Exception in putJobLog: " + e.getMessage());
		}
		return;
	}

	/**
	 * @param fld
	 * @param val
	 * @return <code>true</code> if the operation was successful
	 */
	public boolean setFileQuota(final String fld, final String val) {
		try {
			final SetFileQuota sq = new SetFileQuota(commander.getUser(), fld, val);
			final SetFileQuota sqres = Dispatcher.execute(sq);
			return sqres.getSucceeded();
		} catch (final Exception e) {
			System.out.println("Exception in setFileQuota: " + e.getMessage());
		}
		return false;
	}

	/**
	 * @param fld
	 * @param val
	 * @return <code>true</code> if the operation was successful
	 */
	public boolean setJobsQuota(final String fld, final String val) {
		try {
			final SetJobsQuota sq = new SetJobsQuota(commander.getUser(), fld, val);
			final SetJobsQuota sqres = Dispatcher.execute(sq);
			return sqres.getSucceeded();
		} catch (final Exception e) {
			System.out.println("Exception in setFileQuota: " + e.getMessage());
		}
		return false;
	}

	/**
	 * @param group
	 * @return group members
	 */
	public Set<String> getGroupMembers(final String group) {
		try {
			final GetGroupMembers gm = new GetGroupMembers(commander.getUser(), group);
			final GetGroupMembers gmres = Dispatcher.execute(gm);
			return gmres.getMembers();
		} catch (final Exception e) {
			System.out.println("Exception in setFileQuota: " + e.getMessage());
		}
		return null;
	}

	/**
	 * @param matchRequest
	 * @return matching job
	 */
	public GetMatchJob getMatchJob(final HashMap<String, Object> matchRequest) {

		try {
			final GetMatchJob gmj = Dispatcher.execute(new GetMatchJob(commander.getUser(), matchRequest));
			return gmj;
		} catch (final ServerException e) {
			System.out.println("Could not get a match Job: " + e.getMessage());
			e.getCause().printStackTrace();
		}
		return null;
	}

	/**
	 * @param matchRequest
	 * @return number of matching jobs
	 */
	public GetNumberWaitingJobs getNumberWaitingForSite(final HashMap<String, Object> matchRequest) {

		try {
			final GetNumberWaitingJobs gmj = Dispatcher.execute(new GetNumberWaitingJobs(commander.getUser(), matchRequest));
			return gmj;
		} catch (final ServerException e) {
			System.out.println("Could not get number of matching jobs: " + e.getMessage());
			e.getCause().printStackTrace();
		}
		return null;
	}

	/**
	 * @param host
	 * @param port
	 * @param ceName
	 * @param version
	 * @return free slots
	 */
	public GetNumberFreeSlots getNumberFreeSlots(final String host, final int port, final String ceName, final String version) {

		try {
			final GetNumberFreeSlots gmj = Dispatcher.execute(new GetNumberFreeSlots(commander.getUser(), host, port, ceName, version));
			return gmj;
		} catch (final ServerException e) {
			System.out.println("Could get not free slots: " + e.getMessage());
			e.getCause().printStackTrace();
		}
		return null;
	}

	/**
	 * Resubmit a job
	 * 
	 * @param queueId
	 * @return true for success, false for failure
	 */
	public ResubmitJob resubmitJob(long queueId) {
		try {
			final ResubmitJob j = Dispatcher.execute(new ResubmitJob(commander.getUser(), queueId));
			return j;
		} catch (final Exception e) {
			System.out.println("Could not resubmit the job  with id: [" + queueId + "]");
			e.printStackTrace();
		}
		return null;
	}

}
