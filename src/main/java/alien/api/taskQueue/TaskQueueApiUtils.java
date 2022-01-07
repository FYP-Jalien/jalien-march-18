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
import alien.user.AliEnPrincipal;

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
		}
		catch (final ServerException e) {
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
	public List<Job> getPS(final Collection<JobStatus> states, final Collection<String> users, final Collection<String> sites, final Collection<String> nodes, final Collection<Long> mjobs,
			final Collection<Long> jobid, final String orderByKey, final int limit) {

		try {
			final GetPS ps = Dispatcher.execute(new GetPS(commander.getUser(), states, users, sites, nodes, mjobs, jobid, orderByKey, limit));

			return ps.returnPS();
		}
		catch (final ServerException e) {
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

		}
		catch (final ServerException e) {
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
		}
		catch (final ServerException e) {
			System.out.println("Could get not a TraceLog: ");
			e.getCause().printStackTrace();
		}
		return null;

	}

	/**
	 * @param queueId
	 * @param originalJDL whether to return the original JDL (<code>true</code>) or the resulting JDL (<code>false</code>)
	 * @return a JDL as String
	 */
	public String getJDL(final long queueId, final boolean originalJDL) {

		try {
			final GetJDL jdl = Dispatcher.execute(new GetJDL(commander.getUser(), queueId, originalJDL));

			return jdl.getJDL();
		}
		catch (final ServerException e) {
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
		}
		catch (final ServerException e) {
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
		}
		catch (final ServerException e) {
			System.out.println("Could get not the Jobs: " + e.getMessage());
			e.getCause().printStackTrace();
		}
		return null;

	}

	/**
	 * @param jobId
	 * @return a CE listing
	 */
	public HashMap<CE, Object> getMatchingCEs(final long jobId) {

		try {
			final GetMatchingCEs matchingCEs = Dispatcher.execute(new GetMatchingCEs(commander.getUser(), jobId));

			return matchingCEs.getMatchingCEs();

		}
		catch (final ServerException e) {
			System.out.println("Could not get a matching CEs listing: ");
			e.getCause().printStackTrace();
		}
		return null;

	}

	/**
	 * @param status Status to set the CE
	 * @param ceNames CEs to set the status to
	 *
	 * @return a list with the successfully updated CEs
	 * @throws SecurityException
	 */
	public List<String> setCEStatus(final String status, final List<String> ceNames) throws SecurityException {

		try {
			final CEStatusSetter statusSetter = Dispatcher.execute(new CEStatusSetter(commander.getUser(), status, ceNames));
			return statusSetter.getUpdatedCEs();

		}
		catch (final ServerException e) {
			System.out.println("Could not update status ");
			e.getCause().printStackTrace();
		}
		return null;

	}

	/**
	 * Set a job's status
	 *
	 * @param jobnumber
	 * @param resubmission 
	 * @param status
	 */
	public static void setJobStatus(final long jobnumber, final int resubmission, final JobStatus status) {
		try {
			Dispatcher.execute(new SetJobStatus(jobnumber, resubmission, status));
		}
		catch (final ServerException e) {
			System.out.println("Could get not a Job's status: " + e.getMessage());
			e.getCause().printStackTrace();
		}
	}

	/**
	 * Set a job's status and sets extra fields on the DB
	 *
	 * @param jobnumber
	 * @param resubmission
	 * @param status
	 * @param extrafields
	 * @return <code>false</code> if the job is not supposed to be running any more
	 */
	public static boolean setJobStatus(final long jobnumber, final int resubmission, final JobStatus status, final HashMap<String, Object> extrafields) {
		try {
			Dispatcher.execute(new SetJobStatus(jobnumber, resubmission, status, extrafields));
		}
		catch (final ServerException e) {
			System.out.println("Could get not a Job's status: " + e.getMessage());
			e.getCause().printStackTrace();

			if (e instanceof JobKilledException)
				return false;
		}

		return true;
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

			final KillJob answer = Dispatcher.execute(j);
			return answer.wasKilled();
		}
		catch (final Exception e) {
			System.out.println("Could not kill the job  with id: [" + queueId + "]");
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * @param user Account to get the quota information for
	 * @return jobs quota for the current user
	 */
	public static Quota getJobsQuota(final AliEnPrincipal user) {
		try {
			final GetQuota gq = new GetQuota(user);
			final GetQuota gqres = Dispatcher.execute(gq);
			return gqres.getQuota();
		}
		catch (final Exception e) {
			System.out.println("Exception in GetQuota: " + e.getMessage());
		}
		return null;
	}

	/**
	 * @return file quota for the current user
	 */
	public FileQuota getFileQuota() {
		return getFileQuota(commander.getUser());
	}

	/**
	 * @param user requested user information
	 * @return file quota for the indicated user
	 */
	public static FileQuota getFileQuota(final AliEnPrincipal user) {
		try {
			final GetFileQuota gq = new GetFileQuota(user);
			final GetFileQuota gqres = Dispatcher.execute(gq);

			return gqres.getFileQuota();
		}
		catch (final Exception e) {
			System.out.println("Exception in getFileQuota: " + e.getMessage());
		}
		return null;
	}

	/**
	 * @param jobid
	 * @param resubmission 
	 * @param tag
	 * @param message
	 * @return <code>false</code> if the job was killed
	 */
	@SuppressWarnings("static-method")
	public boolean putJobLog(final long jobid, final int resubmission, final String tag, final String message) {
		try {
			final PutJobLog sq = new PutJobLog(jobid, resubmission, tag, message);
			Dispatcher.execute(sq);
		}
		catch (final Exception e) {
			System.out.println("Exception in putJobLog: " + e.getMessage());

			if (e instanceof JobKilledException)
				return false;
		}

		return true;
	}

	/**
	 * @param user
	 * @param fld
	 * @param val
	 * @return <code>true</code> if the operation was successful
	 */
	public static boolean setFileQuota(final AliEnPrincipal user, final String fld, final String val) {
		try {
			final SetFileQuota sq = new SetFileQuota(user, fld, val);
			final SetFileQuota sqres = Dispatcher.execute(sq);
			return sqres.getSucceeded();
		}
		catch (final Exception e) {
			System.out.println("Exception in setFileQuota: " + e.getMessage());
		}
		return false;
	}

	/**
	 * @param user User for whom to set the new value
	 * @param fld
	 * @param val
	 * @return <code>true</code> if the operation was successful
	 */
	public static boolean setJobsQuota(final AliEnPrincipal user, final String fld, final String val) {
		try {
			final SetJobsQuota sq = new SetJobsQuota(user, fld, val);
			final SetJobsQuota sqres = Dispatcher.execute(sq);
			return sqres.getSucceeded();
		}
		catch (final Exception e) {
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
		}
		catch (final Exception e) {
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
		}
		catch (final ServerException e) {
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
		}
		catch (final ServerException e) {
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
		}
		catch (final ServerException e) {
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
	public ResubmitJob resubmitJob(final long queueId) {
		try {
			final ResubmitJob j = Dispatcher.execute(new ResubmitJob(commander.getUser(), queueId));
			return j;
		}
		catch (final Exception e) {
			System.out.println("Could not resubmit the job  with id: [" + queueId + "]");
			e.printStackTrace();
		}
		return null;
	}

	/**
	 *
	 * Submit resultsJdl for given queueId
	 *
	 * @param jdl
	 * @param queueId
	 * @return <code>AddResultsJDL</code> from which the database update status can be retrieved
	 */
	public static AddResultsJDL addResultsJdl(final JDL jdl, final long queueId) {
		try {
			final AddResultsJDL arj = Dispatcher.execute(new AddResultsJDL(jdl, queueId));
			return arj;
		}
		catch (final Exception e) {
			System.out.println("Could not add the results JDL for job : " + e.getMessage());
			e.printStackTrace();
		}
		return null;
	}
}
