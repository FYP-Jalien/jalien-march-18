package alien.api.taskQueue;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import alien.api.Request;
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
public class GetPS extends Request {

	/**
	 *
	 */
	private static final long serialVersionUID = -1486633687303580187L;

	/**
	 *
	 */
	private List<Job> jobs;

	private final Collection<JobStatus> states;

	private final Collection<String> users;

	private final Collection<String> sites;

	private final Collection<String> nodes;

	private final Collection<Long> mjobs;

	private final Collection<Long> jobid;

	private final HashMap<PsFilters, Collection<Object>> filters;

	private final String orderByKey;

	private int limit = 0;

	public enum PsFilters {
		mtime
	}

	/**
	 * @param user
	 * @param states
	 * @param users
	 * @param sites
	 * @param nodes
	 * @param mjobs
	 * @param jobid
	 * @param orderByKey
	 * @param limit
	 */
	public GetPS(final AliEnPrincipal user, final Collection<JobStatus> states, final Collection<String> users, final Collection<String> sites, final Collection<String> nodes,
				 final Collection<Long> mjobs, final Collection<Long> jobid, final HashMap<PsFilters, Collection<Object>> filters, final String orderByKey, final int limit) {
		setRequestUser(user);
		this.states = states;
		this.users = users;
		this.sites = sites;
		this.nodes = nodes;
		this.mjobs = mjobs;
		this.jobid = jobid;
		this.filters = filters;
		this.limit = limit;
		this.orderByKey = orderByKey;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(states != null ? states.toString() : null, users != null ? users.toString() : null, sites != null ? sites.toString() : null, nodes != null ? nodes.toString() : null,
				mjobs != null ? mjobs.toString() : null, jobid != null ? jobid.toString() : null, filters != null ? filters.toString() : null, String.valueOf(limit), orderByKey);
	}

	@Override
	public void run() {
		this.jobs = TaskQueueUtils.getPS(states, users, sites, nodes, mjobs, jobid, filters, orderByKey, limit);
	}

	/**
	 * @return a JDL
	 */
	public List<Job> returnPS() {
		return this.jobs;
	}

	@Override
	public String toString() {
		return "Asked for PS :  reply is: " + this.jobs;
	}
}
