package alien.api.taskQueue;

import java.io.Serializable;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import alien.api.Cacheable;
import alien.api.Request;
import alien.taskQueue.JobStatus;
import alien.taskQueue.TaskQueueUtils;

/**
 * Get the "uptime" or "w" statistics
 *
 * @author costing
 * @since Nov 4, 2011
 */
public class GetUptime extends Request implements Cacheable {
	/**
	 *
	 */
	private static final long serialVersionUID = 766147136470300455L;

	/**
	 * Statistics for one user
	 *
	 * @author costing
	 */
	public static final class UserStats implements Serializable {
		/**
		 *
		 */
		private static final long serialVersionUID = 7614077240613964647L;

		/**
		 * Currently active jobs for this user
		 */
		public int runningJobs = 0;

		/**
		 * Currently waiting jobs for this user
		 */
		public int waitingJobs = 0;

		/**
		 * Sum up the values (for computing totals)
		 *
		 * @param other
		 */
		public void add(final UserStats other) {
			this.runningJobs += other.runningJobs;
			this.waitingJobs += other.waitingJobs;
		}
	}

	private Map<String, UserStats> stats = null;

	/**
	 */
	public GetUptime() {
		// nothing
	}

	@Override
	public List<String> getArguments() {
		return null;
	}

	private static EnumSet<JobStatus> activeJobsSet = EnumSet.range(JobStatus.ASSIGNED, JobStatus.SAVING);
	private static EnumSet<JobStatus> waitingJobsSet = EnumSet.range(JobStatus.INSERTING, JobStatus.WAITING);

	@Override
	public void run() {
		stats = new TreeMap<>();

		Map<String, Integer> jobs = TaskQueueUtils.getJobCounters(activeJobsSet);

		for (final Map.Entry<String, Integer> entry : jobs.entrySet()) {
			final UserStats u = new UserStats();
			u.runningJobs = entry.getValue().intValue();

			stats.put(entry.getKey(), u);
		}

		jobs = TaskQueueUtils.getJobCounters(waitingJobsSet);

		for (final Map.Entry<String, Integer> entry : jobs.entrySet()) {
			final String user = entry.getKey();

			UserStats u = stats.get(user);

			if (u == null) {
				u = new UserStats();
				stats.put(user, u);
			}

			u.waitingJobs = entry.getValue().intValue();
		}
	}

	/**
	 * @return a JDL
	 */
	public Map<String, UserStats> getStats() {
		return this.stats;
	}

	@Override
	public String toString() {
		return "Asked for uptime, answer is: " + this.stats;
	}

	@Override
	public String getKey() {
		return "uptime";
	}

	@Override
	public long getTimeout() {
		return 1000 * 60 * 1;
	}
}
