package alien.priority;

import lazyj.DBFunctions;

/**
 * @author Jørn-Are Flaten
 * @since 2023-12-04
 */
class PriorityDto {
	public PriorityDto(final DBFunctions db) {
		this.userId = db.geti("userId");
		this.priority = db.getf("priority");
		this.maxParallelJobs = db.geti("maxparallelJobs");
		this.userload = db.getf("userload");
		this.maxTotalRunningTime = db.getl("maxTotalRunningTime");
		this.computedPriority = 1;
		this.totalRunningTimeLast24h = db.getl("totalRunningTimeLast24h");
		this.running = db.geti("running");
	}

	/**
	 * User id
	 */
	private int userId;

	/**
	 * User baseline priority
	 */
	private float priority;

	/**
	 * Maximum number of cpu cores that can be utilized simulataneously by a user
	 */
	private int maxParallelJobs;
	/**
	 * Current user load = runningJobs / maxParallelJobs
	 */
	private float userload;

	/**
	 * Max total cpu time that can be used by a user
	 */
	private long maxTotalRunningTime;

	/**
	 * Number of running jobs
	 */
	private int running;

	public int getRunning() {
		return running;
	}

	public void setRunning(int running) {
		this.running = running;
	}

	public float getUserload() {
		return userload;
	}

	public void setUserload(float userload) {
		this.userload = userload;
	}

	public long getMaxTotalRunningTime() {
		return maxTotalRunningTime;
	}

	public void setMaxTotalRunningTime(long maxTotalRunningTime) {
		this.maxTotalRunningTime = maxTotalRunningTime;
	}

	public int getUserId() {
		return userId;
	}

	public void setUserId(int userId) {
		this.userId = userId;
	}

	public float getPriority() {
		return priority;
	}

	public void setPriority(float priority) {
		this.priority = priority;
	}

	public int getMaxParallelJobs() {
		return maxParallelJobs;
	}

	public void setMaxParallelJobs(int maxParallelJobs) {
		this.maxParallelJobs = maxParallelJobs;
	}

	public float getComputedPriority() {
		return computedPriority;
	}

	public void setComputedPriority(float computedPriority) {
		this.computedPriority = computedPriority;
	}

	public float getMaxTotalCpuCost() {
		return maxTotalCpuCost;
	}

	public void setMaxTotalCpuCost(float maxTotalCpuCost) {
		this.maxTotalCpuCost = maxTotalCpuCost;
	}

	public long getTotalRunningTimeLast24h() {
		return totalRunningTimeLast24h;
	}

	public void setTotalRunningTimeLast24h(long totalRunningTimeLast24h) {
		this.totalRunningTimeLast24h = totalRunningTimeLast24h;
	}

	/**
	 * computed priority determines which user gets priority to run a job.
	 */
	private float computedPriority;

	/**
	 * ??
	 */
	private float maxTotalCpuCost;

	/**
	 * Total running time of all jobs of this user in the last 24 hours
	 */
	private long totalRunningTimeLast24h;
}
