package alien.priority;

/**
 * @author Jørn-Are Flaten
 * @since 2023-12-04
 */
public class QueueProcessingDto {
	private final int userId;
	private double cost;
	private long cputime;

	/**
	 * @param userId
	 */
	public QueueProcessingDto(int userId) {
		this.userId = userId;
		this.cost = 0;
		this.cputime = 0;
	}

	/**
	 * @return User ID
	 */
	public int getUserId() {
		return userId;
	}

	/**
	 * @return accumulated cost
	 */
	public double getCost() {
		return cost;
	}

	/**
	 * @return accumulated CPU time
	 */
	public long getCputime() {
		return cputime;
	}

	/**
	 * @param jobCost
	 * @param cpuTime
	 */
	public void addAccounting(double jobCost, long cpuTime) {
		this.cost += jobCost;
		this.cputime += cpuTime;
	}

}
