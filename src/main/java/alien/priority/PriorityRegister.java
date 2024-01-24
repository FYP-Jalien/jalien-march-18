package alien.priority;

import com.google.common.util.concurrent.AtomicDouble;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Jørn-Are Flaten
 * @since 2023-11-23
 */
public class PriorityRegister {
	/**
	 * @author Jørn-Are Flaten
	 * @since 2023-11-23
	 */
	public static class JobCounter {
		private AtomicInteger waiting;
		private AtomicInteger running;
		private AtomicLong cputime;
		private AtomicDouble cost;

		/**
		 * Default constructor
		 */
		public JobCounter() {
			waiting = new AtomicInteger(0);
			running = new AtomicInteger(0);
			cputime = new AtomicLong(0);
			cost = new AtomicDouble(0);
		}

		/**
		 * Copy constructor
		 * 
		 * @param other
		 */
		public JobCounter(final JobCounter other) {
			this.waiting = new AtomicInteger(other.getWaiting());
			this.running = new AtomicInteger(other.getRunning());
			this.cputime = new AtomicLong(other.getCputime());
			this.cost = new AtomicDouble(other.getCost());
		}

		/**
		 * Increment the number of waiting jobs for this user
		 */
		public void incWaiting() {
			waiting.incrementAndGet();
		}

		/**
		 * Decrement the number of waiting jobs for this user
		 */
		public void decWaiting() {
			waiting.decrementAndGet();
		}

		/**
		 * Bulk insert of new jobs for a user
		 * 
		 * @param n
		 */
		public void addWaiting(final int n) {
			waiting.addAndGet(n);
		}

		/**
		 * Assigning a job with a given number of CPU cores to a site
		 * 
		 * @param n
		 */
		public void incRunningAndDecWaiting(int n) {
			running.addAndGet(n);
			decWaiting();
		}

		/**
		 * Decrease the number of running active cpu cores by n
		 * 
		 * @param n
		 */
		public void decRunning(int n) {
			running.addAndGet(-n);
		}

		/**
		 * @return delta waiting jobs
		 */
		public int getWaiting() {
			return waiting.get();
		}

		/**
		 * @return delta running CPU cores
		 */
		public int getRunning() {
			return running.get();
		}

		/**
		 * @return delta CPU time
		 */
		public long getCputime() {
			return cputime.get();
		}

		/**
		 * @return delta cost
		 */
		public double getCost() {
			return cost.get();
		}

		/**
		 * Accounting data from the job about the consumed resources
		 * 
		 * @param n
		 */
		public void addCputime(long n) {
			cputime.addAndGet(n);
		}

		/**
		 * Accounting data from the job about the consumed resources
		 * 
		 * @param n
		 */
		public void addCost(double n) {
			cost.addAndGet(n);
		}

		/**
		 * After flushing the values to the database, apply the changes to the in-memory counters
		 * 
		 * @param other
		 */
		public void subtractValues(JobCounter other) {
			this.waiting.addAndGet(-other.waiting.intValue());
			this.running.addAndGet(-other.running.intValue());
			this.cputime.addAndGet(-other.getCputime());
			this.cost.addAndGet(-other.getCost());
		}

		/**
		 * Creating a deep copy of the registry with new atomic values to ensure that the snapshot has a separate memory location from the global registry
		 * 
		 * @return registry clone
		 */
		public static Map<Integer, JobCounter> getRegistrySnapshot() {
			Map<Integer, JobCounter> snapshot = new ConcurrentHashMap<>();
			for (Map.Entry<Integer, JobCounter> entry : registry.entrySet()) {
				snapshot.put(entry.getKey(), new JobCounter(entry.getValue()));
			}
			return snapshot;
		}

		// Global registry map
		private static final Map<Integer, JobCounter> registry = new ConcurrentHashMap<>();

		/**
		 * @param userId
		 * @return the object corresponding to the given userId
		 */
		public static JobCounter getCounterForUser(Integer userId) {
			// Lazily initialize the counters for a user if they don't exist
			return registry.computeIfAbsent(userId, k -> new JobCounter());
		}

		/**
		 * @return the pointer to the entire registry
		 */
		public static Map<Integer, JobCounter> getRegistry() {
			return registry;
		}
	}
}
