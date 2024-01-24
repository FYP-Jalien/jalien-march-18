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
    public static class JobCounter {
        private AtomicInteger waiting = new AtomicInteger(0);
        private AtomicInteger running = new AtomicInteger(0);
        private AtomicLong cputime = new AtomicLong(0);
        private AtomicDouble cost = new AtomicDouble(0);

        public JobCounter(AtomicInteger waiting, AtomicInteger running, AtomicLong cputime, AtomicDouble cost) {
            this.waiting = waiting;
            this.running = running;
            this.cputime = cputime;
            this.cost = cost;
        }

        public JobCounter() {
        }

        public void incWaiting() {
            waiting.incrementAndGet();
        }

        public void decWaiting() {
            waiting.decrementAndGet();
        }

        public void addWaiting(int n) {
            waiting.addAndGet(n);
        }

        public void incRunningAndDecWaiting(int n) {
            running.addAndGet(n);
            decWaiting();
        }

        // Decrease the number of running active cpu cores by n
        public void decRunning(int n) {
            running.addAndGet(-n);
        }

        public int getWaiting() {
            return waiting.get();
        }

        public int getRunning() {
            return running.get();
        }

        public long getCputime() {
            return cputime.get();
        }

        public double getCost() {
            return cost.get();
        }

        public void addCputime(long n) {
            cputime.addAndGet(n);
        }

        public void addCost(double n) {
            cost.addAndGet(n);
        }

        public void resetCounters() {
            waiting.getAndSet(0);
            running.getAndSet(0);
            cputime.getAndSet(0);
            cost.getAndSet(0);
        }

        public void subtractWaiting(int n) {
            this.waiting.addAndGet(-n);
        }

        public void subtractRunning(int n) {
            this.running.addAndGet(-n);
        }

        public void subtractCputime(long n) {
            this.cputime.addAndGet(-n);
        }

        public void subtractCost(double n) {
            this.cost.addAndGet(-n);
        }

        public void subtractValues(int waiting, int running, long cputime, double cost) {
            subtractWaiting(waiting);
            subtractRunning(running);
            subtractCputime(cputime);
            subtractCost(cost);
        }

        // Creating a deep copy of the registry with new atomic values to ensure that the snapshot has a
        // separate memory location from the global registry
        public static Map<Integer, JobCounter> getRegistrySnapshot() {
            Map<Integer, JobCounter> snapshot = new ConcurrentHashMap<>();
            for (Map.Entry<Integer, JobCounter> entry : registry.entrySet()) {
                snapshot.put(entry.getKey(), new JobCounter(
                        new AtomicInteger(entry.getValue().waiting.get()),
                        new AtomicInteger(entry.getValue().running.get()),
                        new AtomicLong(entry.getValue().cputime.get()),
                        new AtomicDouble(entry.getValue().cost.get())));
            }
            return snapshot;
        }

        // Global registry map
        private static final Map<Integer, JobCounter> registry = new ConcurrentHashMap<>();

        public static JobCounter getCounterForUser(Integer userId) {
            // Lazily initialize the counters for a user if they don't exist
            return registry.computeIfAbsent(userId, k -> new JobCounter());
        }

        public static Map<Integer, JobCounter> getRegistry() {
            return registry;
        }

        public static void resetUserCounters(Integer userId) {
            JobCounter counter = registry.get(userId);
            if (counter != null) {
                counter.resetCounters();
            }
        }
    }
}
