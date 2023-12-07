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
