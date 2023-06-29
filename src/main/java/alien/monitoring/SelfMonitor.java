package alien.monitoring;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

import alien.user.UserFactory;

/**
 * @author costing
 * @since 2010-11-12
 */
public class SelfMonitor implements MonitoringObject {

	private static final Set<String> systemProperties;

	private static final double MB = 1024 * 1024d;

	private static final long systemStarted = System.currentTimeMillis();

	static {
		final Set<String> temp = new HashSet<>();

		temp.add("java.vm.vendor");
		temp.add("java.version");
		temp.add("user.dir");

		systemProperties = Collections.unmodifiableSet(temp);
	}

	@Override
	public void fillValues(final Vector<String> paramNames, final Vector<Object> paramValues) {
		for (final String s : systemProperties) {
			final String value = System.getProperty(s);

			if (value != null) {
				paramNames.add(s);
				paramValues.add(System.getProperty(s));
			}
		}

		paramNames.add("user.name");
		paramValues.add(UserFactory.getUserName());

		final int selfPid = MonitorFactory.getSelfProcessID();

		if (selfPid > 0) {
			paramNames.add("jvm_pid");
			paramValues.add(Integer.valueOf(selfPid));

			// TODO self CPU time monitoring
		}

		final Runtime r = Runtime.getRuntime();

		final long freeMemory = r.freeMemory();

		paramNames.add("jvm_free_memory");
		paramValues.add(Double.valueOf(freeMemory / MB));

		final long maxMemory = r.maxMemory();

		paramNames.add("jvm_max_memory");
		paramValues.add(Double.valueOf(maxMemory / MB));

		final long totalMemory = r.totalMemory();

		paramNames.add("jvm_total_memory");
		paramValues.add(Double.valueOf(totalMemory / MB));

		final long usedMemory = totalMemory - freeMemory;

		paramNames.add("jvm_used_memory");
		paramValues.add(Double.valueOf(usedMemory / MB));

		final long availableMemory = maxMemory - usedMemory;

		paramNames.add("jvm_available_memory");
		paramValues.add(Double.valueOf(availableMemory / MB));

		if (totalMemory > 0) {
			paramNames.add("jvm_memory_usage");
			paramValues.add(Double.valueOf(usedMemory * 100d / totalMemory));
		}

		 MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();

		final long initHeapMemory = memoryMXBean.getHeapMemoryUsage().getInit();
		paramNames.add("jvm_heap_init_memory");
		paramValues.add(Double.valueOf(initHeapMemory / MB));

		final long usedHeapMemory = memoryMXBean.getHeapMemoryUsage().getUsed();
		paramNames.add("jvm_heap_used_memory");
		paramValues.add(Double.valueOf(usedHeapMemory / MB));

		final long maxHeapMemory = memoryMXBean.getHeapMemoryUsage().getMax();
		paramNames.add("jvm_heap_max_memory");
		paramValues.add(Double.valueOf(maxHeapMemory / MB));

		final long committedHeapMemory = memoryMXBean.getHeapMemoryUsage().getCommitted();
		paramNames.add("jvm_heap_committed_memory");
		paramValues.add(Double.valueOf(committedHeapMemory / MB));

		paramNames.add("jvm_available_processors");
		paramValues.add(Integer.valueOf(r.availableProcessors()));

		paramNames.add("jvm_uptime");
		paramValues.add(Double.valueOf((System.currentTimeMillis() - systemStarted) / 1000d));

		paramNames.add("jvm_threads");
		paramValues.add(Double.valueOf(ManagementFactory.getThreadMXBean().getThreadCount()));

		paramNames.add("jvm_total_started_threads");
		paramValues.add(Double.valueOf(ManagementFactory.getThreadMXBean().getTotalStartedThreadCount()));
	}
}
