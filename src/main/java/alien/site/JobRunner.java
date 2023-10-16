package alien.site;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.api.DispatchSSLClient;
import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import lazyj.commands.SystemCommand;

/**
 * @author sweisz
 * @since Mar 25, 2021
 */
public class JobRunner extends JobAgent {

	/**
	 * logger object
	 */
	private static final Logger logger = ConfigUtils.getLogger(JobRunner.class.getCanonicalName());

	/**
	 * ML monitor object
	 */
	static final Monitor monitor = MonitorFactory.getMonitor(JobRunner.class.getCanonicalName());

	static {
		monitor.addMonitoring("resource_status", (names, values) -> {
			names.add("totalcpu");
			values.add(JobAgent.MAX_CPU);

			names.add("availablecpu");
			values.add(JobAgent.RUNNING_CPU);

			names.add("allocatedcpu");
			values.add(Long.valueOf(JobAgent.MAX_CPU.longValue() - JobAgent.RUNNING_CPU.longValue()));

			names.add("runningja");
			values.add(Long.valueOf(JobAgent.RUNNING_JOBAGENTS));

			names.add("slotlength");
			values.add(Integer.valueOf(JobAgent.origTtl));

		});
	}

	@Override
	public void run() {
		long timestamp = System.currentTimeMillis() / 1000;
		final long ttlEnd = timestamp + JobAgent.origTtl;
		Thread jaThread;
		int i = 0;

		final int maxRetries = Integer.parseInt(System.getenv().getOrDefault("MAX_RETRIES", "2"));

		int jrPid = MonitorFactory.getSelfProcessID();

		createSelfCgroup(jrPid);

		while (timestamp < ttlEnd) {
			synchronized (JobAgent.requestSync) {
				try {
					if (checkParameters()) {
						logger.log(Level.INFO, "Spawned thread nr " + i);
						jaThread = new Thread(new JobAgent(), "JobAgent #" + i);
						jaThread.start();
						if (cpuIsolation == true)
							checkAndApplyIsolation(jrPid);
						monitor.sendParameter("state", "Waiting for JA to get a job");
						monitor.sendParameter("statenumeric", Long.valueOf(1));
						i++;
					}
					else {
						monitor.sendParameter("state", "All slots busy");
						monitor.sendParameter("statenumeric", Long.valueOf(3));
						logger.log(Level.INFO, "No new thread");
					}

					JobAgent.requestSync.wait(3 * 60 * 1000);

				}
				catch (final InterruptedException e) {
					logger.log(Level.WARNING, "JobRunner interrupted", e);
				}

				timestamp = System.currentTimeMillis() / 1000;

				monitor.incrementCounter("startedja");

				monitor.sendParameter("retries", Long.valueOf(JobAgent.retries.get()));

				monitor.sendParameter("remainingttl", Long.valueOf(ttlEnd - timestamp));

				if (JobAgent.retries.get() >= maxRetries) {
					monitor.sendParameter("state", "Last JA cannot get job");
					monitor.sendParameter("statenumeric", Long.valueOf(2));
					logger.log(Level.INFO, "JobRunner going to exit from lack of jobs");
					System.exit(0);
					//break;
				}
			}
		}
		System.out.println("JobRunner Exiting");
	}

	public static void main(final String[] args) {
		ConfigUtils.setApplicationName("JobRunner");
		DispatchSSLClient.setIdleTimeout(30000);
		ConfigUtils.switchToForkProcessLaunching();
		final JobRunner jr = new JobRunner();
		jr.run();
	}

	private boolean createSelfCgroup(int selfPid) {
		if (new File("/sys/fs/cgroup/cgroup.controllers").isFile()) {
			try {
				final String slotCgroup = SystemCommand.bash("echo /sys/fs/cgroup$(cat /proc/" + selfPid + "/cgroup | grep slot | cut -d \":\" -f 3)").stdout;

				File runnerSubCgroup = new File(slotCgroup + "/runner");
				runnerSubCgroup.mkdir();

				if (!runnerSubCgroup.exists())
					return false;

				String procsToMove = Files.readString(Paths.get(slotCgroup + "/cgroup.procs"));
				Arrays.stream(procsToMove.split("\\r?\\n")).forEach(line -> SystemCommand.bash("echo " + line + " >> " + runnerSubCgroup.getPath() + "/cgroup.procs"));

				SystemCommand.bash("echo +memory >> " + slotCgroup + "/cgroup.subtree_control");

				return true;
			}
			catch (final Exception e) {
				return false;
			}
		}
		else
			return false;
	}
}
