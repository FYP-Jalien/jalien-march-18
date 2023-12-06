package alien.site;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import alien.config.ConfigUtils;
import alien.monitoring.MonitorFactory;
import alien.shell.commands.JAliEnCOMMander;
import lia.util.process.ExternalProcess.ExitStatus;
import utils.ProcessWithTimeout;

import java.nio.file.Path;
import java.io.StringReader;
import java.nio.file.Files;

/**
 * @author marta
 */
public class MemoryController implements Runnable {

	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(MemoryController.class.getCanonicalName());

	final JAliEnCOMMander commander = JAliEnCOMMander.getInstance();

	protected static double memHardLimit;
	protected static double memswHardLimit;
	private boolean swapUsageAllowed;
	private static long slotCPUs;

	protected static String cgroupRootPath;
	protected static String cgroupId;

	static HashMap<Long, Double> memPastPerJob;
	static HashMap<Long, Double> memCurrentPerJob;
	static HashMap<Long, Double> derivativePerJob;

	static HashMap<Long, JobAgent> activeJAInstances; // This variable is repeated to NUMAExplorer

	private boolean cgroupsv2;

	protected static boolean preemptingJob;

	static Object lockMemoryController = new Object();

	protected static int preemptionRound;

	protected static boolean debugMemoryController = false;

	final static long SLOT_MEMORY_MARGIN = 50000; // Margin left to the slot before taking the preemption decision. 50MB * NCORES
	final static long SLOT_SWAP_MARGIN = 1000000; // Margin left to the machine swap before taking the preemption decision. 1GB
	final static long MACHINE_MEMORY_MARGIN = 1000000; // Margin left to the whole machine before taking the preemption decision. 1GB
	final static double DEFAULT_CGROUP_NOT_CONFIG = 9007199254740988d; // Default set to PAGE_COUNTER_MAX,which is LONG_MAX/PAGE_SIZE on 64-bit platform. In MB
	final static int EVALUATION_FREQUENCY = 5; // Seconds between iterations

	public MemoryController(long cpus) {
		cgroupRootPath = "";
		memPastPerJob = new HashMap<>();
		memCurrentPerJob = new HashMap<>();
		derivativePerJob = new HashMap<>();
		activeJAInstances = new HashMap<>();
		memHardLimit = 0;
		slotCPUs = cpus;
		swapUsageAllowed = true;
		preemptionRound = 0;
		registerCgroupPath();
		registerHTCondorPeriodicRemove();
		logger.log(Level.INFO, "Parsed global memory limit of " + memHardLimit);
	}

	/**
	 * HTCondor can kill our jobs through the defined periodic remove policies in the job classad or in the submitted macro
	 */
	private void registerHTCondorPeriodicRemove() {
		// Parse environment SYSTEM_PERIODIC_REMOVE

		// Parse job classad periodic_remove
		String jobClassad = (String) JobAgent.siteMap.get("CONDOR_JOB_CLASSAD_CONTENTS");
		if (jobClassad != null && !jobClassad.isBlank() && !jobClassad.isEmpty()) {
			String s;
			try (BufferedReader br = new BufferedReader(new StringReader(jobClassad))) {
				while ((s = br.readLine()) != null) {
					if (s.startsWith("JobMemoryLimit"))
						break;
				}
				if (s != null)
					logger.log(Level.INFO, "Parsed memory limit for periodic_remove in classad is " + s + " kB");
				if (s != null && !s.isBlank() && !s.isEmpty()) {
					double tmpMemHardLimit = Double.valueOf(s.split("=")[1]).doubleValue();
					if (memHardLimit == 0 || tmpMemHardLimit < memHardLimit)
						memHardLimit = tmpMemHardLimit;
					swapUsageAllowed = false;
				}
			}
			catch (final IOException | IllegalArgumentException e) {
				logger.log(Level.WARNING, "Found exception while processing job classad exploration ", e);
			}
		}
	}

	/**
	 * Register the cgroups path assigned to our slot and parses memory limits
	 */
	private void registerCgroupPath() {

		cgroupsv2 = CgroupUtils.haveCgroupsv2();
		cgroupId = parseCgroupsPath(cgroupsv2);
		if (!cgroupId.isEmpty()) {
			if (cgroupsv2) {
				cgroupRootPath = buildCgroupPath("/sys/fs/cgroup/", cgroupId);
				logger.log(Level.INFO, "Memory cgroupsv2 set to " + cgroupRootPath);
			}
			else {
				cgroupRootPath = buildCgroupPath("/sys/fs/cgroup/memory/", cgroupId);
				logger.log(Level.INFO, "Memory cgroupsv1 set to " + cgroupRootPath);
			}
		}
		try {
			if (!cgroupRootPath.isEmpty() && cgroupsv2 == false) {
				String memLimitContents = Files.readString(Path.of(cgroupRootPath + "/memory.stat"));
				try (BufferedReader br = new BufferedReader(new StringReader(memLimitContents))) {
					String s;
					while ((s = br.readLine()) != null) {
						if (s.startsWith("hierarchical_memory_limit")) {
							double tmpMemHardLimit = Double.parseDouble(s.substring(s.indexOf(" ") + 1)) / 1024;// Convert to KB
							if (memHardLimit == 0 || tmpMemHardLimit < memHardLimit)
								memHardLimit = tmpMemHardLimit;
							if (memHardLimit >= DEFAULT_CGROUP_NOT_CONFIG)
								memHardLimit = 0;
							logger.log(Level.INFO, "Registered new memory hard limit CGROUPS V1 of " + memHardLimit);
						}
						if (s.startsWith("hierarchical_memsw_limit")) {
							double tmpMemswHardLimit = Double.parseDouble(s.substring(s.indexOf(" ") + 1)) / 1024; // Convert to KB
							if (memswHardLimit == 0 || tmpMemswHardLimit < memswHardLimit)
								memswHardLimit = tmpMemswHardLimit;
							if (memswHardLimit >= DEFAULT_CGROUP_NOT_CONFIG)
								memswHardLimit = 0;
							logger.log(Level.INFO, "Registered new memsw (RAM+SWAP) hard limit CGROUPS V1 of " + memswHardLimit);
						}
					}
				}
			}
			else if (!cgroupRootPath.isEmpty() && cgroupsv2 == true) {
				double tmpMemHardLimit = getCgroupV2Limits("/memory.max");
				if (memHardLimit == 0 || tmpMemHardLimit < memHardLimit)
					memHardLimit = tmpMemHardLimit;
				tmpMemHardLimit = getCgroupV2Limits("/memory.swap.max");
				if (memswHardLimit == 0 || tmpMemHardLimit < memswHardLimit)
					memswHardLimit = tmpMemHardLimit;
			}
		}
		catch (final IOException | IllegalArgumentException e) {
			logger.log(Level.WARNING, "Found exception while processing cgroups exploration ", e);
		}
	}

	private static double getCgroupV2Limits(String constraint) throws IOException {
		double tmpMemHardLimit;
		String memLimitContents = Files.readString(Path.of(cgroupRootPath + constraint));
		try (BufferedReader brMax = new BufferedReader(new StringReader(memLimitContents))) {
			String s = brMax.readLine();
			// This case means there is no enforced limit
			if (!s.equals("max")) {
				tmpMemHardLimit = Double.parseDouble(s) / 1024; // Convert to KB
				logger.log(Level.INFO, "Setting cgroupsV2 hard limit (" + constraint + ") to " + tmpMemHardLimit + " kB");
			}
			else
				tmpMemHardLimit = 0;
		}
		return tmpMemHardLimit;

	}

	protected static String parseCgroupsPath(boolean usingCgroupsv2) {
		try {
			String cgroupContents = Files.readString(Path.of("/proc/" + MonitorFactory.getSelfProcessID() + "/cgroup"));
			try (BufferedReader brCgroup = new BufferedReader(new StringReader(cgroupContents))) {
				String s;
				if (usingCgroupsv2 == false) {
					while ((s = brCgroup.readLine()) != null) {
						s = s.substring(s.indexOf(":") + 1);
						int idx = s.indexOf(":");
						String cgroupController = s.substring(0, idx);
						if (cgroupController.equals("memory")) {
							cgroupId = s.substring(idx + 1);
							break;
						}
					}
				}
				else {
					while ((s = brCgroup.readLine()) != null) {
						s = s.substring(s.indexOf(":") + 1);
						int idx = s.indexOf(":");
						String cgroupController = s.substring(0, idx);
						if (!cgroupController.equals("freezer")) {
							cgroupId = s.substring(idx + 1);
							break;
						}
					}
				}
			}
		}
		catch (final IOException | IllegalArgumentException e) {
			logger.log(Level.WARNING, "Found exception while processing cgroups exploration ", e);
		}
		return cgroupId;
	}

	public static ArrayList<String> getCgroupProcessTree(String procsFile) {
		ArrayList<Integer> processPids = new ArrayList<>();
		try {
			Scanner scan = new Scanner (new File (procsFile));
			while (scan.hasNextInt()) {
				int procPid = scan.nextInt();
				processPids.add(Integer.valueOf(procPid));
			}
		} catch (IOException e) {
			logger.log(Level.WARNING, "Exception while processing " + procsFile,e);
		}
		ArrayList<String> processInfo = new ArrayList<>();
		HashMap<Integer,String> processCmd = getProcessesCmd(processPids);
		HashMap<Integer, Long> processPss = getProcessesPSS(processPids);
		LinkedHashMap<Integer, Long> sortedPSSMap = processPss.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
				.collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue,(oldValue, newValue) -> oldValue, LinkedHashMap::new));
		for (Integer pid : sortedPSSMap.keySet()) {
			if (processCmd.containsKey(pid))
				processInfo.add(sortedPSSMap.get(pid) + " MB --> " + String.join(" ", processCmd.get(pid)));
		}
		if (debugMemoryController) {
			logger.log(Level.INFO, "Cgroup processes sorted by memory : ");
			for (String p : processInfo) {
				logger.log(Level.INFO, p);
			}
		}
		return processInfo;
	}

	private static HashMap<Integer,String> getProcessesCmd(ArrayList<Integer> processPids) {
		HashMap<Integer,String> processCmd = new HashMap<>();
		try {
			ExitStatus exitStatus = ProcessWithTimeout.executeCommand(Arrays.asList("ps", "-eo", "pid,command", "--sort", "-rss"), false, true, 60, TimeUnit.SECONDS);
			String[] psOutput = exitStatus.getStdOut().split("\n");
			for (String pInfo : psOutput) {
				if (pInfo.contains("PID"))
					continue;
				String[] components = pInfo.trim().split("\\s+");
				if (components.length >= 2) {
					Integer pPID = Integer.valueOf(components[0]);
					if (processPids.contains(pPID)) {
						processCmd.put(pPID, String.join(" ", Arrays.copyOfRange(components, 1, components.length)));
					}
				}
			}
		}
		catch (final Exception e) {
			logger.log(Level.WARNING, "Could  notcreate process list: " + e);
		}
		return processCmd;
	}

	private static HashMap<Integer,Long> getProcessesPSS(ArrayList<Integer> processPids) {
		HashMap<Integer, Long> processPss = new HashMap<>();
		for (Integer child : processPids) {
			long pssKB = 0;
			try {
				final String content = Files.readString(Path.of("/proc/" + child + "/smaps"));
				try (BufferedReader br = new BufferedReader(new StringReader(content))) {
					String s;
					while ((s = br.readLine()) != null) {
						if (s.length() < 8)
							continue;
						final char c0 = s.charAt(0);
						if ((c0 == 'P' && s.startsWith("Pss:"))) {
							final int idxLast = s.lastIndexOf(' ');
							final int idxPrev = s.lastIndexOf(' ', idxLast - 1);
							if (idxPrev > 0 && idxLast > 0) {
								final long value = Long.parseLong(s.substring(idxPrev + 1, idxLast));
								pssKB += value;
							}
						}
					}
				}
				processPss.put(child, Long.valueOf(pssKB / 1024));
			}
			catch (@SuppressWarnings("unused") final IOException | IllegalArgumentException e) {
				// ignore
			}
		}
		return processPss;
	}

	/**
	 * Get root cgroup for the allocated slot
	 *
	 * @param prefix Cgroup prefix (will depend on cgroup version)
	 * @param cgroupPath Parsed from proc files
	 * @return Root cgroup for the slot
	 */
	private static String buildCgroupPath(String prefix, String cgroupPath) {
		// Getting slurm job root cgroups
		String rootCgroups = "";
		for (String del : cgroupPath.split("/")) {
			rootCgroups += "/";
			rootCgroups += del;
			if (del.startsWith("job_")) { // This is for SLURM cgroups
				break;
			}
		}
		rootCgroups = prefix + rootCgroups;
		return rootCgroups;
	}

	/**
	 * Checks memory consumption of the slot
	 */
	private void checkMemoryConsumption() {
		double slotMem = 0, slotMemsw = 0;
		if (!cgroupRootPath.isEmpty()) {
			try {
				String s;
				if (cgroupsv2 == false) {
					String[] memTypesSimpl = {"total_rss", "total_cache"};
					slotMem = computeSlotMemory(memTypesSimpl);
					if (debugMemoryController)
						logger.log(Level.INFO, "Current mem usage (total_rss + total_cache) is " + slotMem);
					File f = new File(cgroupRootPath + "/memory.memsw.usage_in_bytes");
					if (f.exists()) {
						String [] memTypesSwap = {"total_rss", "total_cache", "total_swap"};
						slotMemsw = computeSlotMemory(memTypesSwap);
						if (debugMemoryController)
							logger.log(Level.INFO, "Current memsw usage (total_rss + total_cache + swap) is " + slotMemsw);
					}
				}
				else {
					String memCurrentUsageContents = Files.readString(Path.of(cgroupRootPath + "/memory.current"));
					try (BufferedReader br = new BufferedReader(new StringReader(memCurrentUsageContents))) {
						if ((s = br.readLine()) != null)
							slotMem = Double.parseDouble(s) / 1024; // Convert to KB
						if (debugMemoryController)
							logger.log(Level.INFO, "Current RAM usage is " + slotMem + " kB");
					}
					String memswCurrentUsageContents = Files.readString(Path.of(cgroupRootPath + "/memory.swap.current"));
					try (BufferedReader br = new BufferedReader(new StringReader(memswCurrentUsageContents))) {
						if ((s = br.readLine()) != null)
							slotMemsw = Double.parseDouble(s) / 1024; // Convert to KB
						if (debugMemoryController)
							logger.log(Level.INFO, "Current swap usage is " + slotMemsw + " kB");
					}
				}
			}
			catch (final IOException | IllegalArgumentException e) {
				logger.log(Level.WARNING, "Found exception while checking cgroups consumption", e);
			}
		}
		for (JobAgent runningJA : activeJAInstances.values()) {
			Long queueId = Long.valueOf(runningJA.getQueueId());

			if (memCurrentPerJob.get(queueId) != null)
				memPastPerJob.put(queueId, memCurrentPerJob.get(queueId));
			memCurrentPerJob.put(queueId, runningJA.RES_VMEM);
			updateGrowthDerivative(runningJA, queueId);
			if (cgroupRootPath.isEmpty())
				slotMem += runningJA.RES_VMEM.doubleValue();
		}

		if (activeJAInstances.size() >= 1) {
			String approachingLimit = approachingSlotMemLimit(slotMem, slotMemsw);
			if (preemptingJob == false && !approachingLimit.isEmpty()) {
				if (debugMemoryController)
					logger.log(Level.INFO, "Detected approaching slot mem limit");
				double boundMem = 0;
				switch (approachingLimit) {
					case "hard limit on RSS":
						boundMem = memHardLimit / 1024;
						break;
					case "hard limit including swap":
						boundMem = memswHardLimit / 1024;
						break;
					case "machine free memory":
						boundMem = parseSystemMemFree() /1024;
						break;
					default:
						boundMem = 0;
						break;
				}
				JobRunner.recordHighestConsumer(slotMem, approachingLimit, boundMem);
			}
		}
	}

	private static void updateGrowthDerivative(JobAgent ja, Long queueId) {
		Double growthDerivativePast = derivativePerJob.get(queueId);
		if (growthDerivativePast == null) {
			growthDerivativePast = Double.valueOf(0d);
		}
		double growthCurrent = (memCurrentPerJob.getOrDefault(queueId, Double.valueOf(0)).doubleValue() - memPastPerJob.getOrDefault(queueId, Double.valueOf(0)).doubleValue());
		double normalizationFactor = 2000 * ja.cpuCores; // lets normalize by 2GB per core
		growthCurrent = growthCurrent / normalizationFactor;
		growthCurrent += growthDerivativePast.doubleValue()/3;
		derivativePerJob.put(queueId, Double.valueOf(growthCurrent));
	}

	private static double computeSlotMemory(String[] memTypes) {
		String s;
		double totalMemory = 0;
		List<String> memTypesList = Arrays.asList(memTypes);
		try {
			String memStatContents = Files.readString(Path.of(cgroupRootPath + "/memory.stat"));
			try (BufferedReader br = new BufferedReader(new StringReader(memStatContents))) {
				while ((s = br.readLine()) != null) {
					String component = s.substring(0, s.indexOf(" "));
					if (memTypesList.contains(component)) {
						totalMemory += Double.parseDouble(s.substring(s.indexOf(" ") + 1)) / 1024;
					}
				}
			}
		}catch (IOException e) {
			logger.log(Level.WARNING, "Could not process memory.stat to compute slot memory " + e);
			return 0;
		}
		return totalMemory;
	}

	/**
	 * Takes out a job from MemoryController registries because it is no longer running
	 *
	 * @param queueId of job to remove
	 */
	public static void removeJobFromRegistries(long queueId) {
		logger.log(Level.INFO, "Removing job " + queueId + " from MemoryController registries");
		activeJAInstances.remove(Long.valueOf(queueId));
		memCurrentPerJob.remove(Long.valueOf(queueId));
		memPastPerJob.remove(Long.valueOf(queueId));
	}

	/**
	 * Checks whether the total slot memory is approaching the configured limit
	 *
	 * @param slotMem RMEM
	 * @param slotMemsw VMEM (Including swap)
	 * @return Whether we are close to the configured memory limit
	 */
	private String approachingSlotMemLimit(double slotMem, double slotMemsw) {
		if (debugMemoryController)
			logger.log(Level.INFO, "Adding a margin of " + SLOT_MEMORY_MARGIN * slotCPUs + " kB to the slot memory bound");
		// Two different meanings for different versions. In V1 accounting for slotMem=ram, slotMemsw=ram+swap. In V2 slotMem=ram, slotMemsw=swap
		if (memHardLimit > 0) {
			if (debugMemoryController)
				logger.log(Level.INFO, "We have a mem Hard limit of " + memHardLimit + " kB and right now we have slotMem " + slotMem + " kB");
			double tmpMemHardLimit = memHardLimit;
			if (swapUsageAllowed && memswHardLimit == 0 && parseSystemSwapFree() > 0) {
				tmpMemHardLimit = memHardLimit + parseSystemSwapFree() - SLOT_SWAP_MARGIN;
				if (debugMemoryController)
					logger.log(Level.INFO, "Adding swap to the limit " + parseSystemSwapFree() + " kB (-1 GB margin). Now hard limit is " + tmpMemHardLimit + " kB");
			}
			if (tmpMemHardLimit <= slotMem + SLOT_MEMORY_MARGIN * slotCPUs) { // adding margin of 50MB per core
				if (debugMemoryController)
					logger.log(Level.INFO, "Still have " + (memHardLimit - slotMem) + " kB RAM left");
				return "hard limit on RSS";
			}
		}
		if (memswHardLimit > 0) {
			if (debugMemoryController)
				logger.log(Level.INFO, "We have a memsw Hard limit of " + memswHardLimit + " and right now we have slotMemsw " + slotMemsw + " kB");
			if (memswHardLimit <= slotMemsw + SLOT_MEMORY_MARGIN * slotCPUs) { // adding margin of 50MB per core
				if (cgroupsv2) {
					if (debugMemoryController)
						logger.log(Level.INFO, "Still have " + (memswHardLimit - slotMemsw) + " kB swap left");
						return "hard limit on swap";
				}
				if (debugMemoryController)
					logger.log(Level.INFO, "Still have " + (memswHardLimit - slotMemsw) + " kB RAM + swap left");
				return "hard limit on RAM + swap";
			}
		}
		if (memHardLimit == 0 && memswHardLimit == 0) {
			double machineMemFree = parseSystemMemFree();
			logger.log(Level.INFO, "We dont have a memHard limit. MachineMemfree= " + machineMemFree + " kB and right now we have slotMem " + slotMem + " kB");
			if (machineMemFree < MACHINE_MEMORY_MARGIN) // Leaving margin for the system
				return "machine free memory";
		}
		return "";
	}

	/**
	 * Parses /proc/meminfo to get free memory on the machine in kB
	 *
	 * @return Free memory of the machine
	 */
	public static double parseSystemMemFree() {
		double memFree = MonitorFactory.getApMonSender().getSystemParameter("mem_free").doubleValue();
		String memInfo;
		try {
			memInfo = Files.readString(Path.of("/proc/meminfo"));
			try (BufferedReader br = new BufferedReader(new StringReader(memInfo))) {
				String s;
				while ((s = br.readLine()) != null) {
					if (s.startsWith("MemAvailable")) {
						final StringTokenizer st = new StringTokenizer(s);
						st.nextToken();
						String memFreeStr = st.nextToken();
						memFree = Double.parseDouble(memFreeStr); // IN kB
						break;
					}
				}
			}
		}
		catch (final IOException | IllegalArgumentException e) {
			logger.log(Level.WARNING, "Found exception while parsing contents of /proc/meminfo", e);
		}
		return memFree;
	}
	/**
	 * Parses /proc/meminfo to get free swap on the machine in kB
	 *
	 * @return Free swap of the machine
	 */
	public static double parseSystemSwapFree() {
		double memFree = MonitorFactory.getApMonSender().getSystemParameter("mem_free").doubleValue();
		String memInfo;
		try {
			memInfo = Files.readString(Path.of("/proc/meminfo"));
			try (BufferedReader br = new BufferedReader(new StringReader(memInfo))) {
				String s;
				while ((s = br.readLine()) != null) {
					if (s.startsWith("SwapFree")) {
						final StringTokenizer st = new StringTokenizer(s);
						st.nextToken();
						String memFreeStr = st.nextToken();
						memFree = Double.parseDouble(memFreeStr); // IN kB
						break;
					}
				}
			}
		}
		catch (final IOException | IllegalArgumentException e) {
			logger.log(Level.WARNING, "Found exception while parsing contents of /proc/meminfo", e);
		}
		return memFree;
	}

	/**
	 * Parses /proc/uptime to get uptime of machine
	 *
	 * @return Uptime in seconds
	 */
	public static double parseUptime() {
		double uptime = 0;
		try {
			String uptimeFile = Files.readString(Path.of("/proc/uptime"));
			try (BufferedReader br = new BufferedReader(new StringReader(uptimeFile))) {
				String s;
				while ((s = br.readLine()) != null) {
					final StringTokenizer st = new StringTokenizer(s);
					uptime = Double.parseDouble(st.nextToken());// IN SEC
					break;
				}
			}
		}
		catch (final IOException | IllegalArgumentException e) {
			logger.log(Level.WARNING, "Found exception while parsing contents of /proc/meminfo", e);
		}
		return uptime;
	}

	@Override
	public void run() {
		while (true) {
			checkMemoryConsumption();
			try {
				Thread.sleep(EVALUATION_FREQUENCY * 1000);
			}
			catch (InterruptedException e) {
				logger.log(Level.SEVERE, "Detected issue running MemoryController thread ", e);
			}
		}
	}

}

/**
 * Sorter by current memory usage
 *
 * @author marta
 *
 */
class SorterByAbsoluteMemoryUsage implements Comparator<JobAgent> {
	static final Logger logger = ConfigUtils.getLogger(SorterByAbsoluteMemoryUsage.class.getCanonicalName());

	@Override
	public int compare(JobAgent ja1, JobAgent ja2) {
		if (MemoryController.debugMemoryController)
			logger.log(Level.INFO, "Comparing ja1 " + ja1.getQueueId() + " RES_VMEM " + ja1.RES_VMEM.doubleValue() + " and ja2 " + ja2.getQueueId() + " RES_VMEM " + ja2.RES_VMEM.doubleValue());
		return Double.compare(ja2.RES_VMEM.doubleValue(), ja1.RES_VMEM.doubleValue());
	}

}

/**
 * Sorter by relative memory usage to the requested memory in the JDL
 *
 * @author marta
 *
 */
class SorterByRelativeMemoryUsage implements Comparator<JobAgent> {
	static final Logger logger = ConfigUtils.getLogger(SorterByRelativeMemoryUsage.class.getCanonicalName());

	@Override
	public int compare(JobAgent ja1, JobAgent ja2) {
		if (MemoryController.debugMemoryController)
			logger.log(Level.INFO,
					"Comparing ja1 " + ja1.getQueueId() + " RES_VMEM " + ja1.RES_VMEM.doubleValue() + ", maxMemJDL " + ja1.jobMaxMemoryMB + " with div value "
							+ ja1.RES_VMEM.doubleValue() / ja1.jobMaxMemoryMB
							+ " and ja2 " + ja2.getQueueId() + " RES_VMEM " + ja2.RES_VMEM.doubleValue() + ", maxMemJDL " + ja2.jobMaxMemoryMB + " with div value "
							+ ja2.RES_VMEM.doubleValue() / ja2.jobMaxMemoryMB);
		return Double.compare(ja2.RES_VMEM.doubleValue() / ja2.jobMaxMemoryMB, ja1.RES_VMEM.doubleValue() / ja1.jobMaxMemoryMB);
	}

}

/**
 * Sorter by memory consumption growth over the last memory measurement
 *
 * @author marta
 *
 */
class SorterByTemporalGrowth implements Comparator<JobAgent> {
	static final Logger logger = ConfigUtils.getLogger(SorterByTemporalGrowth.class.getCanonicalName());

	@Override
	public int compare(JobAgent ja1, JobAgent ja2) {
		/*double memGrowthJA1 = MemoryController.memCurrentPerJob.getOrDefault(Long.valueOf(ja1.getQueueId()), Double.valueOf(0)).doubleValue()
				- MemoryController.memPastPerJob.getOrDefault(Long.valueOf(ja1.getQueueId()), Double.valueOf(0)).doubleValue();
		double memGrowthJA2 = MemoryController.memCurrentPerJob.getOrDefault(Long.valueOf(ja2.getQueueId()), Double.valueOf(0)).doubleValue()
				- MemoryController.memPastPerJob.getOrDefault(Long.valueOf(ja2.getQueueId()), Double.valueOf(0)).doubleValue();*/
		double memGrowthJA1 = MemoryController.derivativePerJob.getOrDefault(Long.valueOf(ja1.getQueueId()), Double.valueOf(0d)).doubleValue();
		double memGrowthJA2 = MemoryController.derivativePerJob.getOrDefault(Long.valueOf(ja2.getQueueId()), Double.valueOf(0d)).doubleValue();
		if (MemoryController.debugMemoryController)
			logger.log(Level.INFO, "Comparing ja1 " + ja1.getQueueId() + " RES_VMEM " + ja1.RES_VMEM.doubleValue() + ", memGrowth " + memGrowthJA1
					+ " and ja2 " + ja2.getQueueId() + " RES_VMEM " + ja2.RES_VMEM.doubleValue() + ", memGrowth " + memGrowthJA2);
		return Double.compare(memGrowthJA2, memGrowthJA1);
	}

}
