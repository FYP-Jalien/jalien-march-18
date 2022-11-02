package alien.site;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Vector;
import java.util.Comparator;
import java.util.Map;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import alien.config.ConfigUtils;
import alien.taskQueue.TaskQueueUtils;
import apmon.MonitoredJob;

public class NUMAExplorer {

	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(TaskQueueUtils.class.getCanonicalName());

	// Updated job assignment
	HashMap<Integer, Integer> availablePerNode;
	HashMap<Integer, long[]> structurePerNode;
	HashMap<Integer, Long> jobNumberToQueueId;
	HashMap<Integer, Integer> jobToNuma;
	static HashMap<Integer, long[]> JAToMask;
	static int[] usedCPUs;

	// Initial host structure
	int numCPUs;
	HashMap<Integer, Integer> initialAvailablePerNode;
	HashMap<Integer, long[]> initialStructurePerNode;
	HashMap<Integer, Integer> coresPerNode;
	HashMap<Integer, Integer> divisionedNUMA;
	HashMap<Integer, Long> coresPerJob;

	public NUMAExplorer(int numCPUs) {
		this.numCPUs = numCPUs;
		availablePerNode = new HashMap<>();
		initialAvailablePerNode = new HashMap<>();
		structurePerNode = new HashMap<>();
		initialStructurePerNode = new HashMap<>();
		coresPerNode = new HashMap<>();
		divisionedNUMA = new HashMap<>();
		jobNumberToQueueId = new HashMap<>();
		coresPerJob = new HashMap<>();
		JAToMask = new HashMap<>();
		jobToNuma = new HashMap<>();
		usedCPUs = new int[numCPUs];
		fillNumaTopology();
	}


	private void fillNumaTopology() {
		int subcounter = 0;
		String filename = "/sys/devices/system/node/";
		File numaDir = new File(filename);
		File[] numaNodes = numaDir.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.startsWith("node");
			}
		});
		if (numaNodes != null) {
			for (File node : numaNodes) {
				if (node.isDirectory()) {
					filename = node.getAbsolutePath() + "/cpulist";
					File f = new File(filename);
					String s;
					if (f.exists() && f.canRead()) {
						try (BufferedReader br = new BufferedReader(new FileReader(f))) {
							while ((s = br.readLine()) != null) {
								String[] splitted = s.split(",");
								for (String range : splitted) {
									long[] cpuRange = new long[numCPUs];
									String patternStr = "(\\d+)-(\\d+)";
									Pattern pattern = Pattern.compile(patternStr);
									Matcher matcher = pattern.matcher(range);
									if (matcher.matches()) {
										int rangeidx = Integer.parseInt(matcher.group(1));
										int maxRangeidx = Integer.parseInt(matcher.group(2));
										while (rangeidx <= maxRangeidx) {
											coresPerNode.put(Integer.valueOf(rangeidx), Integer.valueOf(subcounter));
											cpuRange[rangeidx] = 1;
											rangeidx += 1;
										}

										String patternNumaStr = ".*(\\d+).*";
										Pattern patternNuma = Pattern.compile(patternNumaStr);
										Matcher matcherNuma = patternNuma.matcher(node.getName());
										if (matcherNuma.matches()) {
											int numaId = Integer.parseInt(matcherNuma.group(1));
											divisionedNUMA.put(Integer.valueOf(subcounter), Integer.valueOf(numaId));
											structurePerNode.put(Integer.valueOf(subcounter), cpuRange);
											initialStructurePerNode.put(Integer.valueOf(subcounter), cpuRange.clone());
											logger.log(Level.INFO, "Feeling initial structure of sub-node counter " + subcounter + " and mask " + getMaskString(initialStructurePerNode.get(subcounter)));
											int coreCount = countAvailableCores(cpuRange);
											availablePerNode.put(Integer.valueOf(subcounter), Integer.valueOf(coreCount));
											initialAvailablePerNode.put(Integer.valueOf(subcounter), Integer.valueOf(coreCount));
											subcounter = subcounter + 1;
										}
										else
											logger.log(Level.INFO, "Format error found when getting NUMA node id");
									}
									else
										logger.log(Level.INFO, "Format error on getting NUMA range");
								}
							}
						}
						catch (IOException e) {
							logger.log(Level.WARNING, "Could not access " + filename + " " + e);
						}
					}
				}
			}
		}
	}

	private static int countAvailableCores(long[] cpuRange) {
		int counter = 0;
		for (int i = 0; i < cpuRange.length; i++) {
			if (cpuRange[i] == 1)
				counter = counter + 1;
		}
		return counter;
	}

	private static String getMaskString(long[] cpuRange) {
		// Aux printing for debugging purposes
		String rangeStr = "";
		for (int i = 0; i < cpuRange.length; i++) {
			rangeStr = rangeStr + cpuRange[i] + " ";
		}
		return rangeStr;
	}

	private static String getMaskString(int[] cpuRange) {
		// Aux printing for debugging purposes
		String rangeStr = "";
		for (int i = 0; i < cpuRange.length; i++) {
			rangeStr = rangeStr + cpuRange[i] + " ";
		}
		return rangeStr;
	}

	String pickCPUs(long[] mask, Long reqCPU, long queueId, int jobNumber) {
		jobNumberToQueueId.put(Integer.valueOf(jobNumber), Long.valueOf(queueId));
		long[] newMask = new long[numCPUs];
		long[] finalMask = new long[numCPUs];

		boolean rearrangementNeeded = true;
		int rearrangementCount = 0;
		while (rearrangementNeeded) {
			for (int i = 0; i < numCPUs; i++) {
				if (mask[i] != 1 && usedCPUs[i] == 0) {
					newMask[i] = 1;
				}
			}
			int numaNode = getNumaNode(reqCPU, queueId, null, availablePerNode);

			// We have not found the space needed in any node. Proceed to partition
			if (numaNode == -1) {
				if (rearrangementCount == 0) {
					int[] auxUsedCPUs = rearrangeCores(jobNumber);
					if (!Arrays.equals(usedCPUs, auxUsedCPUs)) {
						rearrangementCount = rearrangementCount + 1;
						restartStructures(auxUsedCPUs);
						for (int i = 0; i < auxUsedCPUs.length; i++)
							usedCPUs[i] = auxUsedCPUs[i];
						continue;
					}
				}
				finalMask = getPartitionedMask(reqCPU, jobNumber, structurePerNode, availablePerNode, usedCPUs);
				jobToNuma.put(Integer.valueOf(jobNumber), Integer.valueOf(-1));
			}
			else {
				long[] availableMask = structurePerNode.get(Integer.valueOf(numaNode));
				finalMask = buildFinalMask(availableMask, reqCPU, jobNumber, availablePerNode, structurePerNode, usedCPUs);
				rearrangementNeeded = false;
				jobToNuma.put(Integer.valueOf(jobNumber), Integer.valueOf(numaNode));
			}

			logger.log(Level.INFO, "Process is going to be pinned to CPU mask " + getMaskString(finalMask));

			if (rearrangementCount > 0)
				rearrangementNeeded = false;
		}
		logger.log(Level.INFO, "Current CPU-job mapping: " + getMaskString(usedCPUs));
		JAToMask.put(Integer.valueOf(jobNumber), finalMask);
		coresPerJob.put(Integer.valueOf(jobNumber), reqCPU);
		return arrayToTaskset(finalMask);
	}

	private int getNumaNode(Long reqCPU, long queueId, Integer previousNuma, HashMap<Integer, Integer> available) {
		int numaNode;
		if (previousNuma == null || previousNuma.intValue() == -1)
			numaNode = (int) (queueId % availablePerNode.keySet().size());
		else
			numaNode = previousNuma.intValue();
		int nodeCount = 0;
		while (nodeCount < available.keySet().size()) {
			if (available.get(Integer.valueOf(numaNode)).intValue() < reqCPU.intValue())
				numaNode = numaNode + 1;
			else
				break;
			nodeCount = nodeCount + 1;
			if (numaNode == available.keySet().size())
				numaNode = 0;
		}
		if (nodeCount == available.keySet().size())
			numaNode = -1;
		return numaNode;
	}

	private void changePinningConfig(int[] auxUsedCPUs) {
		HashMap<Integer, long[]> masksToPin = new HashMap<>();
		for (int i = 0; i < numCPUs; i++) {
			int jobNum = auxUsedCPUs[i];
			if (jobNum != 0) {
				long[] initMask = masksToPin.get(Integer.valueOf(jobNum));
				if (initMask == null)
					initMask = new long[numCPUs];
				initMask[i] = 1;
				masksToPin.put(Integer.valueOf(auxUsedCPUs[i]), initMask);
			}
		}
		for (Integer jobId : masksToPin.keySet()) {
			if (!Arrays.equals(JAToMask.get(jobId), masksToPin.get(jobId))) {
				if (JobAgent.JAToPid.get(jobId) != null) {
					applyTaskset(arrayToTaskset(masksToPin.get(jobId)), JobAgent.JAToPid.get(jobId).intValue());
					JAToMask.put(jobId, masksToPin.get(jobId).clone());
					logger.log(Level.INFO, "Modifying pinning configuration of job ID " + jobId + ". New mask " + getMaskString(JAToMask.get(jobId)));
				}
			}
		}
	}

	public static void applyTaskset(String isolCmd, int pidToConstrain) {

		Vector<Integer> children = MonitoredJob.getChildrenProcessIDs(pidToConstrain);

		if (isolCmd != null && isolCmd.compareTo("") != 0) {
			for (Integer pid : children) {
				logger.log(Level.INFO, "Constraining PID " + pid);
				try {
					final Process CPUConstrainer = Runtime.getRuntime().exec("taskset -a -cp " + isolCmd + " " + pid);
					CPUConstrainer.waitFor();

				}
				catch (final Exception e) {
					logger.log(Level.WARNING, "Could not apply CPU mask " + e);
				}
			}
		}
	}

	private void restartStructures(int[] auxUsedCPUs) {
		logger.log(Level.INFO, "NUMAExplorer reconfiguring after job rescheduling.");
		changePinningConfig(auxUsedCPUs);

		for (Integer node : initialStructurePerNode.keySet()) {
			long[] nodeMask = initialStructurePerNode.get(node).clone();
			int available = initialAvailablePerNode.get(node).intValue();
			for (int idxMask = 0; idxMask < numCPUs; idxMask++) {
				if (nodeMask[idxMask] == 1 && auxUsedCPUs[idxMask] != 0) {
					nodeMask[idxMask] = 0;
					available = available - 1;
				}
			}
			availablePerNode.put(node, Integer.valueOf(available));
			structurePerNode.put(node, nodeMask);
		}
	}

	private long[] getPartitionedMask(Long reqCPU, int jobNumber, HashMap<Integer, long[]> structure, HashMap<Integer, Integer> available, int[] auxUsedCPUs) {
		long[] finalMask;
		logger.log(Level.INFO, "Computing a partitioned mask for job " + jobNumber);
		HashMap<Integer, Long> freeOnNuma = new HashMap<>();
		for (int i = 0; i < available.keySet().size(); i++) {
			Integer absoluteNuma = divisionedNUMA.get(Integer.valueOf(i));
			int totalFree = 0;
			if (freeOnNuma.containsKey(absoluteNuma))
				totalFree = freeOnNuma.get(absoluteNuma).intValue();
			totalFree = totalFree + available.get(Integer.valueOf(i)).intValue();
			freeOnNuma.put(absoluteNuma, Long.valueOf(totalFree));
		}

		List<Map.Entry<Integer, Long>> freeList = getSortedList(freeOnNuma);

		Map.Entry<Integer, Long> freestEntry = freeList.get(0);
		Integer freestIdx = freestEntry.getKey();
		long[] availableMask = new long[numCPUs];
		if (freestEntry.getValue().intValue() >= reqCPU.intValue()) {
			for (int i = 0; i < divisionedNUMA.keySet().size(); i++) {
				if (divisionedNUMA.get(Integer.valueOf(i)) == freestIdx) {
					availableMask = addPinnedCores(structure.get(Integer.valueOf(i)), availableMask);
				}
			}
		}
		else {
			for (int i = 0; i < divisionedNUMA.keySet().size(); i++) {
				availableMask = addPinnedCores(structure.get(Integer.valueOf(i)), availableMask);
			}
		}
		finalMask = buildFinalMask(availableMask, reqCPU, jobNumber, available, structure, auxUsedCPUs);
		return finalMask;
	}

	public void refillAvailable(int jobNumber) {
		logger.log(Level.INFO, "Reconfiguring structures of NUMAExplorer. Taking out job " + jobNumber);
		for (int i = 0; i < numCPUs; i++) {
			if (usedCPUs[i] == jobNumber) {
				int numaNode = coresPerNode.get(Integer.valueOf(i)).intValue();
				int left = availablePerNode.get(Integer.valueOf(numaNode)).intValue() + 1;
				availablePerNode.put(Integer.valueOf(numaNode), Integer.valueOf(left));
				long[] availableMask = structurePerNode.get(Integer.valueOf(numaNode)).clone();
				availableMask[i] = 1;
				structurePerNode.put(Integer.valueOf(numaNode), availableMask);
				usedCPUs[i] = 0;
			}
		}
		jobToNuma.remove(Integer.valueOf(jobNumber));
		jobNumberToQueueId.remove(Integer.valueOf(jobNumber));
		coresPerJob.remove(Integer.valueOf(jobNumber));
	}

	private static List<Map.Entry<Integer, Long>> getSortedList(HashMap<Integer, Long> freeOnNuma) {
		List<Map.Entry<Integer, Long>> freeList = new LinkedList<>(freeOnNuma.entrySet());
		Collections.sort(freeList, new Comparator<Map.Entry<Integer, Long>>() {
			@Override
			public int compare(Map.Entry<Integer, Long> o1, Map.Entry<Integer, Long> o2) {
				return (o2.getValue().compareTo(o1.getValue()));
			}
		});
		return freeList;
	}

	private int[] rearrangeCores(int newJobNumber) {
		logger.log(Level.INFO, "Starting job CPU cores rearrangement");

		//Init structures of node structure
		int[] auxUsedCPUs = new int[numCPUs];
		HashMap<Integer, Integer> auxAvailablePerNode = new HashMap<>();
		for (Integer node : initialAvailablePerNode.keySet())
			auxAvailablePerNode.put(node, initialAvailablePerNode.get(node));
		HashMap<Integer, long[]> auxStructurePerNode = new HashMap<>();
		for (Integer node : initialStructurePerNode.keySet())
			auxStructurePerNode.put(node, initialStructurePerNode.get(node).clone());

		List<Map.Entry<Integer, Long>> jobCoreList = getSortedList(coresPerJob);
		for (Map.Entry<Integer, Long> entry : jobCoreList) {
			Long reqCPU = entry.getValue();
			int jobNumber = entry.getKey().intValue();
			long queueId = jobNumberToQueueId.get(entry.getKey()).longValue();
			Integer previousNuma = jobToNuma.get(Integer.valueOf(jobNumber));

			int numaNode = getNumaNode(reqCPU, queueId, previousNuma, auxAvailablePerNode);

			// We have not found the space needed in any node. Proceed to partition
			if (numaNode == -1) {
				getPartitionedMask(reqCPU, jobNumber, auxStructurePerNode, auxAvailablePerNode, auxUsedCPUs);
				jobToNuma.put(Integer.valueOf(jobNumber), Integer.valueOf(-1));
			}
			else {
				long[] availableMask = auxStructurePerNode.get(Integer.valueOf(numaNode));
				buildFinalMask(availableMask, reqCPU, jobNumber, auxAvailablePerNode, auxStructurePerNode, auxUsedCPUs);
				jobToNuma.put(Integer.valueOf(jobNumber), Integer.valueOf(numaNode));
			}
		}
		for (int i = 0; i < auxUsedCPUs.length; i++) {
			if (auxUsedCPUs[i] == newJobNumber)
				auxUsedCPUs[i] = 0;
		}
		logger.log(Level.INFO, "The rearrangment result was " + getMaskString(auxUsedCPUs));
		return auxUsedCPUs;
	}

	private long[] buildFinalMask(long[] availableMask, Long reqCPU, int jobNumber, HashMap<Integer, Integer> available, HashMap<Integer, long[]> structure, int[] auxUsedCPUs) {
		int remainingCPU = reqCPU.intValue();
		long[] finalMask = new long[numCPUs];
		for (int i = 0; i < numCPUs; i++) {
			if (availableMask[i] == 1) {
				finalMask[i] = 1;
				remainingCPU = remainingCPU - 1;
				auxUsedCPUs[i] = jobNumber;
				availableMask[i] = 0;
				int numaNode = coresPerNode.get(Integer.valueOf(i)).intValue();
				int left = available.get(Integer.valueOf(numaNode)).intValue() - 1;
				available.put(Integer.valueOf(numaNode), Integer.valueOf(left));
				structure.put(Integer.valueOf(numaNode), availableMask);
				if (remainingCPU == 0) {
					break;
				}
			}
		}
		return finalMask;
	}

	static String arrayToTaskset(long[] array) {
		String out = "";

		for (int i = (array.length - 1); i >= 0; i--) {
			if (array[i] == 1) {
				if (out.length() != 0)
					out += ",";
				out += i;
			}
		}

		return out;
	}

	public static int[] getUsedCPUs() {
		return usedCPUs;
	}

	private static long[] addPinnedCores(long[] toAdd, long[] current) {
		long[] finalMask = current.clone();
		for (int i = 0; i < toAdd.length; i++) {
			if (toAdd[i] == 1)
				finalMask[i] = 1;
		}
		return finalMask;
	}
}
