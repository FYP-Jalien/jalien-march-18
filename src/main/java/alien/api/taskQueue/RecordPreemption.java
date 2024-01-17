package alien.api.taskQueue;

import java.util.Arrays;
import java.util.List;

import alien.api.Request;
import alien.taskQueue.TaskQueueUtils;

/**
 * Record a job preemption for oom
 *
 * @author marta
 * @since Oct 13, 2023
 */
public class RecordPreemption extends Request {
	/**
	 *
	 */
	private static final long serialVersionUID = -6330031807464568555L;
	private final long queueId;
	private final long preemptionTs;
	private final long killingTs;
	private final double preemptionSlotMemory;
	private final double preemptionJobMemory;
	private final double preemptionSlotSwMemory;
	private final int numConcurrentJobs;
	private final int resubmissionCounter;
	private final String hostName;
	private final String siteName;
	private final double memoryPerCore;
	private final double growthDerivative;
	private final double timeElapsed;
	private final String username;
	private final int preemptionRound;
	private final long wouldPreempt;
	private final double memHardLimit;
	private final double memswHardLimit;
	private final String killedProcess;
	private final String cgroupPath;
	private final double killingSlotMemory;
	private final double killingSlotSwMemory;
	private boolean recordingSuccess;

	/**
	 * @param queueId
	 * @param preemptionTs
	 * @param killingTs
	 * @param preemptionSlotMemory
	 * @param preemptionJobMemory
	 * @param killedJobMemory
	 * @param numConcurrentJobs
	 * @param preemptionTechnique
	 * @param resubmissionCounter
	 * @param hostName
	 * @param siteName
	 * @param memoryPerCore
	 * @param growthDerivative
	 * @param timeElapsed
	 * @param username
	 * @param preemptionRound
	 * @param wouldPreempt
	 * @param memHardLimit
	 * @param memswHardLimit
	 * @param killingSlotMemory
	 * @param killingSlotSwMemory
	 */
	public RecordPreemption(final long queueId, final long preemptionTs, final long killingTs, final double preemptionSlotMemory, final double preemptionSlotSwMemory, final double preemptionJobMemory, final int numConcurrentJobs, final int resubmissionCounter, final String hostName, final String siteName, double memoryPerCore, double growthDerivative, double timeElapsed, String username, int preemptionRound, long wouldPreempt, double memHardLimit, double memswHardLimit, String killedProcess, String cgroupPath, double killingSlotMemory, double killingSlotSwMemory) {
		this.queueId = queueId;
		this.preemptionTs = preemptionTs;
		this.killingTs = killingTs;
		this.preemptionSlotMemory = preemptionSlotMemory;
		this.preemptionSlotSwMemory = preemptionSlotSwMemory;
		this.preemptionJobMemory = preemptionJobMemory;
		this.numConcurrentJobs = numConcurrentJobs;
		this.resubmissionCounter = resubmissionCounter;
		this.hostName = hostName;
		this.siteName = siteName;
		this.memoryPerCore = memoryPerCore;
		this.growthDerivative = growthDerivative;
		this.timeElapsed = timeElapsed;
		this.username = username;
		this.preemptionRound = preemptionRound;
		this.wouldPreempt = wouldPreempt;
		this.memHardLimit = memHardLimit;
		this.memswHardLimit = memswHardLimit;
		this.killedProcess = killedProcess;
		this.cgroupPath = cgroupPath;
		this.killingSlotMemory = killingSlotMemory;
		this.killingSlotSwMemory = killingSlotSwMemory;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(String.valueOf(queueId), String.valueOf(preemptionTs), String.valueOf(killingTs), String.valueOf(preemptionSlotMemory), String.valueOf(preemptionSlotSwMemory), String.valueOf(preemptionJobMemory), String.valueOf(numConcurrentJobs), String.valueOf(resubmissionCounter), hostName, siteName, String.valueOf(memoryPerCore), String.valueOf(growthDerivative), String.valueOf(timeElapsed), username, String.valueOf(preemptionRound), String.valueOf(wouldPreempt), String.valueOf(this.getVMUUID()), String.valueOf(memHardLimit), String.valueOf(memswHardLimit), killedProcess, cgroupPath);
	}

	@Override
	public void run() {
		recordingSuccess = TaskQueueUtils.recordPreemption(queueId, preemptionTs, killingTs, preemptionSlotMemory, preemptionSlotSwMemory, preemptionJobMemory, numConcurrentJobs, resubmissionCounter, hostName, siteName, memoryPerCore, growthDerivative, timeElapsed, username, preemptionRound, wouldPreempt, this.getVMUUID(), memHardLimit, memswHardLimit, killedProcess, cgroupPath, killingSlotMemory, killingSlotSwMemory);
	}

	public boolean getRecordingSuccess() {
		return this.recordingSuccess;
	}

	@Override
	public String toString() {
		if (killingTs > 0)
			return "System killed the job due to OOM at timestamp " + killingTs + " running in " + siteName + " (host " + hostName + ")";
		return "Recording preemption for " + queueId + " at timestamp " + preemptionTs + " for memory overconsumption. The job was consuming " + preemptionJobMemory + " MB and was killed.It was running in " + siteName + " (host " + hostName + " site " + siteName + ")";
	}
}
