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
	private final int numConcurrentJobs;
	private final String preemptionTechnique;
	private final int resubmissionCounter;
	private final String hostName;
	private final String siteName;

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
	 */
	public RecordPreemption(final long queueId, final long preemptionTs, final long killingTs, final double preemptionSlotMemory, final double preemptionJobMemory, final int numConcurrentJobs, final String preemptionTechnique, final int resubmissionCounter, final String hostName, final String siteName) {
		this.queueId = queueId;
		this.preemptionTs = preemptionTs;
		this.killingTs = killingTs;
		this.preemptionSlotMemory = preemptionSlotMemory;
		this.preemptionJobMemory = preemptionJobMemory;
		this.numConcurrentJobs = numConcurrentJobs;
		this.preemptionTechnique = preemptionTechnique;
		this.resubmissionCounter = resubmissionCounter;
		this.hostName = hostName;
		this.siteName = siteName;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(String.valueOf(queueId), String.valueOf(preemptionTs), String.valueOf(killingTs), String.valueOf(preemptionSlotMemory), String.valueOf(preemptionJobMemory), String.valueOf(numConcurrentJobs), preemptionTechnique, String.valueOf(resubmissionCounter), hostName, siteName);
	}

	@Override
	public void run() {
		TaskQueueUtils.recordPreemption(queueId, preemptionTs, killingTs, preemptionSlotMemory, preemptionJobMemory, numConcurrentJobs, preemptionTechnique, resubmissionCounter, hostName, siteName);
	}

	@Override
	public String toString() {
		if (killingTs > 0)
			return "System killed the job due to OOM at timestamp " + killingTs + " running in " + siteName + " (host " + hostName + ")";
		return "Recording preemption for " + queueId + " at timestamp " + preemptionTs + " for memory overconsumption. The job was consuming " + preemptionJobMemory + " MB and was killed using strategy " + preemptionTechnique + "  .It was running in " + siteName + " (host " + hostName + " site " + siteName + ")";
	}
}
