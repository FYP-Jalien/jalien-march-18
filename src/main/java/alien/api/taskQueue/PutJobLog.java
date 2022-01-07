package alien.api.taskQueue;

import java.util.Arrays;
import java.util.List;

import alien.api.Request;
import alien.taskQueue.TaskQueueUtils;

/**
 * Put a job log
 *
 * @author mmmartin
 * @since Dec 15, 2015
 */
public class PutJobLog extends Request {
	/**
	 *
	 */
	private static final long serialVersionUID = -6330031807464568555L;
	private final long jobnumber;
	private final int resubmission;
	private String tag;
	private String message;
	private long timestamp = System.currentTimeMillis();

	/**
	 * @param jobnumber
	 * @param resubmission
	 * @param tag
	 * @param message
	 */
	public PutJobLog(final long jobnumber, final int resubmission, final String tag, final String message) {
		this.jobnumber = jobnumber;
		this.resubmission = resubmission;
		this.tag = tag;
		this.message = message;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(String.valueOf(jobnumber), String.valueOf(resubmission), tag, message);
	}

	@Override
	public void run() {
		if (TaskQueueUtils.getResubmission(Long.valueOf(jobnumber)) == resubmission)
			TaskQueueUtils.putJobLog(timestamp, jobnumber, tag, message, null);

		tag = message = null;
	}

	@Override
	public String toString() {
		return "Asked to put joblog [" + this.tag + "-" + this.message + "] for job: " + this.jobnumber + "/" + this.resubmission + "@" + this.timestamp;
	}
}
