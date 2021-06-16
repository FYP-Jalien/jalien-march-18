package alien.api.taskQueue;

import java.io.IOException;
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
	private String tag;
	private String message;
	private long timestamp = System.currentTimeMillis();

	/**
	 * @param jobnumber
	 * @param tag
	 * @param message
	 */
	public PutJobLog(final long jobnumber, final String tag, final String message) {
		this.jobnumber = jobnumber;
		this.tag = tag;
		this.message = message;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(String.valueOf(jobnumber), tag, message);
	}

	@Override
	public void run() {
		TaskQueueUtils.putJobLog(timestamp, jobnumber, tag, message, null);
		tag = message = null;
	}

	@Override
	public String toString() {
		return "Asked to put joblog [" + this.tag + "-" + this.message + "] for job: " + this.jobnumber + "@" + this.timestamp;
	}
}
