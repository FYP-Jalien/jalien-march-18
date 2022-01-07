package alien.site.supercomputing.titan;

/**
 * @author psvirin
 *
 */
public class ProcInfoPair {
	/**
	 * Job ID
	 */
	public final long queue_id;

	/**
	 * Job resubmission counter
	 */
	public final int resubmission;

	/**
	 * Proc line to add to the trace
	 */
	public final String procinfo;

	/**
	 * @param queue_id
	 * @param resubmission 
	 * @param procinfo
	 */
	public ProcInfoPair(final String queue_id, final String resubmission, final String procinfo) {
		this.queue_id = Long.parseLong(queue_id);
		this.resubmission = Integer.parseInt(resubmission);
		this.procinfo = procinfo;
	}
}
