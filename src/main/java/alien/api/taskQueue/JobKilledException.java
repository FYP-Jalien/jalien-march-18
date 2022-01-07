/**
 * 
 */
package alien.api.taskQueue;

import alien.api.ServerException;

/**
 * Messages from killed / resubmitted jobs are rejected with this exception
 * 
 * @author costing
 * @since Jan 7, 2022
 */
public class JobKilledException extends ServerException {
	/**
	 * Generated serial ID
	 */
	private static final long serialVersionUID = 7637881867137874750L;

	/**
	 * @param message
	 * @param cause
	 */
	public JobKilledException(final String message, final Throwable cause) {
		super(message, cause);
	}
}
