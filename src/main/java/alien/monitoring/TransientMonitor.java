/**
 * 
 */
package alien.monitoring;

/**
 * @author costing
 * @since Aug 19, 2022
 */
public interface TransientMonitor {
	/**
	 * @return <code>true</code> if there is no more data on this module and should be removed
	 */
	boolean shouldStop();
}
