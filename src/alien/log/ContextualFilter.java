/**
 * ContextualFilter.java
 * @author thallybu
 * @since Nov 23, 2017
 * 
 *  	The ContextualFilter class considers the current config/Context in their isLoggable() method.
 *  	The conditions can be set in logging.properties
 */
package alien.log;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.*;

import alien.config.Context;

/**
 * @author thallybu
 *
 */
public class ContextualFilter implements Filter {
	
	@Override
	public boolean isLoggable(LogRecord record) {
		
		// get the current context
		HashMap<String, Object> ctx = (HashMap<String, Object>) Context.getThreadContext();
		
		// get the filter conditions
		
		
		return false;
	}

}
