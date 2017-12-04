package alien.config;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author costing
 */
public class Context {

	/**
	 * The String that is used for the contextual logging tags. It should not collide
	 * with any other used tags.
	 */
	final private static String loggingTag = "jalien::log::loggingContext";
	
	/**
	 * Thread-tied contexts
	 */
	static Map<Thread, Map<String, Object>> context = new ConcurrentHashMap<>();

	static {
		final Thread cleanupThread = new Thread("alien.config.Context.cleanup") {
			@Override
			public void run() {
				while (true)
					try {
						Thread.sleep(1000 * 60);

						context.keySet().retainAll(Thread.getAllStackTraces().keySet());
					} catch (@SuppressWarnings("unused")
					final Throwable t) {
						// ignore
					}
			}
		};

		cleanupThread.setDaemon(true);
		cleanupThread.start();
	}

	/**
	 * Associate an object to a key in current thread's context
	 * 
	 * @param key
	 * @param value
	 * @return the previously set value for this key
	 * 
	 * @see #resetContext()
	 */
	public static Object setThreadContext(final String key, final Object value) {
		Map<String, Object> m = context.get(Thread.currentThread());

		if (m == null) {
			// the map will be accessed from within the same thread, so there
			// can be no conflict here
			m = new HashMap<>();
			context.put(Thread.currentThread(), m);
		}

		return m.put(key, value);
	}

	/**
	 * Get the content of a particular key from the current thread's context
	 * 
	 * @param key
	 * @return the value associated with this key for the current thread. Can be <code>null</code> if the key is not set
	 */
	public static Object getTheadContext(final String key) {
		final Map<String, Object> m = context.get(Thread.currentThread());

		if (m == null)
			return null;

		return m.get(key);
	}

	/**
	 * Get the entire current thread's context map
	 * 
	 * @return everything that is set in current thread's context. Can be <code>null</code>
	 */
	public static Map<String, Object> getThreadContext() {
		return context.get(Thread.currentThread());
	}

	/**
	 * Reset current thread's context so that previously set values don't leak into next call's environment
	 * 
	 * @return previous content, if any
	 */
	public static Map<String, Object> resetContext() {
		return context.remove(Thread.currentThread());
	}
	
	/**
	 * Make sure we can use the logging-shortcuts without worrying about non-existing thread context and floating null pointers.
	 */
	private static void assertThreadContextExists() {
		if(getThreadContext() != null) {
			return;
		} else {
			setThreadContext("", "");
		}
	}
	
	/**
	 * Adds a key (tag) to the current logging context. This method does not inspect the
	 * logging context to avoid duplicates at the moment. 
	 * 
	 * @param tag - The tag to be added into the logging context
	 */
	public static void addToLoggingContext(String tag) {
		assertThreadContextExists();
		// check if there already is a logging context
		Map<String, Object> currThreadContetxt = getThreadContext();
		String currLogContext = (String) currThreadContetxt.get(loggingTag);						
		
		// update or create the logging context
		if(currLogContext != null && currLogContext.length() > 0) {
			currLogContext = String.join(",", currLogContext, tag);
		} else {
			currLogContext = tag;
		}
		
		// update the thread context
		currThreadContetxt.put(loggingTag, currLogContext);
	}
	
	/**
	 * Same as addToLoggingContext with the convenience of adding multiple tags at once.
	 * 
	 * @param tag
	 */
	public static void addToLoggingContext(String... tag) {
		addToLoggingContext(String.join(",", tag));
	}
	
	/**
	 * Searches for `tags` in the current logging context and, if one or more were found, removes them.
	 * This method has the side effect of removing duplicate tags from the logging context.
	 * 
	 * @param tags - the logging tags to be removed
	 */
	public static void removeFromLoggingContext(String... tags) {
		assertThreadContextExists();
		// check if there already is a logging context
		Map<String, Object> currThreadContetxt = getThreadContext();
		String currLogContext = (String) currThreadContetxt.get(loggingTag);
		
		// convert the array to a set, this also removed possible duplicate tags
		Set<String> tagsAsSet = new HashSet<String>(Arrays.asList(currLogContext.split(",")));
		if(tagsAsSet.removeAll(Arrays.asList(tags))) {
			currThreadContetxt.put(loggingTag, String.join(",", tagsAsSet));
		}
		
	};
	
	/**
	 * Conveniance method. Shortcut for <code>getThreadContext("logging")</code>
	 */
	public static String getLoggingContext() {
		assertThreadContextExists();
		return (String) getThreadContext().get(loggingTag);
	}
		 
	/**
	 * Resets the logging context - effectively removing all tags at once from it.
	 */
	public static void resetLoggingContext() {
		assertThreadContextExists();
		getThreadContext().put(loggingTag, "");
	}
}
