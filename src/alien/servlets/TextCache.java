/**
 * 
 */
package alien.servlets;

import java.lang.ref.WeakReference;
import java.util.concurrent.ConcurrentHashMap;

import lazyj.ExtendedServlet;
import lazyj.cache.ExpirationCache;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;

/**
 * @author costing
 * @since Apr 28, 2011
 */
public class TextCache extends ExtendedServlet {
	private static final long serialVersionUID = 6024682549531639348L;
	
	private final static ConcurrentHashMap<String, ExpirationCache<String, WeakReference<String>>> namespaces = new ConcurrentHashMap<String, ExpirationCache<String,WeakReference<String>>>();
	
	/**
	 * Monitoring component
	 */
	static transient final Monitor monitor = MonitorFactory.getMonitor(TextCache.class.getCanonicalName());
	
	private final ExpirationCache<String, WeakReference<String>> getNamespace(final String name){
		ExpirationCache<String, WeakReference<String>> ret = namespaces.get(name);
		
		if (ret!=null)
			return ret;
		
		ret = new ExpirationCache<String, WeakReference<String>>(10240);
		
		namespaces.put(name, ret);
		
		return ret;
	}
	
	/* (non-Javadoc)
	 * @see lazyj.ExtendedServlet#execGet()
	 */
	@Override
	public void execGet() {
		final String ns = gets("ns", "default");
		
		final String key = gets("key");
		
		if (key.length()==0){
			pwOut.println("ERR: key");
			return;
		}
		
		final ExpirationCache<String, WeakReference<String>> cache = getNamespace(ns);
		
		String value = gets("value", null);
		
		if (value!=null){
			if (monitor != null)
				monitor.incrementCounter("SET_"+ns);
			
			cache.put(key, new WeakReference<String>(value), getl("timeout", 60*60)*1000);
			return;
		}
		
		final WeakReference<String> weak = cache.get(key);
		
		if (weak==null){
			if (monitor != null)
				monitor.incrementCounter("NULL_"+ns);
			
			pwOut.println("ERR: null");
			return;
		}
		
		value = weak.get();
		
		if (value==null){
			if (monitor != null)
				monitor.incrementCounter("EXPIRED_"+ns);
			
			pwOut.println("ERR: expired");
			return;
		}
		
		if (monitor != null)
			monitor.incrementCounter("HIT_"+ns);
		
		pwOut.println(value);
	}

}
