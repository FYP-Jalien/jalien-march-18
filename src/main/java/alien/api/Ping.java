package alien.api;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author costing
 * @since 2019-06-07
 */
public class Ping extends Request {

	/**
	 * Generated serial UID
	 */
	private static final long serialVersionUID = -2970460898632648056L;

	private Map<String, String> serverInfo = null;

	@Override
	public void run() {
		serverInfo = new LinkedHashMap<>();

		try {
			serverInfo.put("hostname", InetAddress.getLocalHost().getCanonicalHostName());
		}
		catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Server side returns some information on its status.
	 * 
	 * @return some server provided information
	 */
	public Map<String, String> getServerInfo() {
		return serverInfo;
	}

}
