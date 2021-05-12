package alien.log;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import alien.api.Request;
import alien.config.ConfigUtils;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import alien.user.AliEnPrincipal;
import lazyj.Format;

/**
 * @author costing
 * @since 2020-01-29
 */
public class RequestEvent implements Closeable {

	/**
	 * Requester identity, if known
	 */
	public AliEnPrincipal identity = null;

	/**
	 * Remote IP address where the request came from
	 */
	public InetAddress clientAddress = null;

	/**
	 * Remote port number, if known
	 */
	public int clientPort = -1;

	/**
	 * Client site mapping, if known
	 */
	public String site = null;

	/**
	 * Unique session ID
	 */
	public UUID clientID = null;

	/**
	 * Server thread ID (commander sequence)
	 */
	public Long serverThreadID = null;

	/**
	 * Request ID of that client, if available
	 */
	public Long requestId = null;

	/**
	 * Command that was run
	 */
	public String command = null;

	/**
	 * Arguments to it
	 */
	public List<String> arguments = null;

	/**
	 * If some exception happened during running
	 */
	public Exception exception = null;

	/**
	 * Command exit code
	 */
	public int exitCode = Integer.MIN_VALUE;

	/**
	 * Any error message?
	 */
	public String errorMessage = null;

	/**
	 * Duration of this request
	 */
	public final Timing timing = new Timing();

	private final Long startTimestamp = Long.valueOf(System.currentTimeMillis());

	private final OutputStream os;

	/**
	 * User-defined connection properties
	 */
	public Map<String, Object> userProperties;

	/**
	 * Create a request event that can be written to the given stream at the end of the execution
	 * 
	 * @param os
	 */
	public RequestEvent(final OutputStream os) {
		this.os = os;
	}

	private Map<String, Object> getValues() {
		final Map<String, Object> values = new LinkedHashMap<>();

		values.put("timestamp", startTimestamp);

		if (identity != null) {
			values.put("user", identity.getDefaultUser());
			values.put("role", identity.getDefaultRole());

			if (clientAddress == null && identity.getRemoteEndpoint() != null)
				clientAddress = identity.getRemoteEndpoint();

			if (clientPort <= 0)
				clientPort = identity.getRemotePort();
		}

		if (clientAddress != null) {
			String address = clientAddress.getHostAddress();

			if (address.indexOf(':') > 0)
				address = "[" + address + "]";

			if (clientPort > 0)
				values.put("address", address + ":" + clientPort);
			else
				values.put("address", address);
		}
		else if (clientPort > 0)
			values.put("address", ":" + clientPort);

		if (site != null)
			values.put("site", site);

		if (serverThreadID != null)
			values.put("serverThreadID", serverThreadID);

		if (clientID != null)
			values.put("clientID", clientID);

		if (requestId != null)
			values.put("requestID", requestId);

		if (command != null)
			values.put("command", command);

		if (arguments != null && arguments.size() > 0)
			values.put("arguments", arguments);

		if (exitCode != Integer.MIN_VALUE)
			values.put("exitCode", Integer.valueOf(exitCode));

		if (errorMessage != null)
			values.put("errorMessage", errorMessage);

		if (exception != null) {
			values.put("exceptionMessage", exception.getMessage());
			values.put("exceptionTrace", Arrays.toString(exception.getStackTrace()));
		}

		values.put("duration", Double.valueOf(timing.getMillis()));

		values.put("server", ConfigUtils.getLocalHostname());
		values.put("server_pid", Integer.valueOf(MonitorFactory.getSelfProcessID()));
		values.put("server_uuid", Request.getVMID());

		if (userProperties != null)
			userProperties.forEach((k, v) -> values.putIfAbsent(k, v));

		return values;
	}

	/**
	 * @return the JSON representation of this event
	 */
	public String toJSON() {
		return Format.toJSON(getValues(), false).toString();
	}

	@Override
	public void close() throws IOException {
		if (os != null)
			os.write((toJSON() + "\n").getBytes());
	}
}
