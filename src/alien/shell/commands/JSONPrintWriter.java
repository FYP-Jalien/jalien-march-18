package alien.shell.commands;

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import alien.config.ConfigUtils;

/**
 * @author yuw
 *
 */
public class JSONPrintWriter extends UIPrintWriter {

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(JSONPrintWriter.class.getCanonicalName());

	/**
	 * error String tag to mark a println for stderr
	 */
	public static final String errTag = "ERR: ";

	private final OutputStream os;

	private final JSONArray resultArray;
	private final JSONObject metadataResult;

	private LinkedHashMap<String, String> currentResult;

	/**
	 * @param os
	 */
	public JSONPrintWriter(final OutputStream os) {
		this.os = os;
		resultArray = new JSONArray();
		metadataResult = new JSONObject();
	}

	@Override
	protected void blackwhitemode() {
		// TODO Auto-generated method stub

	}

	@Override
	protected void colourmode() {
		// TODO Auto-generated method stub

	}

	@Override
	protected boolean colour() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	protected void printOut(final String line) {
		//
	}

	@Override
	protected void printErr(final String line) {
		//
	}

	@Override
	protected void setenv(final String cDir, final String user) {
		// TODO Auto-generated method stub

	}

	/**
	 * Write data to the client OutputStream
	 */
	@SuppressWarnings("unchecked")
	@Override
	protected void flush() {
		nextResult(); // Add the last item and go

		try {
			final JSONObject replyObject = new JSONObject();
			if (metadataResult != null)
				replyObject.put("metadata", metadataResult);

			replyObject.put("results", resultArray);
			os.write(replyObject.toJSONString().getBytes());
			os.flush();
		} catch (final IOException e) {
			e.printStackTrace();
			logger.log(Level.FINE, "Could not write JSON to the client OutputStream", e);
		}

		resultArray.clear();
		metadataResult.clear();
	}

	@Override
	protected void pending() {
		// TODO Auto-generated method stub

	}

	@SuppressWarnings("unchecked")
	@Override
	void nextResult() {
		if (currentResult != null) {
			resultArray.add(currentResult);
			currentResult = null;
		}
	}

	@Override
	void setField(final String key, final String value) {
		if (currentResult == null)
			currentResult = new LinkedHashMap<>();

		currentResult.put(key, value);
	}

	/**
	 * Set a result meta information
	 *
	 * @param key
	 * @param value
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void setMetaInfo(final String key, final String value) {
		if (value != null)
			metadataResult.put(key, value);
	}

	@Override
	void setReturnCode(final int exitCode, final String errorMessage) {
		setMetaInfo("exitcode", String.valueOf(exitCode));
		setMetaInfo("error", errorMessage);
	}

	@Override
	protected boolean isRootPrinter() {
		return true;
	}

}
