package alien.shell.commands;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;
import lazyj.Format;

/**
 * @author ron
 * @since July 15, 2011
 */
public class JShPrintWriter extends UIPrintWriter {

	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(JShPrintWriter.class.getCanonicalName());

	/**
	 *
	 */
	public static final String lineTerm = String.valueOf((char) 0);
	/**
	 *
	 */
	public static final String SpaceSep = String.valueOf((char) 1);

	/**
	 *
	 */
	public static final String pendSignal = String.valueOf((char) 9);

	/**
	 * error String tag to mark a println for stderr
	 */
	public static final String errTag = String.valueOf((char) 5);

	/**
	 * String tag to mark the last line of an output
	 */
	public static final String outputterminator = String.valueOf((char) 7);

	/**
	 * String tag to mark the last line of an transaction stream
	 */
	public static final String streamend = String.valueOf((char) 0);

	/**
	 * String tag to mark separated fields
	 */
	public static final String fieldseparator = String.valueOf((char) 1);

	/**
	 * String tag to signal pending action
	 */
	public static final String pendingSignal = String.valueOf((char) 9);

	/**
	 * marker for -Colour argument
	 */
	protected boolean bColour = true;

	/**
	 * Metainfo of the command
	 */
	private HashMap<String, String> metadataResult;

	@Override
	protected void blackwhitemode() {
		bColour = false;
	}

	@Override
	protected void colourmode() {
		bColour = true;
	}

	/**
	 * color status
	 *
	 * @return state of the color mode
	 */
	@Override
	protected boolean colour() {
		return bColour;
	}

	private final OutputStream os;

	/**
	 * @param os
	 */
	public JShPrintWriter(final OutputStream os) {
		this.os = os;
		metadataResult = new HashMap<>();
	}

	private void print(final String line) {
		try {
			os.write(line.getBytes());
			os.flush();
		}
		catch (final IOException e) {
			e.printStackTrace();
			logger.log(Level.FINE, "Could not write to OutputStream" + line, e);
		}
	}

	private static String cleanupLine(final String line) {
		if (line.indexOf('\0') < 0)
			return line;

		return Format.replace(line, "\0", "");
	}

	@Override
	protected void printOut(final String line) {
		print(cleanupLine(line));
	}

	@Override
	protected void printErr(final String line) {
		print(errTag + cleanupLine(line));
	}

	@Override
	protected void setenv(final String cDir, final String user) {
		print(outputterminator + cDir + fieldseparator + user + "\n");
	}

	@Override
	protected void flush() {
		print(streamend + "\n");
	}

	@Override
	protected void pending() {
		print(pendingSignal + "\n");
	}

	@Override
	protected void nextResult() {
		// ignored
	}

	@Override
	public void setField(final String key, final Object value) {
		// ignored
	}

	@Override
	public void setMetaInfo(final String key, final String value) {
		if (key != null && !key.isEmpty())
			if (value != null)
				metadataResult.put(key, value);
			else
				metadataResult.remove(key);
	}

	@Override
	protected void setReturnArgs(final String args) {
		// void
	}

	@Override
	public void setReturnCode(final int exitCode, final String errorMessage) {
		if (!errorMessage.isEmpty()) {
			printErrln(errorMessage);
		}
		setMetaInfo("exitcode", String.valueOf(exitCode));
		setMetaInfo("error", errorMessage);
	}

	@Override
	public String getMetaInfo(String key) {
		return key != null && !key.isEmpty() ? metadataResult.get(key).toString() : "";
	}

	@Override
	public int getReturnCode() {
		final String exitCode = getMetaInfo("exitcode");
		return !exitCode.isEmpty() ? Integer.parseInt(exitCode) : 0;
	}

	@Override
	public String getErrorMessage() {
		return getMetaInfo("error");
	}
}
