package alien.site.batchqueue;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.logging.Logger;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lia.util.process.ExternalProcess.ExitStatus;
import utils.ProcessWithTimeout;
import java.util.logging.Level;

/**
 * Base interface for batch queues
 *
 * @author mmmartin
 */
public abstract class BatchQueue {
	/**
	 * Logging mechanism shared with the implementing code
	 */
	protected Logger logger = null;

	/**
	 * Common env variables for running commands
	 */
	public HashMap<String, String> additional_env_vars = new HashMap<>(){{
			put("LD_LIBRARY_PATH", System.getenv("LD_LIBRARY_PATH"));
			put("PATH", System.getenv("PATH"));
	}};

	/**
	 * Common configuration mechanism with the BQ implementations
	 */
	protected HashMap<String, Object> config = null;

	/**
	 * Submit a new job agent to the queue
	 * 
	 * @param script
	 */
	public abstract void submit(final String script);

	/**
	 * @return number of currently active jobs
	 */
	public abstract int getNumberActive();

	/**
	 * @return number of queued jobs
	 */
	public abstract int getNumberQueued();

	/**
	 * @return how many jobs were killed
	 */
	public abstract int kill();
	// Previously named "_system" in perl

	public ArrayList<String> executeCommand(String cmd) {
		ArrayList<String> proc_output = new ArrayList<>();
		try {
			ArrayList<String> cmd_full = new ArrayList<>();
			cmd_full.add("/bin/bash");
			cmd_full.add("-c");
			cmd_full.add(cmd);
			final ProcessBuilder proc_builder = new ProcessBuilder(cmd_full);

			Map<String, String> env = proc_builder.environment();
			env.clear();

			env.putAll(additional_env_vars);

			proc_builder.redirectErrorStream(false);

			final Process proc = proc_builder.start();

			final ProcessWithTimeout pTimeout = new ProcessWithTimeout(proc, proc_builder);

			pTimeout.waitFor(60, TimeUnit.SECONDS);

			final ExitStatus exitStatus = pTimeout.getExitStatus();
			logger.info("Process exit status: " + exitStatus.getExecutorFinishStatus());

			if (exitStatus.getExtProcExitStatus() == 0) {
				final BufferedReader reader = new BufferedReader(new StringReader(exitStatus.getStdOut()));

				String output_str;

				while ((output_str = reader.readLine()) != null)
					proc_output.add(output_str.trim());
			}
		}
		catch (final Throwable t) {
			logger.log(Level.WARNING, "Exception executing command: " + cmd, t);
		}
		this.logger.info("[BatchQueue] Command output: " + proc_output);
		return proc_output;
	}

	public final String getValue(final String keyValue, final String key, final String defaultValue){
		if (keyValue.startsWith(key+'='))
			return keyValue.substring(key.length()+1).trim();

		return defaultValue;
	}
}
