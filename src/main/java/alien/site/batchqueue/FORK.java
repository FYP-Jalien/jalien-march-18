package alien.site.batchqueue;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * @author mmmartin
 */
public class FORK extends BatchQueue {

	/**
	 * @param conf
	 * @param logr
	 */
	public FORK(HashMap<String, Object> conf, Logger logr) {
		this.config = conf;
		logger = logr;
		// String host_logdir = (String) config.get("host_logdir");
		// logger = LogUtils.redirectToCustomHandler(logger, Functions.resolvePathWithEnv(host_logdir) + "/JAliEn." + (new Timestamp(System.currentTimeMillis()).getTime() + ".out"));

		logger.info("This VO-Box is " + config.get("ALIEN_CM_AS_LDAP_PROXY") + ", site is " + config.get("site_accountname"));
	}

	@Override
	public void submit(final String script) {
		logger.info("Submit FORK");

		ArrayList<String> cmd = new ArrayList<>();
		cmd.add(script);

		ArrayList<String> proc_output = new ArrayList<>();
		try {
			final ProcessBuilder proc_builder = new ProcessBuilder(cmd);

			Map<String, String> env = proc_builder.environment();
			env.clear();
			proc_builder.redirectErrorStream(false);

			final Process proc = proc_builder.start();
			try (BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()))) {
				String output_str;
				while ((output_str = reader.readLine()) != null) {
					proc_output.add(output_str);
				}
			}
		}
		catch (final Throwable t) {
			logger.info("[FORK] Exception executing command '" + cmd + "':" + t.toString());
		}
		logger.info(String.format("[FORK] Command output: %s", proc_output));
	}

	@Override
	public int getNumberActive() {
		return 0;
	}

	@Override
	public int getNumberQueued() {
		return 0;
	}

	@Override
	public int kill() {
		return 0;
	}

}
