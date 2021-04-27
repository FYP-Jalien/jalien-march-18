package alien.site.batchqueue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import alien.site.Functions;

/**
 * @author mmmartin
 */
public class HTCONDOR extends BatchQueue {

	private final HashMap<String, String> environment = new HashMap<>();
	private TreeSet<String> envFromConfig;
	private final String submitCmd;
	private String submitArgs = "";
	private String htc_logdir = "$HOME/htcondor";
	private String grid_resource = null;
	private boolean use_job_router = false;
	private boolean use_external_cloud = false;

	private static final Pattern p = Pattern.compile("(\\d+)\\s*\\*\\s*(\\S+)");

	//
	// 2020-06-24 - Maarten Litmaath, Maxim Storetvedt
	//
	// to support weighted, round-robin load-balancing over a CE set:
	//

	private final ArrayList<String> ce_list = new ArrayList<>();
	private final HashMap<String, Double> ce_weight = new HashMap<>();
	private int next_ce = 0;
	private final HashMap<String, AtomicInteger> running = new HashMap<>();
	private final HashMap<String, AtomicInteger> waiting = new HashMap<>();
	private int tot_running = 0;
	private int tot_waiting = 0;
	private long job_numbers_timestamp = 0;
	private long proxy_check_timestamp = 0;

	//
	// our own Elvis operator approximation...
	//

	private static String if_else(final String value, final String fallback) {
		return value != null ? value : fallback;
	}

	/**
	 * @param conf
	 * @param logr
	 */
	@SuppressWarnings("unchecked")
	public HTCONDOR(final HashMap<String, Object> conf, final Logger logr) {
		config = conf;
		logger = logr;

		logger.info("This VO-Box is " + config.get("ALIEN_CM_AS_LDAP_PROXY") +
				", site is " + config.get("site_accountname"));

		final String ce_env_str = "ce_environment";

		if (config.get(ce_env_str) == null) {
			final String msg = ce_env_str + " needs to be defined!";
			logger.warning(msg);
			config.put(ce_env_str, new TreeSet<String>());
		}

		try {
			envFromConfig = (TreeSet<String>) config.get(ce_env_str);
		}
		catch (@SuppressWarnings("unused") final ClassCastException e) {
			envFromConfig = new TreeSet<>(Arrays.asList((String) config.get(ce_env_str)));
		}

		//
		// initialize our environment from the LDAP configuration
		//

		for (final String env_field : envFromConfig) {
			final String[] parts = env_field.split("=", 2);
			final String var = parts[0];
			final String val = parts.length > 1 ? parts[1] : "";
			environment.put(var, val);
			logger.info("envFromConfig: " + var + "=" + val);
		}

		//
		// allow the process environment to override any variable and add others
		//

		environment.putAll(System.getenv());

		final String ce_submit_cmd_str = "CE_SUBMITCMD";

		submitCmd = if_else(environment.get(ce_submit_cmd_str),
				if_else((String) config.get(ce_submit_cmd_str), "condor_submit"));

		String use_job_router_tmp = "0";
		String use_external_cloud_tmp = "0";

		for (final Map.Entry<String, String> entry : environment.entrySet()) {
			final String var = entry.getKey();
			final String val = entry.getValue();

			if ("CE_LCGCE".equals(var)) {
				double tot = 0;

				//
				// support weighted, round-robin load-balancing over a CE set
				// (mind: the WLCG SiteMon VO feed currently needs the ports):
				//
				// CE_LCGCE=[N1 * ]ce1.abc.xyz[:port], [N2 * ]ce2.abc.xyz[:port], ...
				//

				logger.info("Load-balancing over these CEs with configured weights:");

				for (final String str : val.split(",")) {
					double w = 1;
					String ce = str;
					final Matcher m = p.matcher(str);

					if (m.find()) {
						w = Double.parseDouble(m.group(1));
						ce = m.group(2);
					}
					else {
						ce = ce.replaceAll("\\s+", "");
					}

					if (!Pattern.matches(".*\\w.*", ce)) {
						logger.severe("syntax error in CE_LCGCE");
						tot = 0;
						break;
					}

					if (!Pattern.matches(".*:.*", ce)) {
						ce += ":9619";
					}

					logger.info(ce + " --> " + String.format("%5.3f", Double.valueOf(w)));

					ce_list.add(ce);
					ce_weight.put(ce, Double.valueOf(w));
					tot += w;
				}

				if (tot <= 0) {
					final String msg = "CE_LCGCE invalid: " + val;
					logger.severe(msg);
					throw new IllegalArgumentException(msg);
				}

				if (ce_weight.size() != ce_list.size()) {
					final String msg = "CE_LCGCE has duplicate CEs: " + val;
					logger.severe(msg);
					throw new IllegalArgumentException(msg);
				}

				logger.info("Load-balancing over these CEs with normalized weights:");

				for (final String ce : ce_list) {
					final Double w = Double.valueOf(ce_weight.get(ce).doubleValue() / tot);
					ce_weight.replace(ce, w);
					logger.info(ce + " --> " + String.format("%5.3f", w));
				}

				continue;
			}

			if ("SUBMIT_ARGS".equals(var)) {
				submitArgs = val;
				logger.info("environment: " + var + "=" + val);
				continue;
			}

			if ("HTCONDOR_LOG_PATH".equals(var)) {
				htc_logdir = val;
				logger.info("environment: " + var + "=" + val);
				continue;
			}

			if ("GRID_RESOURCE".equals(var)) {
				grid_resource = val;
				logger.info("environment: " + var + "=" + val);
				continue;
			}

			if ("USE_JOB_ROUTER".equals(var)) {
				use_job_router_tmp = val;
				logger.info("environment: " + var + "=" + val);
				continue;
			}

			if ("USE_EXTERNAL_CLOUD".equals(var)) {
				use_external_cloud_tmp = val;
				logger.info("environment: " + var + "=" + val);
				continue;
			}
		}

		htc_logdir = Functions.resolvePathWithEnv(htc_logdir);
		logger.info("htc_logdir: " + htc_logdir);

		use_job_router = Integer.parseInt(use_job_router_tmp) == 1;
		use_external_cloud = Integer.parseInt(use_external_cloud_tmp) == 1;

		if (ce_list.size() <= 0 && grid_resource == null && !use_job_router) {
			final String msg = "No CE usage specified in the environment";
			logger.severe(msg);
			throw new IllegalArgumentException(msg);
		}
	}

	private void proxyCheck() {

		final String proxy = environment.get("X509_USER_PROXY");
		final File proxy_no_check = new File(environment.get("HOME") + "/no-proxy-check");

		if (proxy == null || proxy_no_check.exists()) {
			return;
		}

		final String vo_str = if_else((String) config.get("LCGVO"), "alice");
		final String proxy_renewal_str = String.format("/etc/init.d/%s-box-proxyrenewal", vo_str);
		final File proxy_renewal_svc = new File(proxy_renewal_str);

		if (!proxy_renewal_svc.exists()) {
			return;
		}

		final String threshold = if_else((String) config.get("CE_PROXYTHRESHOLD"), String.valueOf(46 * 3600));
		logger.info(String.format("X509_USER_PROXY is %s", proxy));
		logger.info("Checking remaining proxy lifetime");

		final String proxy_info_cmd = "voms-proxy-info -acsubject -actimeleft 2>&1";
		final ArrayList<String> proxy_info_output = executeCommand(proxy_info_cmd);

		String dn_str = "";
		String time_left_str = "";

		for (final String line : proxy_info_output) {
			final String trimmed_line = line.trim();

			if (trimmed_line.matches("^/.+")) {
				dn_str = trimmed_line;
				continue;
			}

			if (trimmed_line.matches("^\\d+$")) {
				time_left_str = trimmed_line;
				continue;
			}
		}

		if (dn_str.length() == 0) {
			logger.warning("[LCG] No valid VOMS proxy found!");
			return;
		}

		logger.info(String.format("DN is %s", dn_str));
		logger.info(String.format("Proxy timeleft is %s (threshold is %s)", time_left_str, threshold));

		if (Integer.parseInt(time_left_str) > Integer.parseInt(threshold)) {
			return;
		}

		//
		// the proxy shall be managed by the proxy renewal service for the VO;
		// restart it as needed...
		//

		logger.info("Checking proxy renewal service");

		final String proxy_renewal_cmd = String.format("%s start 2>&1", proxy_renewal_svc);
		ArrayList<String> proxy_renewal_output = null;

		try {
			proxy_renewal_output = executeCommand(proxy_renewal_cmd);
		}
		catch (final Exception e) {
			logger.info(String.format("[LCG] Problem while executing command: %s", proxy_renewal_cmd));
			e.printStackTrace();
		}
		finally {
			if (proxy_renewal_output != null) {
				logger.info("Proxy renewal output:\n");

				for (final String line : proxy_renewal_output) {
					logger.info(line.trim());
				}
			}
		}
	}

	@Override
	public void submit(final String script) {
		logger.info("Submit HTCONDOR");

		final DateFormat date_format = new SimpleDateFormat("yyyy-MM-dd");
		final String current_date_str = date_format.format(new Date());
		final String log_folder_path = htc_logdir + "/" + current_date_str;
		final File log_folder = new File(log_folder_path);

		if (!log_folder.exists()) {
			try {
				log_folder.mkdirs();
			}
			catch (final Exception e) {
				logger.severe(String.format("[HTCONDOR] log folder mkdirs() exception: %s",
						log_folder_path));
				e.printStackTrace();
			}

			if (!log_folder.exists()) {
				logger.severe(String.format("[HTCONDOR] Couldn't create log folder: %s",
						log_folder_path));
				return;
			}
		}

		final String file_base_name = String.format("%s/jobagent_%d", log_folder_path, Long.valueOf(System.currentTimeMillis()));
		final String log_cmd = String.format("log = %s.log%n", file_base_name);
		String out_cmd = "";
		String err_cmd = "";

		final File enable_sandbox_file = new File(environment.get("HOME") + "/enable-sandbox");

		if (enable_sandbox_file.exists()) {
			out_cmd = String.format("output = %s.out%n", file_base_name);
			err_cmd = String.format("error = %s.err%n", file_base_name);
		}

		String submit_jdl = "cmd = " + script + "\n" +
				out_cmd +
				err_cmd +
				log_cmd +
				"+TransferOutput = \"\"\n" +
				"periodic_hold = JobStatus == 1 && " +
				"GridJobStatus =?= undefined && CurrentTime - EnteredCurrentStatus > 1800 || " +
				"JobStatus <= 2 && CurrentTime - EnteredCurrentStatus > 172800\n" +
				"periodic_remove = CurrentTime - QDate > 259200\n";

		//
		// via our own load-balancing (preferred), via the JobRouter, or to the single CE
		//

		if (!use_job_router && ce_list.size() > 0) {
			logger.info("Determining the next CE to use:");

			for (final String element : ce_list) {
				final String ce = ce_list.get(next_ce);
				final AtomicInteger idle = waiting.computeIfAbsent(ce, (r) -> new AtomicInteger(0));
				final Double w = ce_weight.get(ce);
				final Double f = Double.valueOf(tot_waiting > 0 ? idle.doubleValue() / tot_waiting : 0);

				logger.info(String.format(
						"--> %s has idle fraction %d / %d = %5.3f vs. weight %5.3f",
						ce, Integer.valueOf(idle.intValue()), Integer.valueOf(tot_waiting), f, w));

				if (f.doubleValue() < w.doubleValue()) {
					break;
				}

				next_ce++;
				next_ce %= ce_list.size();
			}

			final String ce = ce_list.get(next_ce);

			logger.info("--> next CE to use: " + ce);

			waiting.computeIfAbsent(ce, (r) -> new AtomicInteger(0)).incrementAndGet();
			tot_waiting++;

			next_ce++;
			next_ce %= ce_list.size();

			final String h = ce.replaceAll(":.*", "");
			grid_resource = "condor " + h + " " + ce;
		}

		if (use_job_router) {
			submit_jdl += "universe = vanilla\n" +
					"+WantJobRouter = True\n" +
					"job_lease_duration = 7200\n" +
					"ShouldTransferFiles = YES\n";
		}
		else {
			submit_jdl += "universe = grid\n" +
					"grid_resource = " + grid_resource + "\n";
		}

		if (use_external_cloud) {
			submit_jdl += "+WantExternalCloud = True\n";
		}

		submit_jdl += "use_x509userproxy = true\n";

		final String cm = config.get("host_host") + ":" + config.get("host_port");
		final String env_cmd = String.format("ALIEN_CM_AS_LDAP_PROXY='%s'", cm);
		submit_jdl += String.format("environment = \"%s\"%n", env_cmd);

		//
		// allow preceding attributes to be overridden and others added if needed
		//

		final String custom_jdl_path = environment.get("HOME") + "/custom-classad.jdl";

		if ((new File(custom_jdl_path)).exists()) {
			String custom_attr_str = "\n#\n# custom attributes start\n#\n\n";
			custom_attr_str += readJdlFile(custom_jdl_path);
			custom_attr_str += "\n#\n# custom attributes end\n#\n\n";
			submit_jdl += custom_attr_str;
			logger.info("Custom attributes added from file: " + custom_jdl_path);
		}

		//
		// finally
		//

		submit_jdl += "queue 1\n";

		//
		// keep overwriting the same file for ~1 minute
		//

		final String submit_file = log_folder_path + "/htc-submit." + (job_numbers_timestamp >> 16) + ".jdl";

		try (PrintWriter out = new PrintWriter(submit_file)) {
			out.println(submit_jdl);
		}
		catch (final Exception e) {
			logger.severe("Error writing to submit file: " + submit_file);
			e.printStackTrace();
			return;
		}

		final String submit_cmd = submitCmd + " " + submitArgs + " " + submit_file;
		final ArrayList<String> output = executeCommand(submit_cmd);

		for (final String line : output) {
			final String trimmed_line = line.trim();
			logger.info(trimmed_line);
		}
	}

	private String readJdlFile(final String path) {
		final StringBuilder file_contents = new StringBuilder();
		String line;

		try (BufferedReader br = new BufferedReader(new FileReader(path))) {
			final Pattern comment_pattern = Pattern.compile("^\\s*(#.*|//.*)?$");
			final Pattern err_spaces_pattern = Pattern.compile("\\\\\\s*$");

			while ((line = br.readLine()) != null) {
				final Matcher comment_matcher = comment_pattern.matcher(line);
				// skip over comment lines

				if (comment_matcher.matches()) {
					continue;
				}

				// remove erroneous spaces
				line = line.replaceAll(err_spaces_pattern.pattern(), "\\\\\n");
				file_contents.append(line).append('\n');
			}
		}
		catch (final Exception e) {
			logger.severe("ERROR when reading JDL file: " + path);
			e.printStackTrace();
			return "";
		}

		return file_contents.toString();
	}

	private boolean getJobNumbers() {

		final long now = System.currentTimeMillis();
		final long dt = (now - job_numbers_timestamp) / 1000;

		if (dt < 60) {
			logger.info("Reusing cached job numbers collected " + dt + " seconds ago");
			return true;
		}

		//
		// take advantage of this regular call to check how the proxy is doing as well
		//

		if ((now - proxy_check_timestamp) / 1000 > 3600) {
			proxyCheck();
			proxy_check_timestamp = now;
		}

		final String cmd = "condor_q -const 'JobStatus < 3' -af JobStatus GridResource";
		final ArrayList<String> job_list = executeCommand(cmd);

		tot_running = tot_waiting = 0;
		final Pattern p = Pattern.compile("^\\s*([12]).*\\s(\\S+)");

		// in case the CE list has changed
		running.clear();
		waiting.clear();

		for (final String ce : ce_list) {
			waiting.put(ce, new AtomicInteger(0));
			running.put(ce, new AtomicInteger(0));
		}

		for (final String line : job_list) {
			final Matcher m = p.matcher(line);

			if (m.matches()) {
				final int job_status = Integer.parseInt(m.group(1));
				final String ce = m.group(2);

				if (job_status == 1) {
					final AtomicInteger w = waiting.get(ce);

					if (w != null)
						w.incrementAndGet();

					tot_waiting++;
				}
				else {
					final AtomicInteger r = running.get(ce);

					if (r != null)
						r.incrementAndGet();

					tot_running++;
				}
			}
		}

		logger.info("Found " + tot_waiting + " idle (and " + tot_running + " running) jobs:");

		for (final String ce : ce_list) {
			logger.info(String.format("%5d (%5d) for %s", Integer.valueOf(waiting.get(ce).intValue()), Integer.valueOf(running.get(ce).intValue()), ce));
		}

		job_numbers_timestamp = now;
		return true;
	}

	@Override
	public int getNumberActive() {

		if (!getJobNumbers()) {
			return -1;
		}

		return tot_running + tot_waiting;
	}

	@Override
	public int getNumberQueued() {

		if (!getJobNumbers()) {
			return -1;
		}

		return tot_waiting;
	}

	@Override
	public int kill() {
		logger.info("Kill command not implemented");
		return 0;
	}
}
