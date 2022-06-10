package alien.site.batchqueue;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import alien.site.Functions;
import lia.util.process.ExternalProcess.ExecutorFinishStatus;
import lia.util.process.ExternalProcess.ExitStatus;

public class PBS extends BatchQueue {

	private final Map<String, String> environment;
	private String submitCmd;
	private String submitArg = "";
	private String statusArg = "";
	private String statusCmd;
	private TreeSet<String> envFromConfig;
	private boolean stageIn = true;
	private final String user;

	/**
	 * @param conf
	 * @param logr
	 */
	@SuppressWarnings("unchecked")
	public PBS(HashMap<String, Object> conf, Logger logr) {
		this.environment = System.getenv();
		this.config = conf;
		this.logger = logr;
		this.logger.info("This VO-Box is " + config.get("ALIEN_CM_AS_LDAP_PROXY") + ", site is "
				+ config.get("site_accountname"));

		try {
			this.envFromConfig = (TreeSet<String>) this.config.get("ce_environment");
		}
		catch (final ClassCastException e) {
			logger.severe(e.toString());
		}

		// Initialize from LDAP
		this.submitCmd = (String) config.getOrDefault("ce_submitcmd", "qsub");
		this.statusCmd = (String) config.getOrDefault("ce_statuscmd", "qstat -n -1");

		this.submitArg = readArgFromLdap("ce_submitarg");
		this.statusArg = readArgFromLdap("ce_statusarg");

		// Get environment from LDAP
		if (envFromConfig != null) {
			for (String env_field : envFromConfig) {
				if (env_field.contains("SUBMIT_ARGS")) {
					this.submitArg = getValue(env_field, "SUBMIT_ARGS", this.submitArg);
				}
				if (env_field.contains("STATUS_ARGS")) {
					this.statusArg = getValue(env_field, "STATUS_ARGS", this.statusArg);
				}
			}
		}

		// Override with process environment
		this.submitArg = environment.getOrDefault("SUBMIT_ARGS", submitArg);
		this.statusArg = environment.getOrDefault("STATUS_ARGS", this.statusArg);
		
		user = environment.get("USER");


		this.statusCmd = statusCmd + " -u " + user + " " + statusArg;  
		this.submitCmd = submitCmd + " " + submitArg;

	}

	@Override
	public void submit(final String script) {
		this.logger.info("Submit PBS");

		final DateFormat date_format = new SimpleDateFormat("yyyy-MM-dd");
		final String current_date_str = date_format.format(new Date());
		final Long timestamp = Long.valueOf(System.currentTimeMillis());

		String out_cmd = "#PBS -o /dev/null\n";
		String err_cmd = "#PBS -e /dev/null\n";

		// Check if logdir is declared
		final String host_logdir = config.get("host_logdir") != null ? config.get("host_logdir").toString() : null;

		if (host_logdir != null) {
			final String log_folder_path = String.format("%s/%s", Functions.resolvePathWithEnv(host_logdir), current_date_str);
			final File log_folder = new File(log_folder_path);
			if (!(log_folder.exists())) {
				try {
					log_folder.mkdirs();
				}
				catch (final SecurityException e) {
					this.logger.info(String.format("[PBS] Couldn't create log folder, dont have permission: %s", log_folder_path));
					e.printStackTrace();
				}
				catch (final Exception e) {
					this.logger.info(String.format("[PBS] Exception with mkdirs(): %s", log_folder_path));
					e.printStackTrace();
				}
				if (!log_folder.exists()) {
					logger.info(String.format("[PBS] Couldn't create log folder: %s",
							log_folder_path));
					return;
				}

			}

			// Generate name for PBS output files
			final String file_base_name = String.format("%s/jobagent_%s_%d", Functions.resolvePathWithEnv(log_folder_path),
					config.get("host_host"), timestamp);
			out_cmd = String.format("#PBS -o %s.out\n", file_base_name);
			err_cmd = String.format("#PBS -e %s.err\n", file_base_name);

		}

		String submit_cmd = out_cmd + err_cmd;
		submit_cmd += "#PBS -V \n";

		// Name must be max 15 characters long
		final String name = String.format("jobagent_%d", timestamp % 1000000L);
		submit_cmd += String.format("#PBS -N %s%n", name);

		// Stage jobagent startup script to PBS node if no "alien_not_stage_files" declared
		if (stageIn) {
			Pattern pattern = Pattern.compile("^.*/([^/]*)$");
			Matcher matcher = pattern.matcher(script);

			if (matcher.find()) {
				String stagewn = matcher.group(1);
				submit_cmd += String.format("#PBS -W stagein=%s@%s:%s\n", stagewn, config.get("ce_host"), script);
			}
			else
				this.logger.log(Level.WARNING, "Unable to use stage in. Issue with script path?");

		}

		this.logger.info(submit_cmd);

		submit_cmd += script + "\n";
		String temp_file_cmd = this.submitCmd + " <<EOF\n" + submit_cmd + "EOF";
		final ExitStatus exitStatus = executeCommand(temp_file_cmd);
		final List<String> output = getStdOut(exitStatus);

		for (final String line : output) {
			final String trimmed_line = line.trim();
			logger.info(trimmed_line);
		}

	}

	public int getStatus(String status) {
		int numberedStatus = 0;
		final ExitStatus exitStatus = executeCommand(statusCmd);
		final List<String> output_list = getStdOut(exitStatus);

		if (exitStatus.getExecutorFinishStatus() != ExecutorFinishStatus.NORMAL)
			return -1;

		for (String output_line : output_list) {
			String[] line = output_line.trim().split("\\s+");
			for (String job_line : line) {
				if (job_line.equals("Q") || job_line.equals(status)) {
					numberedStatus++;
				}
			}
		}
		return numberedStatus;
	}

	@Override
	public int getNumberActive() {
		return getStatus("R");
	}

	@Override
	public int getNumberQueued() {
		return getStatus("Q");
	}

	@Override
	public int kill() {
		// Not implemented. Is it needed?
		return 0;
	}

	@SuppressWarnings("unchecked")
	private String readArgFromLdap(final String argToRead) {
		if (!config.containsKey(argToRead) || config.get(argToRead) == null)
			return "";
		else if ((config.get(argToRead) instanceof TreeSet)) {
			final StringBuilder args = new StringBuilder();
			for (final String arg : (TreeSet<String>) config.get(argToRead)) {
				if (!arg.equalsIgnoreCase("alien_not_stage_files"))
					args.append(arg).append(' ');
				else
					stageIn = false;

			}
			return args.toString();
		}
		else {
			String arg = config.get(argToRead).toString();
			if (!arg.equalsIgnoreCase("alien_not_stage_files"))
				return arg;
			else
				stageIn = false;
			return "";
		}
	}
}
