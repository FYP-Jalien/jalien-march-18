package alien.site.batchqueue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
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

import alien.site.Functions;
import lazyj.Utils;

import lia.util.process.ExternalProcess.ExitStatus;
import lia.util.process.ExternalProcess.ExecutorFinishStatus;

/**
 *
 */
public class SLURM extends BatchQueue {

	private final Map<String, String> environment;
	private TreeSet<String> envFromConfig;
	private final String submitCmd;
	private String submitArgs = "";
	private String killCmd;
	private String killArgs = "";
	private String statusCmd;
	private String statusArgs = "";
	private String runArgs = "";
	private final String user;
	private File temp_file;

	/**
	 * @param conf
	 * @param logr
	 */
	@SuppressWarnings("unchecked")
	public SLURM(final HashMap<String, Object> conf, final Logger logr) {
		String statusOpts;
		this.environment = System.getenv();
		this.config = conf;
		this.logger = logr;

		this.logger.info("This VO-Box is " + config.get("ALIEN_CM_AS_LDAP_PROXY") + ", site is " + config.get("site_accountname"));

		try {
			this.envFromConfig = (TreeSet<String>) this.config.get("ce_environment");
		}
		catch (final ClassCastException e) {
			logger.severe(e.toString());
		}

		this.temp_file = null;

		// Get SLURM
		this.submitCmd = (String) config.getOrDefault("ce_submitcmd", "sbatch");
		this.killCmd = (String) config.getOrDefault("ce_killcmd", "scancel");
		this.statusCmd = (String) config.getOrDefault("ce_statuscmd", "squeue");

		this.submitArgs = readArgFromLdap("ce_submitarg");
		this.killArgs = readArgFromLdap("ce_killarg");
		this.runArgs = readArgFromLdap("ce_runarg");
		this.statusArgs = readArgFromLdap("ce_statusarg");

		// Get args from the environment
		if (envFromConfig != null) {
			for (final String env_field : envFromConfig) {
				if (env_field.contains("SUBMIT_ARGS")) {
					this.submitArgs = getValue(env_field, "SUBMIT_ARGS", this.submitArgs);
				}
				if (env_field.contains("STATUS_ARGS")) {
					this.statusArgs = getValue(env_field, "STATUS_ARGS", this.statusArgs);
				}
				if (env_field.contains("RUN_ARGS")) {
					this.runArgs = getValue(env_field, "RUN_ARGS", this.runArgs);
				}
				if (env_field.contains("KILL_ARGS")) {
					this.killArgs = getValue(env_field, "KILL_ARGS", this.killArgs);
				}
			}
		}

		this.submitArgs = environment.getOrDefault("SUBMIT_ARGS", submitArgs);
		this.statusArgs = environment.getOrDefault("STATUS_ARGS", this.statusArgs);
		this.runArgs = environment.getOrDefault("RUN_ARGS", this.runArgs);
		this.killArgs = environment.getOrDefault("RUN_ARGS", this.killArgs);

		user = environment.get("USER");

		statusOpts = "-h -o \"%i %t %j\" -u " + user;

		statusCmd = statusCmd + " " + statusOpts;

		killArgs += " --ctld -Q -u " + user;
		// killArgs += " --ctld -Q";

		killCmd = killCmd + killArgs;
	}

	@Override
	public void submit(final String script) {

		this.logger.info("Submit SLURM");

		final DateFormat date_format = new SimpleDateFormat("yyyy-MM-dd");
		final String current_date_str = date_format.format(new Date());
		final Long timestamp = Long.valueOf(System.currentTimeMillis());

		// Logging setup
		String out_cmd = "#SBATCH -o /dev/null";
		String err_cmd = "#SBATCH -e /dev/null";
		final String name = String.format("jobagent_%s_%d", this.config.get("host_host"), timestamp);

		// Check if we can use SLURM_LOG_PATH instead of sending to /dev/null
		final String host_logdir = environment.getOrDefault("SLURM_LOG_PATH", config.get("host_logdir") != null ? config.get("host_logdir").toString() : null);

		if (host_logdir != null) {
			final String log_folder_path = String.format("%s/%s", host_logdir, current_date_str);
			final File log_folder = new File(log_folder_path);
			if (!(log_folder.exists()) || !(log_folder.isDirectory())) {
				try {
					log_folder.mkdir();
				}
				catch (final SecurityException e) {
					this.logger.info(String.format("[SLURM] Couldn't create log folder: %s", log_folder_path));
					e.printStackTrace();
				}
			}

			// Generate name for SLURM output files
			final String file_base_name = String.format("%s/jobagent_%s_%d", Functions.resolvePathWithEnv(log_folder_path),
					config.get("host_host"), timestamp);

			// Put generate output options
			final File enable_sandbox_file = new File(environment.get("TMP") + "/enable-sandbox");
			if (enable_sandbox_file.exists() || (this.logger.getLevel() != null)) {
				out_cmd = String.format("#SBATCH -o %s.out", file_base_name);
				err_cmd = String.format("#SBATCH -e %s.err", file_base_name);
			}
		}

		// Build SLURM script
		String submit_cmd = "#!/bin/bash\n";

		// Create JobAgent workdir
		final String workdir_path = String.format("%s/jobagent_%s_%d", config.get("host_workdir"),
				config.get("host_host"), timestamp);
		final String workdir_path_resolved = Functions.resolvePathWithEnv(workdir_path);
		final File workdir_file = new File(workdir_path_resolved);
		workdir_file.mkdir();

		submit_cmd += String.format("#SBATCH -J %s%n", name);
		// submit_cmd += String.format("#SBATCH -D %s\n", workdir_path_resolved);
		submit_cmd += String.format("#SBATCH -D /tmp%n");
		submit_cmd += "#SBATCH -N 1\n";
		submit_cmd += "#SBATCH -n 1\n";
		submit_cmd += "#SBATCH --no-requeue\n";
		submit_cmd += String.format("%s%n%s%n", out_cmd, err_cmd);

		String scriptContent;
		try {
			scriptContent = Files.readString(Paths.get(script));
		}
		catch (final IOException e2) {
			this.logger.log(Level.WARNING, "Error reading agent startup script!", e2);
			return;
		}

		final String encodedScriptContent = Utils.base64Encode(scriptContent.getBytes()).replaceAll("(\\w{76})", "$1\n");

		submit_cmd += "cat<<__EOF__ | base64 -d > " + script + "\n";
		submit_cmd += encodedScriptContent;
		submit_cmd += "\n__EOF__\n";
		submit_cmd += "chmod a+x " + script + "\n";
		submit_cmd += "srun " + runArgs + " " + script + "\n";

		submit_cmd += "rm " + script;

		if (this.temp_file != null && (!this.temp_file.exists() || !this.temp_file.canExecute() || this.temp_file.length() == 0)) {
			if (!this.temp_file.delete())
				logger.log(Level.INFO, "Invalid previous file: " + this.temp_file.getAbsolutePath());

			this.temp_file = null;
		}

		if (this.temp_file != null) {
			List<String> temp_file_lines = null;
			boolean keep = false;

			try {
				temp_file_lines = Files.readAllLines(Paths.get(this.temp_file.getAbsolutePath()), StandardCharsets.UTF_8);
			}
			catch (final IOException e1) {
				this.logger.log(Level.INFO, "Error reading old temp file " + this.temp_file.getAbsolutePath(), e1);
			}
			finally {
				if (temp_file_lines != null) {
					final StringBuilder temp_file_lines_str = new StringBuilder();
					for (final String line : temp_file_lines)
						temp_file_lines_str.append(line).append('\n');

					keep = temp_file_lines_str.toString().equals(submit_cmd);
				}
			}

			if (!keep) {
				if (!this.temp_file.delete())
					this.logger.info("Could not delete temp file " + this.temp_file.getAbsolutePath());

				this.temp_file = null;
			}
		}

		// Create temp file
		if (this.temp_file == null) {
			try {
				this.temp_file = File.createTempFile("slurm-submit.", ".sh");
			}
			catch (final IOException e) {
				this.logger.info("Error creating temp file");
				e.printStackTrace();
				return;
			}

			this.temp_file.setReadable(true);
			this.temp_file.setExecutable(true);

			try (PrintWriter out = new PrintWriter(this.temp_file.getAbsolutePath())) {
				out.println(submit_cmd);
			}
			catch (final FileNotFoundException e) {
				this.logger.info("Error writing to temp file");
				e.printStackTrace();
			}
		}

		final String cmd = "cat " + this.temp_file.getAbsolutePath() + " | " + this.submitCmd + " " + this.submitArgs;
		final ExitStatus exitStatus = executeCommand(cmd);
		final ArrayList<String> output = getStdOut(exitStatus);
		for (final String line : output) {
			final String trimmed_line = line.trim();
			this.logger.info(trimmed_line);
		}
	}

	/**
	 * @return number of currently active jobs
	 */
	@Override
	public int getNumberActive() {
		final String status = "R,S,CG";
		final ExitStatus exitStatus = executeCommand(statusCmd + " -t " + status + " " + statusArgs);
		final ArrayList<String> output_list = getStdOut(exitStatus);

		if (exitStatus.getExecutorFinishStatus() != ExecutorFinishStatus.NORMAL)
			return -1;

		return output_list.size();
	}

	/**
	 * @return number of queued jobs
	 */
	@Override
	public int getNumberQueued() {
		final String status = "PD,CF";
		final ExitStatus exitStatus = executeCommand(statusCmd + " -t " + status + " " + statusArgs);
		final ArrayList<String> output_list = getStdOut(exitStatus);

		if (exitStatus.getExecutorFinishStatus() != ExecutorFinishStatus.NORMAL)
			return -1;

		return output_list.size();
	}

	@Override
	public int kill() {
		final ExitStatus exitStatus;
		ArrayList<String> kill_cmd_output = null;
		try {
			exitStatus = executeCommand(this.killCmd);
			kill_cmd_output = getStdOut(exitStatus);
		}
		catch (final Exception e) {
			this.logger.info(String.format("[SLURM] Prolem while executing command: %s", this.killCmd));
			e.printStackTrace();
			return -1;
		}
		finally {
			if (kill_cmd_output != null) {
				this.logger.info("Kill cmd output:\n");
				for (String line : kill_cmd_output) {
					line = line.trim();
					this.logger.info(line);
				}
			}
		}

		if (temp_file != null && temp_file.exists()) {
			this.logger.info(String.format("Deleting temp file  %s after command.", this.temp_file.getAbsolutePath()));
			if (!temp_file.delete())
				this.logger.info(String.format("Could not delete temp file: %s", this.temp_file.getAbsolutePath()));
			else
				this.temp_file = null;
		}
		return 0;
	}

	@SuppressWarnings("unchecked")
	private String readArgFromLdap(final String argToRead) {
		if (!config.containsKey(argToRead) || config.get(argToRead) == null)
			return "";
		else if ((config.get(argToRead) instanceof TreeSet)) {
			final StringBuilder args = new StringBuilder();
			for (final String arg : (TreeSet<String>) config.get(argToRead)) {
				args.append(arg).append(' ');
			}
			return args.toString();
		}
		else {
			return config.get(argToRead).toString();
		}
	}
}