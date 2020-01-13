package alien.site.batchqueue;

import java.io.BufferedReader;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;
import java.util.TreeSet;
import java.util.ArrayList;
import java.util.List;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.FileNotFoundException;
import alien.test.utils.Functions;

public class SLURM extends BatchQueue {

	private Map<String, String> environment;
	private TreeSet<String> envFromConfig;
	private String submitCmd;
	private String submitArgs = "";
	private String killCmd;
	private String killArgs = "";
	private String statusCmd;
	private String statusArgs = "";
	private String runArgs = "";
	private String user;
	private File temp_file;

	@SuppressWarnings("unchecked")
	public SLURM(HashMap<String, Object> conf, Logger logr) {
		String statusOpts;
		this.environment = System.getenv();
		this.config = conf;
		this.logger = logr;

		this.logger.info("This VO-Box is " + config.get("ALIEN_CM_AS_LDAP_PROXY") + ", site is " + config.get("site_accountname"));

		try {
			this.envFromConfig = (TreeSet<String>) this.config.get("ce_environment");
		} catch (ClassCastException e) {
			logger.severe(e.toString());
		}

		this.temp_file = null;

		// Get SLURM
		this.submitCmd = (config.get("CE_SUBMITCMD") != null ? (String) config.get("CE_SUBMITCMD") : "sbatch");
		this.killCmd = (config.get("CE_KILLCMD") != null ? (String) config.get("CE_KILLCMD") : "scancel");
		this.statusCmd = (config.get("CE_STATUSCMD") != null ? (String) config.get("CE_STATUSCMD") : "squeue");

		// Get args from the environment
		if (envFromConfig != null) {
			for (String env_field : envFromConfig) {
				if (env_field.contains("SUBMIT_ARGS")) {
					this.submitArgs = env_field.split("=")[1];
				}
				if (env_field.contains("STATUS_ARGS")) {
					this.statusArgs = env_field.split("=")[1];
				}
				if (env_field.contains("RUN_ARGS")) {
					this.runArgs = env_field.split("=")[1];
				}
				if (env_field.contains("KILL_ARGS")) {
					this.runArgs = env_field.split("=")[1];
				}
			}
		}
		if (environment.get("SUBMIT_ARGS") != null)
			this.submitArgs = environment.get("SUBMIT_ARGS");
		if (environment.get("STATUS_ARGS") != null)
			this.statusArgs = environment.get("STATUS_ARGS");
		if (environment.get("RUN_ARGS") != null)
			this.runArgs = environment.get("RUN_ARGS");
		if (environment.get("KILL_ARGS") != null)
			this.killArgs = environment.get("RUN_ARGS");

		user = environment.get("USER");

		statusOpts = "-h -o \"%i %t %j\" -u " + user;

		statusCmd = statusCmd + " " + statusOpts;

		killArgs += " --ctld -Q -u " + user;

		killCmd = killCmd + killArgs;
	}


	public void submit(final String script) {

		this.logger.info("Submit SLURM");

		DateFormat date_format = new SimpleDateFormat("yyyy-MM-dd");
		String current_date_str = date_format.format(new Date());

		// Create log directory
		String host_logdir = (environment.get("SLURM_LOG_PATH") != null ? environment.get("SLURM_LOG_PATH") : (String) config.get("host_logdir"));
		String log_folder_path = String.format("%s/%s", host_logdir, current_date_str);
		File log_folder = new File(log_folder_path);
		if (!(log_folder.exists()) || !(log_folder.isDirectory())) {
			try {
				log_folder.mkdir();
			} catch (SecurityException e) {
				this.logger.info(String.format("[SLURM] Couldn't create log folder: %s", log_folder_path));
				e.printStackTrace();
			}
		}

		// Generate name for SLURM output files
		Long timestamp = System.currentTimeMillis();
		String file_base_name = String.format("%s/jobagent_%s_%d",
				Functions.resolvePathWithEnv(log_folder_path), config.get("host_host"),
				timestamp);

		// Put generate output options
		String out_cmd = "";
		String err_cmd = "";
		String name = String.format("jobagent_%s_%d", this.config.get("host_host"),
				timestamp);
		File enable_sandbox_file = new File(environment.get("TMP") + "/enable-sandbox");
		if (enable_sandbox_file.exists() || (this.logger.getLevel() != null)) {
			out_cmd = String.format("#SBATCH -o %s.out", file_base_name);
			err_cmd = String.format("#SBATCH -e %s.err", file_base_name);
		} else {
			out_cmd = "#SBATCH -o /dev/null";
			err_cmd = "#SBATCH -e /dev/null";
		}

		// Build SLURM script
		String submit_cmd = "#!/bin/bash\n";

		// Create JobAgent workdir
		String workdir_path = String.format("%s/jobagent_%s_%d", config.get("host_workdir"),
				config.get("host_host"), timestamp);
		final String workdir_path_resolved = Functions.resolvePathWithEnv(workdir_path);
		File workdir_file = new File(workdir_path_resolved);
		workdir_file.mkdir();

		submit_cmd += String.format("#SBATCH -J %s\n", name);
		submit_cmd += String.format("#SBATCH -D %s\n", workdir_path_resolved);
		if (((String) config.get("host_host")).contains("cori")) {
			submit_cmd += "#SBATCH -C haswell\n";
		} else {
			submit_cmd += "#SBATCH --reservation=Zach\n";
		}
		submit_cmd += "#SBATCH -N 1\n";
		submit_cmd += "#SBATCH -n 1\n";
		submit_cmd += "#SBATCH --no-requeue\n";
		submit_cmd += String.format("%s\n%s\n", out_cmd, err_cmd);

		/**
		 * We need the runArgs here in order to start using shifter
		 */
		submit_cmd += "srun " + runArgs + " ";
		submit_cmd += script + "\n";


		// Create temp file
		if (this.temp_file != null) {
			List<String> temp_file_lines = null;
			try {
				temp_file_lines = Files.readAllLines(Paths.get(this.temp_file.getAbsolutePath()), StandardCharsets.UTF_8);
			} catch (IOException e1) {
				this.logger.info("Error reading old temp file");
				e1.printStackTrace();
			} finally {
				if (temp_file_lines != null) {
					String temp_file_lines_str = "";
					for (String line : temp_file_lines) {
						temp_file_lines_str += line + '\n';
					}
					if (!temp_file_lines_str.equals(submit_cmd)) {
						if (!this.temp_file.delete()) {
							this.logger.info("Could not delete temp file");
						}
						try {
							this.temp_file = File.createTempFile("slurm-submit.", ".sh");
						} catch (IOException e) {
							this.logger.info("Error creating temp file");
							e.printStackTrace();
							return;
						}
					}
				}
			}
		}
		else {
			try {
				this.temp_file = File.createTempFile("slurm-submit.", ".sh");
			} catch (IOException e) {
				this.logger.info("Error creating temp file");
				e.printStackTrace();
				return;
			}
		}

		this.temp_file.setReadable(true);
		this.temp_file.setExecutable(true);

		try (PrintWriter out = new PrintWriter(this.temp_file.getAbsolutePath())) {
			out.println(submit_cmd);
			out.close();
		} catch (FileNotFoundException e) {
			this.logger.info("Error writing to temp file");
			e.printStackTrace();
		}

		String temp_file_cmd = String.format("%s %s %s", this.submitCmd, this.submitArgs, this.temp_file.getAbsolutePath());
		ArrayList<String> output = executeCommand(temp_file_cmd);
		for (String line : output) {
			String trimmed_line = line.trim();
			this.logger.info(trimmed_line);
		}

	}

	/**
	 * @return number of currently active jobs
	 */
	public int getNumberActive() {
		String status = "R,S,CG";
		ArrayList<String> output_list = this.executeCommand(statusCmd + " -t " + status + " " + statusArgs);
		if (output_list == null) {
			this.logger.info("Couldn't retrieve the number of active jobs.");
			return -1;
		}
		return output_list.size();
	}

	/**
	 * @return number of queued jobs
	 */
	public int getNumberQueued() {
		String status = "PD,CF";
		ArrayList<String> output_list = this.executeCommand(statusCmd + " -t " + status + " " + statusArgs);
		if (output_list == null) {
			this.logger.info("Couldn't retrieve the number of queued jobs.");
			return -1;
		}
		return output_list.size();
	}

	public int kill() {
		ArrayList<String> kill_cmd_output = null;
		try {
			kill_cmd_output = executeCommand(this.killCmd);
		} catch (Exception e) {
			this.logger.info(String.format("[SLURM] Prolem while executing command: %s", this.killCmd));
			e.printStackTrace();
			return -1;
		} finally {
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
}
