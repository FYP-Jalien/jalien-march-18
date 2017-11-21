package alien.site.batchqueue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import alien.log.LogUtils;
import alien.test.utils.Functions;
import lia.util.process.ExternalProcess.ExitStatus;
import utils.ProcessWithTimeout;

/**
 * @author mmmartin
 */
public class CREAM extends BatchQueue {

	private boolean updateClassad = false;
	private HashMap<String, String> commands = new HashMap<>();
	private Map<String, String> environment = System.getenv();
	private File _temp_file;
	private File _job_id_file;
	
	private int _last_job_id;

	/**
	 * @param conf
	 * @param logr
	 *            logger
	 */
	public CREAM(HashMap<String, Object> conf, Logger logr) {
		this.config = conf;
		logger = logr;
		String host_logdir_resolved = Functions.resolvePathWithEnv((String) config.get("host_logdir"));
		logger = LogUtils.redirectToCustomHandler(logger, host_logdir_resolved + "JAliEn." + (new Timestamp(System.currentTimeMillis()).getTime() + ".out"));
		this._job_id_file = new File(host_logdir_resolved + "/CE.db/JOBIDS");
		if (!this._job_id_file.exists()) {
			try {
				this._job_id_file.createNewFile();
			} catch (IOException e) {
				logger.info("Could not create a file for JobId storage.");
				e.printStackTrace();
			}
		}

		logger.info("This VO-Box is " + config.get("ALIEN_CM_AS_LDAP_PROXY") + ", site is " + config.get("site_accountName"));

		// Environment for submit

//		String fix_env = "";
//		if (environment.containsKey("LD_LIBRARY_PATH")) {
//			fix_env += environment.get("LD_LIBRARY_PATH");
//			if (environment.containsKey("JALIEN_ROOT")) {
//				fix_env = fix_env.replaceAll(environment.get("JALIEN_ROOT") + "+[^:]+:?", "");
//				fix_env = "unset X509_CERT_DIR; LD_LIBRARY_PATH=" + fix_env;
//			}
//		}

		// Commands
		commands.put("submitcmd", (config.containsKey("ce_submitcmd") ? (String) config.get("ce_submitcmd") : "glite-ce-job-submit")); // need fix_env?
		commands.put("statuscmd", (config.containsKey("ce_statuscmd") ? (String) config.get("ce_statuscmd") : "glite-ce-job-status"));
		commands.put("killcmd", (config.containsKey("ce_killcmd") ? (String) config.get("ce_killcmd") : "glite-ce-job-cancel"));
		commands.put("delegationcmd", (config.containsKey("ce_delegationcmd") ? (String) config.get("ce_delegationcmd") : "glite-ce-delegate-proxy"));
	}
	
	private String generateStartAgent(String command) {		// TODO: wip
		return "StartAgent dummy";
	}
	
	private String translateRequirements(String ca, String bdiireq) {		// TODO: wip
		return "requirements dummy";
	}
	
	private int wrapSubmit(File log_file, HashMap<String, String> arguments) {
		int job_id = -1;
		
		ArrayList<String> submit_cmd = new ArrayList<String>();
		submit_cmd.add(this.commands.get("submitcmd"));
		submit_cmd.add("--noint");
		submit_cmd.add("--nomsg");
		submit_cmd.add("--logfile");
		submit_cmd.add(log_file.getAbsolutePath());
		for (String key : arguments.keySet()) {
			submit_cmd.add(key);
			String value = arguments.get(key);
			if (value != null) {
				submit_cmd.add(value);
			}
		}
		submit_cmd.add(this._temp_file.getAbsolutePath());
		submit_cmd.add("2>&1");
		
		ArrayList<String> output = this.executeCommand(submit_cmd);
		String link_from_output = "";
		for (String line : output) {
			if (line.contains("https:")) {
				link_from_output = line;
				break;
			}
		}
		Pattern comment_pattern = Pattern.compile("https:\\/\\/[A-Za-z0-9.-]*:8443\\/CREAM\\d+");
		Matcher comment_matcher = comment_pattern.matcher(link_from_output);
		if(!comment_matcher.matches()) {
			this.logger.info("Could not find link in submission output. JobID unknown.");
			return -1;
		}
		String[] split_link = link_from_output.split("/");
		link_from_output = split_link[split_link.length - 1];
		link_from_output = link_from_output.replaceAll("\\D+","");		// Leave only the id at the end
		job_id = Integer.parseInt(link_from_output);
		
		return job_id;
	}
	
	// Same as HTCONDOR method atm
	private ArrayList<String> executeCommand(ArrayList<String> cmd) {
		ArrayList<String> proc_output = new ArrayList<String>();
		try {
			ArrayList<String> cmd_full = new ArrayList<String>();
			cmd_full.add("/bin/bash");
			cmd_full.add("-c");
			cmd_full.addAll(cmd);
			final ProcessBuilder proc_builder = new ProcessBuilder(cmd_full);

			Map<String, String> env = proc_builder.environment();
			env.clear();
			
			final HashMap<String, String> additional_env_vars = new HashMap<String, String>();
			additional_env_vars.put("X509_USER_PROXY", System.getenv("X509_USER_PROXY"));
			additional_env_vars.put("LD_LIBRARY_PATH", System.getenv("LD_LIBRARY_PATH"));
			additional_env_vars.put("PATH", System.getenv("PATH"));
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
		} catch (final Throwable t) {
			logger.log(Level.WARNING, String.format("Exception executing command: ", cmd), t);
		}
		this.logger.info(String.format("[HTCONDOR] Command output: %s", proc_output));
		return proc_output;
	}
	
	private void generateJDL(String ca, String command, String bdiireq) {
		String exe_file_path = this.generateStartAgent(command);
		String requirements = this.translateRequirements(ca, bdiireq);
		if ( exe_file_path == "" || exe_file_path == null)
		{
			logger.warning("[CREAM] Could not create StartAgent!");
			return;
		}
		
		File temp_output_dir = new File(this._temp_file.getParent() + "/job-output");
		if (!temp_output_dir.exists()) {
			if (!temp_output_dir.mkdir())
				logger.warning("[CREAM] The directory for the CREAM output cannot be created");
		}
		
		Long time_value = new Timestamp(System.currentTimeMillis()).getTime();
		
		String submit_cmd = "\\# JDL automatically generated by AliEn\n" + 
				"[\n" + 
				"Executable = \\\"/bin/sh\\\";\n" + 
				"Arguments = \\\"-x dg-submit." + Long.toString(time_value) + ".sh\\\";\n" + 
				"StdOutput = \\\"std.out\\\";\n" + 
				"StdError = \\\"std.err\\\";\n" + 
				"InputSandbox = {\\\"" + exe_file_path + "\\\"};\n" + 
				"Environment = {\n" + 
				"\\\"ALIEN_CM_AS_LDAP_PROXY=" + config.get("ALIEN_CM_AS_LDAP_PROXY") + "\\\",\n" + 
				"\\\"ALIEN_JOBAGENT_ID=" + ((Integer)this.config.get("CLUSTERMONITOR_PORT") + time_value) + "\\\",\n" + 
				"\\\"ALIEN_ORGANISATION=" + this.config.get("ALIEN_ORGANISATION") + "\\\",\n" + 
				"};\n";
		
		if (new File("$ENV{HOME}/enable-sandbox").exists()) {	// || this.logger.getLevel().intValue() > 0) {
			submit_cmd += "OutputSandbox = { \"std.err\" , \"std.out\" };\n";
			submit_cmd += "Outputsandboxbasedesturi = \"gsiftp://localhost\";\n";
		}
		
		if (requirements != "" && requirements != null) {
			submit_cmd += "Requirements = " + requirements + ";\n";
		}
		if (config.containsKey("CREAM_CEREQ")) {
			submit_cmd += "CeRequirements = " + config.get("CREAM_CEREQ") + ";\n";
		}
		submit_cmd += "]\n";
		
		if (this._temp_file != null) {
			List<String> temp_file_lines = null;
			try {
				temp_file_lines = Files.readAllLines(Paths.get(this._temp_file.getAbsolutePath()), StandardCharsets.UTF_8);
			} catch (IOException e1) {
				this.logger.info("Error reading old temp file");
				e1.printStackTrace();
			} finally {
				if(temp_file_lines != null) {
					String temp_file_lines_str = "";
					for (String line : temp_file_lines) {
						temp_file_lines_str += line + '\n';
					}
					if (temp_file_lines_str != submit_cmd) {
						if (!this._temp_file.delete()) {
							this.logger.info("Could not delete temp file");
						}
						try {
							this._temp_file = File.createTempFile("dg-submit.", ".jdl");
						} catch(IOException e) {
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
				this._temp_file = File.createTempFile("dg-submit.", ".jdl");
			} catch(IOException e) {
				this.logger.info("Error creating temp file");
				e.printStackTrace();
				return;
			}
		}
		
		this._temp_file.setReadable(true);
		this._temp_file.setExecutable(true);

		try(PrintWriter out = new PrintWriter( this._temp_file.getAbsolutePath() )){
		    out.println( submit_cmd );
		    out.close();
		} catch (FileNotFoundException e) {
			this.logger.info("Error writing to temp file");
			e.printStackTrace();
		}
	}

	@Override
	public void submit(final String script) {
		logger.info("Submit CREAM");
		long submission_start_time = new java.util.Date().getTime();
		
		// this._temp_file will now be created
		this.generateJDL("ca", script, "bdiiReq");		// TODO: find out correct parameters
		if (this._temp_file == null) {
			this.logger.warning("Could not generate JDL file. Exiting...");
			return;
		}
		
		HashMap<String, String> arguments = new HashMap<>();
		if (this.config.containsKey("CE_SUBMITARG_LIST")) {
			arguments = (HashMap<String, String>)this.config.get("CE_SUBMITARG_LIST");		// TODO: Need to check type
		}
		
		arguments.put("-D", (String)this.config.get("DELEGATION_ID"));
		logger.info("Submitting to CREAM with arguments: " + arguments);
		
		File submit_log_file;
		try {
			submit_log_file = File.createTempFile("job-submit.", ".log");
		} catch(IOException e) {
			this.logger.info("Error creating log file for submission.");
			e.printStackTrace();
			return;
		}
		
		int job_id = this.wrapSubmit(submit_log_file, arguments);
		this._last_job_id = job_id;
		
//		  # //Comment from Perl//
//		  #
//		  # Return success even when this job submission failed,
//		  # because further job submissions may still work,
//		  # in particular when another CE gets picked.
//		  # Always decrement the number of slots,
//		  # to ensure the submission loop will terminate.
//		  #
		
		if (job_id == -1) {
			return;
		}
		this.logger.info("Current Job ID: " + job_id);
		
		try(PrintWriter out = new PrintWriter( this._job_id_file.getAbsolutePath() )){
		    out.println( new java.util.Date().toString() + " | JobID: " + job_id + "\n" );
		    out.close();
		} catch (FileNotFoundException e) {
			this.logger.info("Error writing to JobId file");
			e.printStackTrace();
		}
		
		long submission_end_time = new java.util.Date().getTime();
		this.logger.info("Submission took " + ((submission_end_time - submission_start_time)/1000) + "seconds.");
		return;
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
