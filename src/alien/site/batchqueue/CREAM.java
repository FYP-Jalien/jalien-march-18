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
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;

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
	private TreeSet<String> _env_from_config;
	private File _temp_file;
	private File _job_id_file;
	private String _last_job_id;
	private ArrayList<ArrayList<String>> _ce_clusterstatus = new ArrayList<ArrayList<String>>();
	
	private enum InfoMode {
		SUM,
		MIN,
		MAX;
	}

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
		
		this._env_from_config = (TreeSet<String>) this.config.get("ce_environment");		// this type is checked
		if (!this.readCEList()) {
			logger.severe("Error reading CE list.");
			return;
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
	
	private boolean readCEList() {		// Form the CE lists
		boolean has_field = false;
		String CE_LCGCE_str = "test-cream-el6.cern.ch:8443/cream-pbs-alice";		// Current default
		for( String env_field : _env_from_config) {
			if( env_field.contains("CE_LCGCE") ){
				CE_LCGCE_str = env_field.split("=")[1];
				has_field = true;
				break;
			}
		}
		if (!has_field) {
			logger.severe("No CE list defined in environment from LDAP.");
			return false;
		}
		
		Matcher matcher = Pattern.compile("\\((.*?)\\)").matcher(CE_LCGCE_str);		// Find groups
		while(matcher.find()) {
			String group_str = matcher.group(1);
			CE_LCGCE_str = CE_LCGCE_str.replace("(" + group_str + ")", "");
			String[] splitted_group_str = group_str.split(",");
			ArrayList<String> arr = new ArrayList<String>();
			for (String sub : splitted_group_str) {
				arr.add(sub);
			}
			_ce_clusterstatus.add(arr);
		}
		String[] splitted_CE_LCGCE_str = CE_LCGCE_str.split(",");					// Find complementary
		for (String sub : splitted_CE_LCGCE_str) {
			if (sub.length() > 0) {
				ArrayList<String> arr = new ArrayList<String>();
				arr.add(sub);
				_ce_clusterstatus.add(arr);
			}
		}
		return true;
	}
	
	private String generateStartAgent(String command) {		// TODO: wip
		return "StartAgent dummy";
	}
	
	private String translateRequirements(String ca, String bdiireq) {		// TODO: wip
		return "requirements dummy";
	}
	
	private String selectCE() {
//		We need to imitate the load-balancing over several such endpoints,
//		which can be grouped e.g. like this:
//
//		     (host-1:8443/cream-xyz-abc,host-2:8443/cream-xyz-abc),host-3:8443/cream-foo-bar
//
//		Hosts _within_ brackets are _equivalent_: they cover the same "subcluster"
//		in the same batch system and their job numbers need to be _averaged_
//		(or you just check one of them, as CREAM.pm does today).
//
//		We need to spread the job submissions evenly over any of those hosts
//		that currently are available.
//
//		Hosts _outside_ brackets are _complementary_: your code needs to try
//		and fill _all_ of them round-robin, up to the limits for the total number
//		of jobs and the number of waiting jobs, configured in LDAP.
		
		String selected_ce = "test-cream-el6.cern.ch:8443/cream-pbs-alice";		// default
		int lowest_avg = Integer.MAX_VALUE;
		
		String[] items = {"GlueCEStateRunningJobs", "GlueCEStateWaitingJobs"};
		for (int i = 0; i < _ce_clusterstatus.size(); i++) {		// find the lowest average in _ce_clusterstatus
			int sum = 0;
			int count = 0;
			boolean can_submit = false;
			int min_jobs = Integer.MAX_VALUE;
			String smallest_ce = "";
			ArrayList<String> ce_group = _ce_clusterstatus.get(i);
			for (String ce : ce_group) {
				HashMap<String, String> query_results = this.queryBDII(ce, "GlueCEUniqueID=" + ce, items);
				int active_jobs = 0;
				count++;
				active_jobs += Integer.parseInt((String)query_results.get("GlueCEStateWaitingJobs")) + 
						Integer.parseInt((String)query_results.get("GlueCEStateRunningJobs"));
				sum += active_jobs;
				if (Integer.parseInt(query_results.get("GlueCEStateWaitingJobs")) < (Integer)config.get("ce_maxqueuedjobs")){
					if (active_jobs < min_jobs) {
						min_jobs = active_jobs;
						smallest_ce = ce;
					}
					can_submit = true;
				}
			}
			int avg = sum/count;
			if ((avg < lowest_avg) && can_submit) {
				lowest_avg = avg;
				selected_ce = smallest_ce;
			}
		}
		
		return selected_ce;
	}
	
	private HashMap<String, String> queryBDII(String ce, String query, String[] items) {		// TODO: wip
		if (query == "") {
			String GlueVOViewLocalID = (String)config.get("LCGVO");
			query = "GlueVOViewLocalID=" + GlueVOViewLocalID.toLowerCase() + ",GlueCEUniqueID=" + ce;
		}

		this.logger.info("Querying CE for: " + items);
		this.logger.info("DN string: " + query);

		String IS = "ldap://" + (String)config.get("ce_host") + ":2170/mds-vo-name=resource,o=grid";
		if (config.containsKey("CE_SITE_BDII")) {
			IS = (String)config.get("CE_SITE_BDII");
		}
		
//		Code adapted from LDAPHelper
		Map<String, String> defaultEnv = new HashMap<>();
		defaultEnv.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
		defaultEnv.put("com.sun.jndi.ldap.read.timeout", "30000");
		defaultEnv.put("com.sun.jndi.ldap.connect.timeout", "10000");
		defaultEnv.put("com.sun.jndi.ldap.connect.pool.maxsize", "50");
		defaultEnv.put("com.sun.jndi.ldap.connect.pool.prefsize", "5");
		defaultEnv.put("com.sun.jndi.ldap.connect.pool.timeout", "120000");
		HashMap<String, String> result = new HashMap<String, String>();

		try {
			final Hashtable<String, String> env = new Hashtable<>();
			env.putAll(defaultEnv);
			env.put(Context.PROVIDER_URL, IS);

			final DirContext context = new InitialDirContext(env);

			try {
				final SearchControls ctrl = new SearchControls();
				ctrl.setSearchScope(SearchControls.SUBTREE_SCOPE);

				final NamingEnumeration<SearchResult> enumeration = context.search("", query, ctrl);

				while (enumeration.hasMore()) {
					final SearchResult entry = enumeration.next();

					final Attributes attribs = entry.getAttributes();

					if (attribs == null)
						continue;

					for (String item : items) {
						final BasicAttribute ba = (BasicAttribute) attribs.get(item);
						result.put(ba.getID(), (String)ba.get());
					}
				}
			} finally {
				context.close();
			}

		} catch (final NamingException ne) {
			ne.printStackTrace();
		}

		return result;
	}
	
	private int getCEInfo(InfoMode mode, String[] items) {
		boolean gotResults = false;
		int results = 0;
		ArrayList<HashMap<String, String>> values = new ArrayList<HashMap<String, String>>();
		this.logger.info("Querying CEs, mode: " + mode.name() + ", requested info: " + items);
		
		for (ArrayList<String> cluster : this._ce_clusterstatus) {
			for (String ce : cluster) {
				this.logger.info("Querying for $CE");
				String lcgvo = (String)config.get("LCGVO");
				lcgvo = lcgvo.toLowerCase();
				HashMap<String, String> res = this.queryBDII(ce, "GlueVOViewLocalID=" + lcgvo + ",GlueCEUniqueID=" + ce, items);
				this.logger.info("getCEInfo() returned results: " + res);
				if (res != null) {
					HashMap<String, String> ce_results = new HashMap<String, String>();
					for (String item : items) {
						if (res.get(item) == "444444") {
							this.logger.warning("Query for " + ce + " gave " + item + "=444444.");
							this.logger.info("Query for " + ce + " gave " + item + "=444444.");
							continue;
						}
						if (res.containsKey(item)) {
							ce_results.put(item, res.get(item));
							values.add(ce_results);
							gotResults = true;
						}
					}
				}else {
					this.logger.warning("Query for " + ce + " failed.");
					this.logger.info("Query for " + ce + " failed.");
					continue;
				}
				break;
			}
		}
		
		if (!gotResults) {
			this.logger.severe("getCEInfo() got no valid answer from any CE");
			this.logger.info("getCEInfo() got no valid answer from any CE");
			return 0;
		}
		
		ArrayList<Integer> value_int_array = new ArrayList<Integer>();
		for (HashMap<String, String> ce_values : values) {
			for (String item : items) {
				if (ce_values.containsKey(item)) {
					value_int_array.add(Integer.parseInt(ce_values.get(item)));
				}
			}
		}
		
		switch (mode) {
		case SUM:
			for (Integer i : value_int_array) results += i;
			break;
		case MIN:
			results = Collections.min(value_int_array);
			break;
		case MAX:
			results = Collections.max(value_int_array);
			break;
		}
		
		return results;
	}
	
	private String wrapSubmit(File log_file, HashMap<String, String> arguments) {
		String job_id = "";
		
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
		Pattern comment_pattern = Pattern.compile("https:\\/\\/[A-Za-z0-9.-]*:8443\\/cream\\d+");
		Matcher comment_matcher = comment_pattern.matcher(link_from_output);
		if(!comment_matcher.matches()) {
			this.logger.info("Could not find link in submission output. JobID unknown.");
			return "";
		}
		job_id = link_from_output;
		
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
		
		// pick a CE from the list (this._ce_clusterstatus)
		String selected_ce = this.selectCE();
		if (selected_ce == "") {
			this.logger.severe("No suitable CE found for submission!");
			this.logger.info("No more slots in the queues?");
			return;
		}
		
		HashMap<String, String> arguments = new HashMap<String, String>();
		if (this.config.containsKey("CE_SUBMITARG_LIST")) {
			arguments = (HashMap<String, String>)this.config.get("CE_SUBMITARG_LIST");		// TODO: Need to check type
		}
		
		arguments.put("-r", selected_ce);
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
		
		String job_id = this.wrapSubmit(submit_log_file, arguments);
		this._last_job_id = job_id;
		
//		  # //Comment from Perl//
//		  #
//		  # Return success even when this job submission failed,
//		  # because further job submissions may still work,
//		  # in particular when another CE gets picked.
//		  # Always decrement the number of slots,
//		  # to ensure the submission loop will terminate.
//		  #
		
		if (job_id == "") {
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
		int result = -1;
		String[] items = {"GlueCEStateRunningJobs", "GlueCEStateWaitingJobs"};
		result = this.getCEInfo(InfoMode.SUM, items);
		if (result == -1) {
			this.logger.info("Could not get number of running/waiting jobs");
			return 0;
		}
		this.logger.info("JobAgents running, waiting total: " + result);
		return result;
	}

	@Override
	public int getNumberQueued() {
		int result = -1;
		String[] items = {"GlueCEStateWaitingJobs"};
		result = this.getCEInfo(InfoMode.MIN, items);
		if (result == -1) {
			this.logger.info("Could not get number of waiting jobs");
			return 0;
		}
		this.logger.info("JobAgents waiting: " + result);
		return result;
	}

	@Override
	public int kill() {
		return 0;
	}

}
