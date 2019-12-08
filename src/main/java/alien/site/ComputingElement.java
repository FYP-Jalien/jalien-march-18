package alien.site;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.api.Dispatcher;
import alien.api.JBoxServer;
import alien.api.taskQueue.GetNumberFreeSlots;
import alien.api.taskQueue.GetNumberWaitingJobs;
import alien.api.token.GetTokenCertificate;
import alien.api.token.TokenCertificateType;
import alien.config.ConfigUtils;
import alien.log.LogUtils;
import alien.monitoring.MonitorFactory;
import alien.shell.commands.JAliEnCOMMander;
import alien.site.batchqueue.BatchQueue;
import alien.test.utils.Functions;
import apmon.ApMon;
import apmon.ApMonException;

/**
 * @author mmmartin
 *
 */
public class ComputingElement extends Thread {

	/**
	 * ApMon sender
	 */
	static transient final ApMon apmon = MonitorFactory.getApMonSender();

	// Logger object
	static transient Logger logger = ConfigUtils.getLogger(ComputingElement.class.getCanonicalName());

	private final JAliEnCOMMander commander = JAliEnCOMMander.getInstance();

	// Config, env, classad
	private int port = 10000;
	private String site;
	private HashMap<String, Object> siteMap = new HashMap<>();
	private HashMap<String, Object> config = null;
	private HashMap<String, String> host_environment = null;
	private HashMap<String, String> ce_environment = null;
	private BatchQueue queue = null;

	/**
	 *
	 */
	public ComputingElement() {
		try {
			config = ConfigUtils.getConfigFromLdap();
			site = (String) config.get("site_accountname");
			getSiteMap();

			final String host_logdir_resolved = Functions.resolvePathWithEnv((String) config.get("host_logdir"));

			logger = LogUtils.redirectToCustomHandler(logger, host_logdir_resolved + "/CE");

			queue = getBatchQueue((String) config.get("ce_type"));

			if (config.containsKey("proxy_cache_file")) {
				int ttl = ((Integer) siteMap.get("TTL")).intValue();
				TokenFileGenerationThread tk = new TokenFileGenerationThread((String) config.get("proxy_cache_file"),
						this.logger, ttl);
				tk.start();
			}

		}
		catch (final Exception e) {
			logger.severe("Problem in construction of ComputingElement: " + e.toString());
		}
	}

	@Override
	public void run() {
		logger.log(Level.INFO, "Starting ComputingElement in " + config.get("host_host"));
		try {
			logger.info("Trying to start JBox");
			JBoxServer.startJBoxService();
			port = JBoxServer.getPort();
		}
		catch (final Exception e) {
			logger.severe("Unable to start JBox: " + e.toString());
		}

		logger.info("Looping");
		while (true) {
			offerAgent();

			try {
				Thread.sleep(System.getenv("ce_loop_time") != null ? Long.parseLong(System.getenv("ce_loop_time")) : 60000);
			}
			catch (InterruptedException e) {
				logger.severe("Unable to sleep: " + e.toString());
			}

		}

	}

	/**
	 * @param args
	 */
	public static void main(final String[] args) {
		final ComputingElement CE = new ComputingElement();
		CE.run();
	}

	private int getNumberFreeSlots() {
		// First we get the maxJobs and maxQueued from the Central Services
		final GetNumberFreeSlots jobSlots = commander.q_api.getNumberFreeSlots((String) config.get("host_host"), port, siteMap.get("CE").toString(),
				ConfigUtils.getConfig().gets("version", "J-1.0").trim());

		final List<Integer> slots = jobSlots.getJobSlots();
		int max_jobs = 0;
		int max_queued = 0;

		if (slots == null) {
			logger.info("Cannot get values from getNumberFreeSlots");
			return 0;
		}

		if (slots.get(0).intValue() == 0 && slots.size() >= 3) { // OK
			max_jobs = slots.get(1).intValue();
			max_queued = slots.get(2).intValue();
			logger.info("Max jobs: " + max_jobs + " Max queued: " + max_queued);
		}
		else { // Error
			switch (slots.get(0).intValue()) {
				case 1:
					logger.info("Failed getting or inserting host in getNumberFreeSlots");
					break;
				case 2:
					logger.info("Failed updating host in getNumberFreeSlots");
					break;
				case 3:
					logger.info("Failed getting slots in getNumberFreeSlots");
					break;
				case -2:
					logger.info("The queue is centrally locked!");
					break;
				default:
					logger.info("Unknown error in getNumberFreeSlots");
			}
			return 0;
		}

		// Now we get the values from the batch interface and calculate the slots available
		final int queued = queue.getNumberQueued();
		if (queued < 0) {
			logger.info("There was a problem getting the number of queued agents!");
			return 0;
		}
		logger.info("Agents queued: " + queued);

		final int active = queue.getNumberActive();
		if (active < 0) {
			logger.info("There was a problem getting the number of active agents!");
			return 0;
		}
		logger.info("Agents active: " + active);

		int free = max_queued - queued;
		if ((max_jobs - active) < free)
			free = max_jobs - active;

		// Report slot status to ML
		final Vector<String> paramNames = new Vector<>();
		final Vector<Object> paramValues = new Vector<>();
		paramNames.add("jobAgents_queued");
		paramNames.add("jobAgents_running");
		paramNames.add("jobAgents_slots");
		paramValues.add(Integer.valueOf(queued));
		paramValues.add(Integer.valueOf(active - queued));
		paramValues.add(Integer.valueOf(free < 0 ? 0 : free));

		try {
			apmon.sendParameters(site + "_CE_" + siteMap.get("CE"), config.get("host_host").toString(), paramNames.size(), paramNames, paramValues);
		}
		catch (ApMonException | IOException e) {
			logger.severe("Can't send parameter to ML (getNumberFreeSlots): " + e);
		}

		return free;
	}

	private void offerAgent() {
		int slots_to_submit = getNumberFreeSlots();

		if (slots_to_submit <= 0) {
			logger.info("No slots available in the CE!");
			return;
		}
		logger.info("CE free slots: " + slots_to_submit);

		// We ask the broker how many jobs we could run
		final GetNumberWaitingJobs jobMatch = commander.q_api.getNumberWaitingForSite(siteMap);
		final int waiting_jobs = jobMatch.getNumberJobsWaitingForSite().intValue();

		if (waiting_jobs <= 0) {
			logger.info("Broker returned 0 available waiting jobs");
			return;
		}
		logger.info("Waiting jobs: " + waiting_jobs);

		if (waiting_jobs < slots_to_submit)
			slots_to_submit = waiting_jobs;

		logger.info("Going to submit " + slots_to_submit + " agents");

		final String script = createAgentStartup();
		if (script == null) {
			logger.info("Cannot create startup script");
			return;
		}
		logger.info("Created AgentStartup script: " + script);

		while (slots_to_submit > 0) {
			queue.submit(script);
			slots_to_submit--;
		}

		logger.info("Submitted " + slots_to_submit);

		return;
	}

	/*
	 * Creates script to execute on worker nodes
	 */
	private String createAgentStartup() {
		String startup_script = System.getenv("HOME") + "/jalien/jalien ";
		if (System.getenv("JALIEN_ROOT") != null) {
			startup_script = System.getenv("JALIEN_ROOT") + "/jalien ";
		}
		String before = "";
		String after = "";

		final long time = new Timestamp(System.currentTimeMillis()).getTime();
		String host_tempdir = (String) config.get("host_tmpdir");
		final String host_tempdir_resolved = Functions.resolvePathWithEnv(host_tempdir);
		if (this.queue.getClass() == alien.site.batchqueue.FORK.class)
			host_tempdir = host_tempdir_resolved;
		// final String cert_file = host_tempdir + "/token_cert." + time;
		// final String key_file = host_tempdir + "/token_key." + time;

		int ttl_hours = ((Integer) siteMap.get("TTL")).intValue();
		ttl_hours = ttl_hours / 3600;

		// get the jobagent token
		final String[] token_certificate_full = getTokenCertificate(ttl_hours / 24 + 1);
		final String token_cert_str = token_certificate_full[0].trim();
		final String token_key_str = token_certificate_full[1].trim();

		// prepare the jobagent token certificate
		before += "export JALIEN_TOKEN_CERT=\"" + token_cert_str + "\";\n" + "export JALIEN_TOKEN_KEY=\"" + token_key_str + "\";\n";
		if (config.containsKey("proxy_cache_file")) {
			String resolvedPath = Functions.resolvePathWithEnv((String) (config.get("proxy_cache_file")));
			before += "if test -f \'"
				+ resolvedPath
				+ "\' ; then\n";
			before += "source "
				+ resolvedPath
				+ "\n";
			before += "fi\n";
		}

		// pass environment variables
		if (config.containsKey("host_logdir") || config.containsKey("site_logdir"))
			before += "export LOGDIR='" + (config.containsKey("host_logdir") ? config.get("host_logdir") : config.get("site_logdir")) + "'\n";
		if (config.containsKey("host_cachedir") || config.containsKey("site_cachedir"))
			before += "export CACHEDIR='" + (config.containsKey("host_cachedir") ? config.get("host_cachedir") : config.get("site_cachedir")) + "'\n";
		if (config.containsKey("host_tmpdir") || config.containsKey("site_tmpdir"))
			before += "export TMPDIR='" + (config.containsKey("host_tmpdir") ? config.get("host_tmpdir") : config.get("site_tmpdir")) + "'\n";
		if (config.containsKey("host_workdir") || config.containsKey("site_workdir"))
			before += "export WORKDIR='" + (config.containsKey("host_workdir") ? config.get("host_workdir") : config.get("site_workdir")) + "'\n";
		before += "export ALIEN_CM_AS_LDAP_PROXY='" + config.get("ALIEN_CM_AS_LDAP_PROXY") + "'\n";
		before += "export site='" + site + "'\n";
		before += "export CE='" + siteMap.get("CE") + "'\n";
		before += "export CEhost='" + siteMap.get("Localhost") + "'\n";
		before += "export TTL='" + siteMap.get("TTL") + "'\n";
		if (config.containsKey("ce_installationmethod"))
			before += "export installationMethod='" + config.get("ce_installationmethod") + "'\n";
		if (config.containsKey("ce_cerequirements"))
			before += "export cerequirements='" + config.get("ce_cerequirements") + "'\n";
		if (config.containsKey("ce_partition"))
			before += "export partition='" + config.get("ce_partition") + "'\n";
		if (siteMap.containsKey("closeSE"))
			before += "export closeSE='" + siteMap.get("closeSE") + "'\n";

		// cleanup jar
		after += "rm -f alien.jar\n";

		if (config.containsKey("ce_installmethod") && config.get("ce_installmethod").equals("CVMFS"))
			startup_script = runFromCVMFS();
		else
			startup_script = runFromDefault();

		final String content_str = before + startup_script + after;

		final String agent_startup_path = host_tempdir_resolved + "/agent.startup." + time;
		final File agent_startup_file = new File(agent_startup_path);
		try {
			agent_startup_file.createNewFile();
			agent_startup_file.setExecutable(true);
		}
		catch (final IOException e1) {
			logger.info("Error creating Agent Sturtup file: " + e1.toString());
			return null;
		}

		try (PrintWriter writer = new PrintWriter(agent_startup_path, "UTF-8")) {
			writer.println("#!/bin/bash");
			writer.println(content_str);
		}
		catch (final FileNotFoundException e) {
			logger.info("Agent Sturtup file not found: " + e.toString());
		}
		catch (final UnsupportedEncodingException e) {
			logger.info("Encoding error while writing Agent Sturtup file: " + e.toString());
		}

		return agent_startup_path;
	}

	private String[] getTokenCertificate(final int ttl_days) {
		GetTokenCertificate gtc = new GetTokenCertificate(commander.getUser(), commander.getUsername(), TokenCertificateType.JOB_AGENT_TOKEN, null, ttl_days);

		try {
			gtc = Dispatcher.execute(gtc);
			final String[] token_cert_and_key = new String[2];
			token_cert_and_key[0] = gtc.getCertificateAsString();
			token_cert_and_key[1] = gtc.getPrivateKeyAsString();
			return token_cert_and_key;
		}
		catch (Exception e) {
			logger.info("Getting JobAgent TokenCertificate failed: " + e);
		}

		return null;
	}

	final class TokenFileGenerationThread extends Thread {
		String resolvedPath;
		Logger log;
		final int ttlDays;

		public TokenFileGenerationThread(String tokenFilePath, Logger logr, int ttl) {
			this.resolvedPath = Functions.resolvePathWithEnv(tokenFilePath);
			ttl = ttl / 3600 / 24 + 1;
			this.ttlDays = ttl;
			this.log = logr;
		}

		public void run() {
			log.info("Starting");

			setName("Token file generation thread");
			while (true) {
				log.info("Starting loop");
				String[] certs = getTokenCertificate(ttlDays);
				if (certs != null) {
					try (FileOutputStream writer = new FileOutputStream(resolvedPath)) {
						String certCmd = "export JALIEN_TOKEN_CERT=\""
								+ certs[0].trim() + "\";\n";
						String keyCmd = "export JALIEN_TOKEN_KEY=\""
								+ certs[1].trim() + "\";\n";
						writer.write((certCmd + keyCmd).getBytes());
					} catch (Exception e) {
						e.printStackTrace();
						break;
					}
				}
				try {
					log.info("Sleep");
					Thread.sleep(5 * 60 * 1000);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	private static String runFromCVMFS() {
		logger.info("The worker node will install with the CVMFS method");
		// String alien_version = System.getenv("ALIEN_VERSION");
		// String cvmfs_path = "/cvmfs/alice.cern.ch/bin";
		//
		// alien_version = (alien_version != null ? alien_version = "--alien-version " + alien_version : ""); // TODO: uncomment
		//
		// if (ce_environment.containsKey("CVMFS_PATH"))
		// cvmfs_path = ce_environment.get("CVMFS_PATH");
		//
		// return cvmfs_path + "/alienv " + alien_version + " -jalien jalien";

		// return System.getenv("HOME") + "/jalien/jalien"; //TODO: local version
		return "curl -k -o alien.jar \"https://alien.cern.ch/jalien/alien.jar\"\n"
				+ "/cvmfs/alice.cern.ch/x86_64-2.6-gnu-4.1.2/Packages/AliEn/v2-19-395/java/MonaLisa/java/bin/java -cp alien.jar alien.site.JobAgent\n";
	}

	private static String runFromDefault() {
		logger.info("The worker node will install with the no method");
		return "curl -k -o alien.jar \"https://alien.cern.ch/jalien/alien.jar\"\njava -cp alien.jar alien.site.JobAgent\n";
	}

	// Prepares a hash to create the sitemap
	void getSiteMap() {
		final HashMap<String, String> smenv = new HashMap<>();

		smenv.put("ALIEN_CM_AS_LDAP_PROXY", config.get("host_host") + ":" + port);
		config.put("ALIEN_CM_AS_LDAP_PROXY", config.get("host_host") + ":" + port);

		smenv.put("site", site);

		smenv.put("CE", "ALICE::" + site + "::" + config.get("host_ce"));

		// TTL will be recalculated in the loop depending on proxy lifetime
		if (config.containsKey("ce_ttl"))
			smenv.put("TTL", (String) config.get("ce_ttl"));
		else
			smenv.put("TTL", "86400");

		smenv.put("Disk", "100000000"); // TODO: df

		if (config.containsKey("ce_cerequirements"))
			smenv.put("cerequirements", config.get("ce_cerequirements").toString());

		if (config.containsKey("ce_partition"))
			smenv.put("partition", config.get("ce_partition").toString());

		if (config.containsKey("host_closese"))
			smenv.put("closeSE", config.get("host_closese").toString());
		else if (config.containsKey("site_closese"))
			smenv.put("closeSE", config.get("site_closese").toString());

		if (config.containsKey("host_environment"))
			host_environment = getValuesFromLDAPField(config.get("host_environment"));

		// environment field can have n values
		if (config.containsKey("ce_environment"))
			ce_environment = getValuesFromLDAPField(config.get("ce_environment"));

		if (host_environment != null)
			config.putAll(host_environment);
		if (ce_environment != null)
			config.putAll(ce_environment);

		siteMap = (new SiteMap()).getSiteParameters(smenv);
	}

	/**
	 * @param field
	 * @return Map with the values of the LDAP field
	 */
	public static HashMap<String, String> getValuesFromLDAPField(final Object field) {
		final HashMap<String, String> map = new HashMap<>();
		if (field instanceof TreeSet) {
			@SuppressWarnings("unchecked")
			final Set<String> host_env_set = (Set<String>) field;
			for (final String env_entry : host_env_set) {
				final String[] host_env_str = env_entry.split("=");
				map.put(host_env_str[0], host_env_str[1]);
			}
		}
		else {
			final String[] host_env_str = ((String) field).split("=");
			map.put(host_env_str[0], host_env_str[1]);
		}
		return map;
	}

	/*
	 * Get queue class with reflection
	 */
	BatchQueue getBatchQueue(final String type) {
		Class<?> cl = null;
		try {
			cl = Class.forName("alien.site.batchqueue." + type);
		}
		catch (final ClassNotFoundException e) {
			logger.severe("Cannot find class for type: " + type + "\n" + e);
			return null;
		}

		Constructor<?> con = null;
		try {
			con = cl.getConstructor(config.getClass(), logger.getClass());
		}
		catch (NoSuchMethodException | SecurityException e) {
			logger.severe("Cannot find class for ceConfig: " + e);
			return null;
		}

		// prepare some needed fields for the queue
		if (!config.containsKey("host_host") || (config.get("host_host") == null))
			config.put("host_host", ConfigUtils.getLocalHostname());

		if (!config.containsKey("host_port") || (config.get("host_port") == null))
			config.put("host_port", Integer.valueOf(this.port));

		try {
			queue = (BatchQueue) con.newInstance(config, logger);
		}
		catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			logger.severe("Cannot instantiate queue class for type: " + type + "\n" + e);
		}

		return queue;
	}

}
