package alien.site.batchqueue;

import java.io.File;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;

import alien.site.Functions;

/**
 * @author maarten
 */
public class ARC extends BatchQueue {

	private Map<String, String> environment = new HashMap<>();
	private Set<String> envFromConfig;
	private String submitCmd;
	private String submitArg = "";
	private List<String> submitArgList = new ArrayList<>();
	private List<String> submitXRSL = new ArrayList<>();
	private String siteBDII;
	private final static int GLUE_2 = 2;
	private int useLDAP = GLUE_2;
	private int cDay = 0;
	private int seqNr = 0;
	private String tmpDir;

	//
	// to support weighted, round-robin load-balancing over a CE set:
	//

	private ArrayList<String> ce_list = new ArrayList<>();
	private HashMap<String, Double> ce_weight = new HashMap<>();
	private int next_ce = 0;
	private HashMap<String, Integer> running = new HashMap<>();
	private HashMap<String, Integer> waiting = new HashMap<>();
	private int tot_running = 0;
	private int tot_waiting = 0;
	private long job_numbers_timestamp = 0;
	private long proxy_check_timestamp = 0;

	/**
	 * @param conf
	 * @param logr
	 */
	@SuppressWarnings("unchecked")
	public ARC(HashMap<String, Object> conf, Logger logr) {
		config = conf;
		logger = logr;

		logger.info("This VO-Box is " + config.get("ALIEN_CM_AS_LDAP_PROXY") +
				", site is " + config.get("site_accountname"));

		String ce_env_str = "ce_environment";

		if (config.get(ce_env_str) == null) {
			String msg = ce_env_str + " needs to be defined!";
			logger.warning(msg);
			config.put(ce_env_str, new TreeSet<String>());
		}

		try {
			envFromConfig = (TreeSet<String>) config.get(ce_env_str);
		}
		catch (@SuppressWarnings("unused") ClassCastException e) {
			envFromConfig = Set.of((String) config.get(ce_env_str));
		}

		//
		// initialize our environment from the LDAP configuration
		//

		for (String env_field : envFromConfig) {
			String[] parts = env_field.split("=", 2);
			String var = parts[0];
			String val = parts.length > 1 ? parts[1] : "";
			environment.put(var, val);
			logger.info("envFromConfig: " + var + "=" + val);
		}

		//
		// allow the process environment to override any variable and add others
		//

		environment.putAll(System.getenv());

		String ce_submit_cmd_str = "CE_SUBMITCMD";

		submitCmd = environment.getOrDefault(ce_submit_cmd_str, (String) config.getOrDefault(ce_submit_cmd_str, "arcsub"));
		logger.info("submit command: " + submitCmd);

		tmpDir = Functions.resolvePathWithEnv((String) config.getOrDefault("host_tmpdir", config.getOrDefault("site_tmpdir", environment.getOrDefault("TMPDIR", "/tmp"))));

		logger.info("temp directory: " + tmpDir);

		for (Map.Entry<String, String> entry : environment.entrySet()) {
			final String var = entry.getKey();
			String val = entry.getValue();

			if (var.equals("CE_LCGCE")) {
				double tot = 0;
				val = val.replaceAll("[()]", "");

				//
				// support weighted, round-robin load-balancing over a CE set
				//
				// CE_LCGCE=[N1 * ]ce1.abc.xyz:port/type-batch-queue, [N2 * ]ce2.abc.xyz:port/...
				//

				logger.info("Load-balancing over these CEs with configured weights:");

				for (String str : val.split(",")) {
					Double w = Double.valueOf(1);
					String ce = str;
					Pattern p = Pattern.compile("(\\d+)\\s*\\*\\s*(\\S+)");
					Matcher m = p.matcher(str);

					if (m.find()) {
						w = Double.valueOf(m.group(1));
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

					ce = ce.replaceAll(":.*", "");

					logger.info(ce + " --> " + String.format("%5.3f", w));

					ce_list.add(ce);
					ce_weight.put(ce, w);
					tot += w.doubleValue();
				}

				if (tot <= 0) {
					String msg = "CE_LCGCE invalid: " + val;
					logger.severe(msg);
					throw new IllegalArgumentException(msg);
				}

				if (ce_weight.size() != ce_list.size()) {
					String msg = "CE_LCGCE has duplicate CEs: " + val;
					logger.severe(msg);
					throw new IllegalArgumentException(msg);
				}

				logger.info("Load-balancing over these CEs with normalized weights:");

				for (String ce : ce_list) {
					Double w = Double.valueOf(ce_weight.get(ce).doubleValue() / tot);
					ce_weight.replace(ce, w);
					logger.info(ce + " --> " + String.format("%5.3f", w));
				}

				continue;
			}

			if (var.equals("CE_SUBMITARG")) {
				logger.info("environment: " + var + "=" + val);
				submitArg = val;
				continue;
			}

			if (var.equals("CE_SUBMITARG_LIST")) {
				logger.info("environment: " + var + "=" + val);

				String[] tmp = val.split("\\s+", 0);
				String p = "^xrsl:";

				for (String s : tmp) {
					if (Pattern.matches(p + "\\(.*\\)", s)) {
						submitXRSL.add(s.replaceAll(p, ""));
					}
					else if (Pattern.matches(p + ".*", s)) {
						submitXRSL.add("(" + s.replaceAll(p, "") + ")");
					}
					else {
						submitArgList.add(s);
					}
				}

				continue;
			}

			if (var.equals("CE_USE_BDII")) {
				logger.info("environment: " + var + "=" + val);
				useLDAP = Integer.parseInt(val);
				continue;
			}

			if (var.equals("CE_SITE_BDII")) {
				logger.info("environment: " + var + "=" + val);

				String s = val.replaceAll("^([^:]+://)?([^/]+).*", "$2");
				siteBDII = "ldap://" + s;

				if (!Pattern.matches(".*:.*", s)) {
					siteBDII += ":2170";
				}

				continue;
			}
		}

		if (ce_list.size() <= 0) {
			String msg = "No CE usage specified in the environment";
			logger.severe(msg);
			throw new IllegalArgumentException(msg);
		}

		if (useLDAP != GLUE_2) {
			String msg = "useLDAP != GLUE_2: not implemented!";
			logger.severe(msg);
			throw new IllegalArgumentException(msg);
		}
	}

	private void proxyCheck() {

		String proxy = environment.get("X509_USER_PROXY");
		File proxy_no_check = new File(environment.get("HOME") + "/no-proxy-check");

		if (proxy == null || proxy_no_check.exists()) {
			return;
		}

		String vo_str = (String) config.getOrDefault("LCGVO", "alice");
		String proxy_renewal_str = String.format("/etc/init.d/%s-box-proxyrenewal", vo_str);
		File proxy_renewal_svc = new File(proxy_renewal_str);

		if (!proxy_renewal_svc.exists()) {
			return;
		}

		String threshold = (String) config.getOrDefault("CE_PROXYTHRESHOLD", String.valueOf(46 * 3600));
		logger.info(String.format("X509_USER_PROXY is %s", proxy));
		logger.info("Checking remaining proxy lifetime");

		String proxy_info_cmd = "voms-proxy-info -acsubject -actimeleft 2>&1";
		ArrayList<String> proxy_info_output = executeCommand(proxy_info_cmd);

		String dn_str = "";
		String time_left_str = "";

		for (String line : proxy_info_output) {
			String trimmed_line = line.trim();

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

		String proxy_renewal_cmd = String.format("%s start 2>&1", proxy_renewal_svc);
		ArrayList<String> proxy_renewal_output = null;

		try {
			proxy_renewal_output = executeCommand(proxy_renewal_cmd);
		}
		catch (Exception e) {
			logger.info(String.format("[LCG] Problem while executing command: %s", proxy_renewal_cmd));
			e.printStackTrace();
		}
		finally {
			if (proxy_renewal_output != null) {
				logger.info("Proxy renewal output:\n");

				for (String line : proxy_renewal_output) {
					logger.info(line.trim());
				}
			}
		}
	}

	@Override
	public void submit(final String script) {
		logger.info("Submit ARC");

		//
		// use our own load-balancing
		//

		logger.info("Determining the next CE to use:");

		for (int i = 0; i < ce_list.size(); i++) {
			String ce = ce_list.get(next_ce);
			Integer idle = waiting.get(ce);
			Double w = ce_weight.get(ce);
			double f = tot_waiting > 0 ? idle.doubleValue() / tot_waiting : 0;

			logger.info(String.format(
					"--> %s has idle fraction %d / %d = %5.3f vs. weight %5.3f",
					ce, idle, Integer.valueOf(tot_waiting), Double.valueOf(f), w));

			if (f < w.doubleValue()) {
				break;
			}

			next_ce++;
			next_ce %= ce_list.size();
		}

		String ce = ce_list.get(next_ce);

		logger.info("--> next CE to use: " + ce);

		waiting.put(ce, Integer.valueOf(waiting.get(ce).intValue() + 1));
		tot_waiting++;

		next_ce++;
		next_ce %= ce_list.size();

		String host = ce.replaceAll(":.*", "");
		String name = "JAliEn-" + job_numbers_timestamp + "-" + (seqNr++);
		String remote_script = name + ".sh";
		String cm = config.get("host_host") + ":" + config.get("host_port");

		String submit_xrsl = "&\n" +
				"(jobName = " + name + ")\n" +
				"(executable = /usr/bin/time)\n" +
				"(arguments = bash " + remote_script + ")\n" +
				"(stdout = std.out)\n" +
				"(stderr = std.err)\n" +
				"(gmlog = gmlog)\n" +
				"(inputFiles = (" + remote_script + " " + script + "))\n" +
				"(outputFiles = (std.err \"\") (std.out \"\") (gmlog \"\") (" + remote_script + " \"\"))\n" +
				"(environment = (ALIEN_CM_AS_LDAP_PROXY " + cm + "))\n";

		for (String s : submitXRSL) {
			submit_xrsl += s + "\n";
		}

		DateFormat date_format = new SimpleDateFormat("yyyy-MM-dd");
		String current_date_str = date_format.format(new Date());
		String log_folder_path = tmpDir + "/arc/" + current_date_str;
		File log_folder = new File(log_folder_path);

		if (!log_folder.exists()) {
			try {
				log_folder.mkdirs();
			}
			catch (Exception e) {
				logger.severe("[ARC] log folder mkdirs() exception: " + log_folder);
				e.printStackTrace();
			}

			if (!log_folder.exists()) {
				logger.severe("[ARC] Couldn't create log folder: " + log_folder);
				return;
			}
		}

		//
		// keep overwriting the same file for ~1 minute
		//

		String submit_file = log_folder + "/arc-" + (job_numbers_timestamp >> 16) + ".xrsl";

		try (PrintWriter out = new PrintWriter(submit_file)) {
			out.println(submit_xrsl);
			out.close();
		}
		catch (Exception e) {
			logger.severe("Error writing to submit file: " + submit_file);
			e.printStackTrace();
			return;
		}

		String submit_cmd = submitCmd + " -t 20 -f " + submit_file + " -c " + host + " " + submitArg;

		for (String s : submitArgList) {
			submit_cmd += " " + s;
		}

		ArrayList<String> output = executeCommand(submit_cmd);

		for (String line : output) {
			String trimmed_line = line.trim();
			logger.info(trimmed_line);
		}
	}

	private int queryLDAP(final String svc) {

		logger.info("query target " + svc);

		Hashtable<String, String> env = new Hashtable<>();
		env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
		env.put(Context.PROVIDER_URL, svc);
		env.put("com.sun.jndi.ldap.connect.timeout", "10000");
		env.put("com.sun.jndi.ldap.read.timeout", "10000");

		String vo_str = (String) config.getOrDefault("LCGVO", "alice");
		String filter = "(|(GLUE2PolicyRule=*:" + vo_str + ")"
				+ "(objectClass=GLUE2ComputingEndpoint)(objectClass=GLUE2ComputingShare))";

		SearchControls sc = new SearchControls();
		sc.setSearchScope(SearchControls.SUBTREE_SCOPE);

		List<String> shares = new ArrayList<>();
		HashMap<String, String> urls = new HashMap<>();
		HashMap<String, String> endp = new HashMap<>();
		HashMap<String, Object> running_on_share = new HashMap<>();
		HashMap<String, Object> waiting_on_share = new HashMap<>();

		DirContext ctx = null;

		try {
			ctx = new InitialDirContext(env);

			NamingEnumeration<SearchResult> results = ctx.search("o=glue", filter, sc);

			while (results.hasMore()) {
				SearchResult sr = results.next();
				Attributes attrs = sr.getAttributes();

				Attribute ep = attrs.get("GLUE2EndpointID");

				if (ep != null) {
					String ep_v = Objects.toString(ep.get());
					Attribute url = attrs.get("GLUE2EndpointURL");
					String url_v = Objects.toString(url.get());
					urls.put(ep_v, url_v);
					continue;
				}

				Attribute fKey = attrs.get("GLUE2MappingPolicyShareForeignKey");

				if (fKey != null) {
					//
					// we found the name of a share for our VO
					//

					String fKey_v = Objects.toString(fKey.get());
					shares.add(fKey_v);
					continue;
				}

				Attribute share = attrs.get("GLUE2ShareID");

				if (share == null) {
					continue;
				}

				//
				// we found a share for some VO
				//

				String share_v = Objects.toString(share.get());

				Attribute fKeys = attrs.get("GLUE2ComputingShareComputingEndpointForeignKey");

				if (fKeys == null) {
					continue;
				}

				boolean found = false;
				NamingEnumeration<?> e = fKeys.getAll();

				while (e.hasMore()) {
					String fk = Objects.toString(e.next());

					for (String ce : ce_list) {
						String s = ce.replaceAll(":.*", ":2811");
						Pattern p = Pattern.compile(s);
						Matcher m = p.matcher(fk);

						//
						// skip endpoints outside of our CE list,
						// taking advantage of the way ARC
						// structures the endpoint IDs...
						//

						if (m.find()) {
							endp.put(share_v, fk);
							found = true;
							break;
						}
					}

					if (found) {
						break;
					}
				}

				if (!found) {
					continue;
				}

				Attribute r = attrs.get("GLUE2ComputingShareRunningJobs");
				Attribute w = attrs.get("GLUE2ComputingShareWaitingJobs");

				running_on_share.put(share_v, r == null ? null : r.get());
				waiting_on_share.put(share_v, w == null ? null : w.get());
			}

		}
		catch (Exception e) {
			logger.warning("Error querying LDAP service " + svc);
			e.printStackTrace();
		}
		finally {
			if (ctx != null) {
				try {
					ctx.close();
				}
				catch (@SuppressWarnings("unused") Exception e) {
					// ignore
				}
			}
		}

		int n = 0;

		for (String share : shares) {
			String ep = endp.get(share);

			if (ep == null) {
				continue;
			}

			String url = urls.get(ep);

			if (url == null) {
				continue;
			}

			Object r_obj = running_on_share.get(share);
			Object w_obj = waiting_on_share.get(share);

			if (r_obj == null || w_obj == null) {
				continue;
			}

			Integer r = null, w = null;

			try {
				r = Integer.valueOf(r_obj.toString());
				w = Integer.valueOf(w_obj.toString());
			}
			catch (@SuppressWarnings("unused") Exception e) {
				continue;
			}

			String ce = url.replaceAll("^[^:]*:?/*([^:/]+).*", "$1");
			String name = share.replaceAll(".*:", "");

			Integer cr = running.get(ce);
			Integer cw = waiting.get(ce);

			if (cr == null || cw == null) {
				continue;
			}

			logger.info(String.format("--> waiting: %5d, running: %5d, share '%s' on %s",
					w, r, name, ce));

			running.put(ce, Integer.valueOf(cr.intValue() + r.intValue()));
			tot_running += r.intValue();

			waiting.put(ce, Integer.valueOf(cw.intValue() + w.intValue()));
			tot_waiting += w.intValue();

			n++;
		}

		return n;
	}

	private boolean getJobNumbers() {

		long now = System.currentTimeMillis();
		long dt = (now - job_numbers_timestamp) / 1000;

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

		if (useLDAP == GLUE_2) {
			//
			// hack to keep the jobs DB size manageable...
			//

			Calendar calendar = Calendar.getInstance();
			int wDay = calendar.get(Calendar.DAY_OF_WEEK);

			if (cDay != 0 && cDay != wDay) {
				String prefix = "~/.arc/jobs.";
				String suffixes[] = { "dat", "xml" };

				for (String suffix : suffixes) {
					String f = prefix + suffix;
					String cmd = String.format("test ! -e %s || mv %s %s.%d", f, f, f, Integer.valueOf(cDay));
					ArrayList<String> output = executeCommand(cmd);

					for (String line : output) {
						logger.info(line);
					}
				}
			}

			cDay = wDay;
		}

		for (String ce : ce_list) {
			running.put(ce, Integer.valueOf(0));
			waiting.put(ce, Integer.valueOf(0));
		}

		tot_running = tot_waiting = 0;
		int n = 0;

		if (useLDAP == GLUE_2) {
			if (siteBDII != null) {
				n = queryLDAP(siteBDII);
			}
			else {
				for (String ce : ce_list) {
					n += queryLDAP("ldap://" + ce + ":2135");
				}
			}

			if (n <= 0) {
				tot_waiting = 444444;
				logger.warning("no result from LDAP --> " + tot_waiting + " waiting jobs");
			}
		}
		else {
			String msg = "useLDAP != GLUE_2: not implemented!";
			logger.severe(msg);
			throw new IllegalArgumentException(msg);
		}

		logger.info("Found " + tot_waiting + " idle (and " + tot_running + " running) jobs:");

		for (String ce : ce_list) {
			logger.info(String.format("%5d (%5d) for %s", waiting.get(ce), running.get(ce), ce));
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
