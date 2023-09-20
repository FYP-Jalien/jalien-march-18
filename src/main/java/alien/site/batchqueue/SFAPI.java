package alien.site.batchqueue;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.interfaces.RSAPrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.net.ssl.HttpsURLConnection;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;

import alien.site.Functions;

import lazyj.Utils;

import lia.util.process.ExternalProcess.ExitStatus;

/**
 *
 */
public class SFAPI extends BatchQueue {

	private final Map<String, String> environment;
	private TreeSet<String> envFromConfig;
	private final String submitCmd;
	private String submitArgs = "";
	private String clientId = "";
	private String killCmd;
	private String killArgs = "";
	private String statusCmd;
	private String statusArgs = "";
	private String runArgs = "";
	private String batchArgs = "";
	private final String user;
	private File temp_file;

	/**
	 * @param conf
	 * @param logr
	 */
	@SuppressWarnings("unchecked")
	public SFAPI(final HashMap<String, Object> conf, final Logger logr) {
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

		this.clientId = (String) config.getOrDefault("ce_cliendid", "");

		this.submitArgs = readArgFromLdap("ce_submitarg");
		this.killArgs = readArgFromLdap("ce_killarg");
		this.runArgs = readArgFromLdap("ce_runarg");
		this.batchArgs = readArgFromLdap("ce_batcharg");
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
				if (env_field.contains("BATCH_ARGS")) {
					this.batchArgs = getValue(env_field, "BATCH_ARGS", this.batchArgs);
				}
				if (env_field.contains("KILL_ARGS")) {
					this.killArgs = getValue(env_field, "KILL_ARGS", this.killArgs);
				}
				if (env_field.contains("CLIENT_ID")) {
					this.clientId = getValue(env_field, "CLIENT_ID", this.clientId);
				}
			}
		}

		this.submitArgs = environment.getOrDefault("SUBMIT_ARGS", submitArgs);
		this.statusArgs = environment.getOrDefault("STATUS_ARGS", this.statusArgs);
		this.runArgs = environment.getOrDefault("RUN_ARGS", this.runArgs);
		this.killArgs = environment.getOrDefault("KILL_ARGS", this.killArgs);
		this.batchArgs = environment.getOrDefault("BATCH_ARGS", this.batchArgs);

		this.clientId = environment.getOrDefault("CLIENT_ID", clientId);

		user = environment.get("USER");

		statusOpts = "-h -o \"%i %t %j\" -u " + user;

		statusCmd = statusCmd + " " + statusOpts;

		killArgs += " --ctld -Q -u " + user;
		// killArgs += " --ctld -Q";

		killCmd = killCmd + killArgs;
	}

	public String getScript(String script) {

		this.logger.info("Submit SFAPI");
		this.logger.info("script location " + script);

		final Long timestamp = Long.valueOf(System.currentTimeMillis());

		// Logging setup
		String out_cmd = "#SBATCH -o /dev/null";
		String err_cmd = "#SBATCH -e /dev/null";
		final String name = String.format("jobagent_%s_%d", this.config.get("host_host"), timestamp);

		// Check if we can use SLURM_LOG_PATH instead of sending to /dev/null
		final String host_logdir = environment.getOrDefault("SLURM_LOG_PATH", config.get("host_logdir") != null ? config.get("host_logdir").toString() : null);

		if (host_logdir != null) {
			final String log_folder_path = String.format("%s", host_logdir);
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
			final String file_base_name = String.format("/pscratch/sd/a/alicepro/vobox/workdir/jobagent_%s_%d",
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
		final String workdir_path = config.get("host_workdir") != null ? String.format("%s/jobagent_%s_%d", config.get("host_workdir"),
				config.get("host_host"), timestamp) : environment.getOrDefault("TMPDIR", "/tmp");
		final String workdir_path_resolved = Functions.resolvePathWithEnv(workdir_path);
		final File workdir_file = new File(workdir_path_resolved);
		workdir_file.mkdir();

		submit_cmd += String.format("#SBATCH -J perlmutter%s%n", name);
		submit_cmd += "#SBATCH -N 1\n";
		submit_cmd += "#SBATCH -n 1\n";
		submit_cmd += "#SBATCH --no-requeue\n";
		submit_cmd += "#SBATCH -D " + workdir_path_resolved + "\n";
		submit_cmd += String.format("%s%n%s%n", out_cmd, err_cmd);
		submit_cmd += batchArgs + "\n";

		String scriptContent;
		try {
			scriptContent = Files.readString(Paths.get(script));
		}
		catch (final IOException e2) {
			this.logger.log(Level.WARNING, "Error reading agent startup script!", e2);
			return "";
		}

		final String encodedScriptContent = Utils.base64Encode(scriptContent.getBytes()).replaceAll("(\\w{76})", "$1\n");
		final String srun_script = String.format("%s_%d", script, timestamp);

		// These changes are useful for enforcing workdir
		submit_cmd += "mkdir " + workdir_path_resolved + "\n";
		submit_cmd += "mkdir " + workdir_path_resolved + "/log\n";
		submit_cmd += "mkdir " + workdir_path_resolved + "/cache\n";
		submit_cmd += "export TMPDIR=" + workdir_path_resolved + "\n";
		submit_cmd += "export LOGDIR=" + workdir_path_resolved + "\n";
		submit_cmd += "export SLURM_TMP=" + workdir_path_resolved + "\n";

		submit_cmd += "cat<<__EOF__ | base64 -d > " + srun_script + "\n";
		submit_cmd += encodedScriptContent;
		submit_cmd += "\n__EOF__\n";
		submit_cmd += "chmod a+x " + srun_script + "\n";
		submit_cmd += "srun " + runArgs + " " + srun_script + "\n";

		submit_cmd += "rm " + srun_script;
		return submit_cmd;
	}

	public void sendJob(HttpsURLConnection https, String script) throws IOException{
		https.setDoOutput(true);
		OutputStream os = https.getOutputStream();
		os.write("isPath=false".getBytes());
		System.out.println("Script out " + script);
		os.write(("&job=" + URLEncoder.encode(script, StandardCharsets.UTF_8)).getBytes());
		os.flush();
		os.close();
	}

	public static void setProps(HttpsURLConnection https, String token) throws IOException{
		https.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
		https.setRequestProperty("authorization", "Bearer " + token);
		https.setRequestProperty("Accept", "application/json");
	}

	public JSONObject getResponse(HttpsURLConnection https) {
		int responseCode;
		StringBuffer response = new StringBuffer();
		JSONParser parser = new JSONParser();
		JSONObject json = null;

		try {
			responseCode = https.getResponseCode();

		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}

		this.logger.info("Response Code :: " + responseCode);

		if (responseCode == HttpsURLConnection.HTTP_OK) { //success
			try (BufferedReader in = new BufferedReader(new InputStreamReader(https.getInputStream()));) {
			String inputLine;

			while ((inputLine = in.readLine()) != null) {
				response.append(inputLine);
			}
			in.close();

			// print result
			//System.out.println(response.toString());
			} catch (IOException e) {
			}
		} else {
			//System.out.println("POST request did not work.");
			this.logger.severe("Request didn't work");
		}

		try {
			json = (JSONObject) parser.parse(response.toString());;
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}

		this.logger.info("response is " + json.toString());
		return json;
	}

	public static RSAPrivateKey readPrivateKey(File file) throws Exception {
		String key = new String(Files.readAllBytes(file.toPath()), Charset.defaultCharset());

		String privateKeyPEM = key
			.replace("-----BEGIN PRIVATE KEY-----", "")
			.replaceAll(System.lineSeparator(), "")
			.replace("-----END PRIVATE KEY-----", "");

		byte[] encoded = Base64.getDecoder().decode(privateKeyPEM);

		KeyFactory keyFactory = KeyFactory.getInstance("RSA");
		PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(encoded);
		return (RSAPrivateKey) keyFactory.generatePrivate(keySpec);
	}

	public static HttpsURLConnection getConnection(String urlString, String method) {

		HttpsURLConnection https = null;

		try {
			final URL url = new URL(urlString);
			final URLConnection conn = url.openConnection();
			https = (HttpsURLConnection) conn;

			https.setRequestMethod(method);
			https.setDoOutput(true);
			https.setDoInput(true);
			https.setConnectTimeout(30000);
			https.setReadTimeout(120000);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return https;
	}

	public static void sendAuthReq(HttpsURLConnection https, String token) throws IOException{
		https.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
		https.setDoOutput(true);
		OutputStream os = https.getOutputStream();
		os.write("grant_type=client_credentials".getBytes());
		os.write("&client_assertion_type=urn%3Aietf%3Aparams%3Aoauth%3Aclient-assertion-type%3Ajwt-bearer".getBytes());
		os.write(("&client_assertion=" + token).getBytes());
		os.flush();
		os.close();
	}

	public String getToken() {
		String accessToken = "";
		try {
			RSAPrivateKey rsaPrivateKey = readPrivateKey(new File("./privateKey8"));
			Algorithm algorithm = Algorithm.RSA256(rsaPrivateKey);
			String token = JWT.create()
				.withIssuer(clientId)
				.withSubject(clientId)
				.withAudience("https://oidc.nersc.gov/c2id/token")
				.withExpiresAt(new Date(System.currentTimeMillis() + 5 * 60 * 1000))
				.sign(algorithm);
			//System.out.println(token);

			HttpsURLConnection https = getConnection("https://oidc.nersc.gov/c2id/token", "POST");

			sendAuthReq(https, token);

			JSONObject json = getResponse(https);

			if (json == null) {
				this.logger.severe("Could not generate auth token");
				return null;
			}

			accessToken = json.get("access_token").toString();
		} catch (Exception exception){
			exception.printStackTrace();
		}
		return accessToken;
	}

	@Override
	public void submit(final String script) {
		String batchScript = getScript(script);
		String accessToken = getToken();
		JSONObject json;
		HttpsURLConnection getTasks;

		try {

			HttpsURLConnection postJob = getConnection("https://api.nersc.gov/api/v1.2/compute/jobs/perlmutter", "POST");
			setProps(postJob, accessToken);
			sendJob(postJob, batchScript);
			json = getResponse(postJob);

			if (json == null) {
				this.logger.severe("Could not submit job");
				return;
			}

			//System.out.println("json is " + json.toString());
			String task_id = json.get("task_id").toString();

			getTasks = getConnection("https://api.nersc.gov/api/v1.2/tasks/" + task_id, "GET");
			setProps(getTasks, accessToken);
			json = getResponse(getTasks);
			//System.out.println("json is " + json.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	JSONArray getJobs() throws Exception {
		HttpsURLConnection getJobs = getConnection("https://api.nersc.gov/api/v1.2/compute/jobs/perlmutter?index=0&kwargs=user%3Dalicepro&sacct=false", "GET");
		JSONObject json = null;
		String accessToken = getToken();
		setProps(getJobs, accessToken);
		json = getResponse(getJobs);

		if (json == null) {
			this.logger.severe("Could get job list");
			return null;
		}

		JSONArray jobs = (JSONArray)json.get("output");

		return jobs;
	}

	/**
	 * @return number of currently active jobs
	 */
	@Override
	public int getNumberActive() {
		final Pattern pat = Pattern.compile("(R)|(S)|(CG)");

		ArrayList<String> states = new ArrayList<String>();
		try {
			JSONArray jobs = getJobs();
			//System.out.println(jobs);
			states = (ArrayList<String>) jobs.stream().map(obj -> (((JSONObject) obj).get("st")))
				.filter(state -> pat.matcher((String) state).matches())
				.collect(Collectors.toList());
		} catch (Exception e) {
			e.printStackTrace();
			return 0;
		}
		return states.size();
	}

	/**
	 * @return number of queued jobs
	 */
	@Override
	public int getNumberQueued() {
		final Pattern pat = Pattern.compile("(PD)|(CF)");
		ArrayList<String> states = new ArrayList<String>();

		try {
			JSONArray jobs = getJobs();
			//System.out.println(jobs);
			states = (ArrayList<String>) jobs.stream().map(obj -> (((JSONObject) obj).get("st")))
				.filter(state -> pat.matcher((String) state).matches())
				.collect(Collectors.toList());
		} catch (Exception e) {
			e.printStackTrace();
			return 0;
		}

		return states.size();
	}

	public void killJob(String jobId, String token) {
		JSONObject json;

		HttpsURLConnection killJob = getConnection("https://api.nersc.gov/api/v1.2/compute/jobs/perlmutter/" + jobId, "DELETE");

		killJob.setRequestProperty("authorization", "Bearer " + token);
		killJob.setRequestProperty("Accept", "application/json");

		json = getResponse(killJob);

		if (json == null) {
			this.logger.severe("Could get kill job " + jobId);
		}

		//System.out.println("response is " + json.toString());
	}

	@Override
	public int kill() {
		String accessToken = getToken();

		try {
			JSONArray jobs = getJobs();
			//System.out.println(jobs);
			jobs.stream().map(obj -> (((JSONObject) obj).get("jobid")))
				.forEach(jobId -> killJob((String) jobId, accessToken));
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
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
