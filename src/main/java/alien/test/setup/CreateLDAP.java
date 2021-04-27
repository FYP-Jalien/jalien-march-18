package alien.test.setup;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import javax.naming.Context;
import javax.naming.NameAlreadyBoundException;
import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;

import alien.site.Functions;
import alien.test.TestBrain;
import alien.test.TestConfig;
import alien.test.utils.TestCommand;
import alien.test.utils.TestException;

/**
 * @author ron
 * @since Sep 16, 2011
 */
public class CreateLDAP {

	/**
	 * the ldap module path of the machine, this is an assumption !
	 */
	public static final String ldap_module_path = "/usr/lib/ldap";

	/**
	 * the testVO ldap cn config location
	 */
	public static final String ldap_conf_cn_dir = TestConfig.ldap_conf_dir + "/cn=config";

	/**
	 * the LDAP args file
	 */
	public static final String ldap_args_file = "/tmp/jalien-slapd.args";

	/**
	 * the LDAP pid file
	 */
	public static final String ldap_pid_file = "/tmp/jalien-slapd.pid";

	/**
	 * the LDAP starter file
	 */
	public static final String ldap_starter = TestConfig.tvo_bin + "/LDAP_starter";

	/**
	 * the LDAP pid
	 */
	static String ldap_pid = null;

	/**
	 * the LDAP context
	 */
	static DirContext context;

	/**
	 * @return state of rampUp
	 * @throws Exception
	 */
	public static boolean rampUpLDAP() throws Exception {
		// try{
		// stopLDAP();
		// } catch(Exception e){
		// // ignore
		// }

		createConfig();
		extractLDAPSchema();
		createLDAPStarter();
		startLDAP();

		Thread.sleep(2000);

		initializeLDAP();
		return true;
	}

	private static void createLDAPStarter() throws Exception {

		Functions.writeOutFile(ldap_starter, "#!/bin/bash\n" +
				TestBrain.cSlapd + " -d -1 -s 0 -h ldap://:" + TestConfig.ldap_port +
				" -F " +
				TestConfig.ldap_conf_dir + " > " + TestConfig.ldap_log + " 2>&1\n");
		new File(ldap_starter).setExecutable(true, true);
	}

	/**
	 * @throws Exception
	 */
	public static void startLDAP() throws Exception {

		TestCommand slapd = new TestCommand(new String[] {
				TestBrain.cNohup, ldap_starter });
		slapd.daemonize();
		slapd.exec();
	}

	/**
	 * @throws Exception
	 */
	public static void stopLDAP() throws Exception {

		ldap_pid = Functions.getFileContent(ldap_pid_file);

		TestCommand slapd = new TestCommand(new String[] { TestBrain.cKill, "-9", ldap_pid });
		// slapd.verbose();
		if (!slapd.exec()) {
			throw new TestException("Could not stop LDAP: \n STDOUT: " + slapd.getStdOut() + "\n STDERR: " + slapd.getStdErr());
		}
	}

	@SuppressWarnings("unused")
	private static String hashPassword(String pass) {
		TestCommand link = new TestCommand(new String[] { TestBrain.cSlappasswd, "-n", "-s", pass });
		if (link.exec())
			return link.getStdOut();
		return null;
	}

	/**
	 * @throws Exception
	 */
	public static void createConfig() throws Exception {

		if (!(new File(ldap_conf_cn_dir)).mkdirs())
			throw new TestException("Could not create LDAP conf directory: [" + ldap_conf_cn_dir + "]");

		Functions.writeOutFile(ldap_conf_cn_file());
		Functions.writeOutFile(ldap_conf_db_backend_file());
		Functions.writeOutFile(ldap_conf_db_config_file());
		Functions.writeOutFile(ldap_conf_db_frontend_file());
		Functions.writeOutFile(ldap_conf_db_hdb_file());
		Functions.writeOutFile(ldap_conf_db_mod_file());

		// hashPassword(TestConfig.ldap_pass)

		Functions.writeOutFile(TestConfig.ldap_config,
				"password=" + TestConfig.ldap_pass + "\n");

	}

	private static void extractLDAPSchema() {
		try {
			Functions.unzip(new File(TestConfig.ldap_schema_zip), new File(ldap_conf_cn_dir));
		}
		catch (IOException e) {
			e.printStackTrace();
			System.err.println("error unzipping ldap schema");
		}
	}

	/**
	 * @return status of the entry add
	 */
	public static final boolean initializeLDAP() {

		try {
			context = getLdapContext();

			addBaseTypesToLDAP();
			addInitConfigToLDAP();

			context.close();
		}
		catch (NamingException ne) {
			ne.printStackTrace();
			return false;
		}
		return true;

	}

	/**
	 * @param sitename
	 * @param domain
	 * @param logdir
	 * @param cachedir
	 * @param tmpdir
	 * @throws NamingException
	 */
	protected static void addSiteToLDAP(
			final String sitename, final String domain,
			final String logdir, final String cachedir, final String tmpdir) throws NamingException {

		context = getLdapContext();
		ArrayList<String> objClasses = new ArrayList<>(2);
		objClasses.add("organizationalUnit");
		objClasses.add("AliEnSite");
		HashMap<String, Object> config = new HashMap<>();
		config.put("domain", domain);
		config.put("logdir", logdir);
		config.put("cachedir", cachedir);
		config.put("tmpdir", tmpdir);
		addToLDAP(objClasses, config, "ou=" + sitename + ",ou=Sites," + TestConfig.ldap_root);

		objClasses = new ArrayList<>(1);
		objClasses.add("organizationalUnit");
		config = new HashMap<>();
		config.put("ou", "Config");
		addToLDAP(objClasses, config, "ou=Config,ou=" + sitename + ",ou=Sites,"
				+ TestConfig.ldap_root);

		config = new HashMap<>();
		config.put("ou", "Services");
		addToLDAP(objClasses, config, "ou=Services,ou=" + sitename + ",ou=Sites,"
				+ TestConfig.ldap_root);

		final String[] services = { "SE", "CE", "FTD", "PackMan" };

		for (String service : Arrays.asList(services)) {
			config = new HashMap<>();
			config.put("ou", service);
			addToLDAP(objClasses, config, "ou=" + service + ",ou=Services,ou="
					+ sitename + ",ou=Sites," + TestConfig.ldap_root);
		}
		context.close();

	}

	/**
	 * @param seName
	 * @param sitename
	 * @param iodeamon
	 * @param storedir
	 * @param qos
	 * @throws NamingException
	 */
	protected static void addSEToLDAP(
			final String seName, final String sitename, final String iodeamon,
			final String storedir, final String qos) throws NamingException {

		String host = iodeamon.substring(0, iodeamon.indexOf(':'));
		String port = iodeamon.substring(iodeamon.indexOf(':') + 1);

		context = getLdapContext();
		ArrayList<String> objClasses = new ArrayList<>(2);
		objClasses.add("AliEnSE");
		objClasses.add("AliEnMSS");
		objClasses.add("AliEnSOAPServer");
		HashMap<String, Object> config = new HashMap<>();
		config.put("name", seName);
		config.put("host", host);
		config.put("mss", "File");
		config.put("savedir", storedir);
		config.put("port", port);
		config.put("ioDaemons", "file:host=" + host + ":port=" + port);
		config.put("QoS", qos);
		config.put("ftdprotocol", "cp");

		addToLDAP(objClasses, config, "name=" + seName + ",ou=SE,ou=Services,ou=" + sitename + ",ou=Sites," + TestConfig.ldap_root);
		context.close();

	}

	/**
	 * @param role
	 * @param user
	 * @throws NamingException
	 */
	protected static void addRoleToLDAP(final String role, final String user) throws NamingException {

		context = getLdapContext();
		ArrayList<String> objClasses = new ArrayList<>(1);
		objClasses.add("AliEnRole");
		HashMap<String, Object> config = new HashMap<>();
		config.put("users", user);
		addToLDAP(objClasses, config, "uid=" + role + ",ou=Roles," + TestConfig.ldap_root);
		context.close();
	}

	/**
	 * @param user
	 * @param uid
	 * @param roles
	 * @param certSubject
	 * @throws NamingException
	 */
	protected static void addUserToLDAP(final String user, final String uid, final String roles,
			final String certSubject) throws NamingException {

		context = getLdapContext();
		ArrayList<String> objClasses = new ArrayList<>(3);
		objClasses.add("posixAccount");
		objClasses.add("AliEnUser");
		objClasses.add("pkiUser");

		HashMap<String, Object> config = new HashMap<>();
		config.put("cn", user);
		config.put("uid", user);
		config.put("uidNumber", uid);
		config.put("gidNumber", "1");
		config.put("homeDirectory", getUserHome(user));
		config.put("userPassword", "{crypt}x");
		config.put("loginShell", "false");
		config.put("subject", certSubject);
		config.put("roles", roles);

		addToLDAP(objClasses, config, "uid=" + user + ",ou=People," + TestConfig.ldap_root);
		context.close();
	}

	/**
	 * @param username
	 * @return user home
	 */
	public static String getUserHome(final String username) {
		return TestConfig.base_home_dir + username.substring(0, 1) + "/" + username + "/";
	}

	private static void addBaseTypesToLDAP() {

		addToLDAP("domain", TestConfig.ldap_suffix);
		addToLDAP("organization", TestConfig.ldap_root);
		addToLDAP("organizationalUnit",
				"ou=Packages," + TestConfig.ldap_root);
		addToLDAP("organizationalUnit",
				"ou=Institutions," + TestConfig.ldap_root);
		addToLDAP("organizationalUnit",
				"ou=Partitions," + TestConfig.ldap_root);
		addToLDAP("organizationalUnit",
				"ou=People," + TestConfig.ldap_root);
		addToLDAP("organizationalUnit",
				"ou=Roles," + TestConfig.ldap_root);
		addToLDAP("organizationalUnit",
				"ou=Services," + TestConfig.ldap_root);
		addToLDAP("organizationalUnit",
				"ou=Sites," + TestConfig.ldap_root);

	}

	private static void addInitConfigToLDAP() {

		ArrayList<String> objClasses = new ArrayList<>(1);
		objClasses.add("AliEnVOConfig");
		HashMap<String, Object> config = new HashMap<>();
		config.put("authPort", "8080");
		config.put("catalogPort", "8081");
		config.put("queuePort", "8082");
		config.put("logPort", "8082");
		config.put("isPort", "8082");
		config.put("clustermonitorPort", "8082");
		config.put("brokerPort", "8082");
		config.put("ldapmanager", TestConfig.ldap_suffix);
		config.put("processPort", "8082");
		config.put("processPort", "8082");
		config.put("brokerHost", "8082");
		config.put("isHost", "8082");
		config.put("logHost", "8082");
		config.put("catalogHost", "8082");
		config.put("queueHost", "8082");
		config.put("authHost", "8082");

		config.put("authenDatabase", "ADMIN");
		config.put("catalogDatabase", "8082");
		config.put("isDatabase", "8082");
		config.put("queueDatabase", "8082");
		config.put("isDbHost", "8082");
		config.put("queueDbHost", "8082");
		config.put("catalogHost", "8082");
		config.put("authenHost", "8082");
		config.put("queueDriver", "8082");
		config.put("catalogDriver", "8082");
		config.put("authenDriver", "8082");

		config.put("isDriver", "testVO/user");
		config.put("userDir", "8082");
		config.put("clusterMonitorUser", "8082");
		config.put("transferManagerAddress", "8082");
		config.put("transferBrokerAddress", "8082");
		config.put("transferOptimizerAddress", "8082");
		config.put("transferDatabase", "8082");
		config.put("jobOptimizerAddress", "8082");
		config.put("jobDatabase", "8082");
		config.put("catalogueOptimizerAddress", "8082");
		config.put("catalogueDatabase", "8082");
		config.put("lbsgAddress", "8082");
		config.put("lbsgDatabase", "8082");
		config.put("jobManagerAddress", "8082");
		config.put("jobBrokerAddress", "8082");
		config.put("authenSubject", TestConfig.certSubjectuser);
		config.put("packmanmasterAddress", "8082");
		config.put("messagesmasterAddress", "8082");
		config.put("semasterManagerAddress", "8082");
		config.put("semasterDatabase", "8082");
		config.put("jobinfoManagerAddress", "8082");
		config.put("sedefaultQosandCount", "disk=1");

		addToLDAP(objClasses, config, "ou=Config," + TestConfig.ldap_root);
	}

	private static void addToLDAP(
			final String objClass, final String attribute) {

		ArrayList<String> objClasses = new ArrayList<>(1);
		objClasses.add(objClass);
		addToLDAP(objClasses, new HashMap<String, Object>(0), attribute);

	}

	private static void addToLDAP(final ArrayList<String> objClasses,
			final HashMap<String, Object> objDesc, final String attribute) {

		BasicAttribute objClass = new BasicAttribute("objectClass", "top");
		for (String objc : objClasses)
			objClass.add(objc);

		Attributes attrs = new BasicAttributes();
		attrs.put(objClass);

		for (Map.Entry<String, Object> entry : objDesc.entrySet())
			attrs.put(new BasicAttribute(entry.getKey(), entry.getValue()));

		try {
			context.createSubcontext(attribute, attrs);
		}
		catch (@SuppressWarnings("unused") NameAlreadyBoundException e) {
			// ignore
			// System.out.println("Entry Already Exists Exception for: [" + attribute +"]");
		}
		catch (NamingException e) {
			e.printStackTrace();
		}

	}

	/**
	 * @return connected LDAP context
	 */
	public static DirContext getLdapContext() {

		DirContext ctx = null;
		try {
			Hashtable<String, String> env = new Hashtable<>();
			env.put(Context.INITIAL_CONTEXT_FACTORY,
					"com.sun.jndi.ldap.LdapCtxFactory");
			env.put(Context.SECURITY_AUTHENTICATION, "Simple");
			env.put(Context.SECURITY_PRINCIPAL, TestConfig.ldap_cred);
			// System.out.println("LDAP LOGIN:" + TestConfig.ldap_root);
			env.put(Context.SECURITY_CREDENTIALS, TestConfig.ldap_pass);
			// System.out.println("LDAP PASS:" + TestConfig.ldap_pass);
			env.put(Context.PROVIDER_URL, "ldap://localhost:"
					+ TestConfig.ldap_port + "/");

			// System.out.println("LDAP URL: ldap://localhost:" + TestConfig.ldap_port + "/");
			ctx = new InitialDirContext(env);

		}
		catch (NamingException nex) {
			System.out.println("LDAP Connection: FAILED");
			nex.printStackTrace();
		}

		return ctx;
	}

	private static final String[] ldap_conf_cn_file() {

		return new String[] { TestConfig.ldap_conf_dir + "/cn=config.ldif",
				//
				"dn: cn=config\n"
						+ "objectClass: olcGlobal\n"
						+ "cn: config\n"
						// + "olcConfigFile: slapd.conf\n"
						+ "olcConfigDir: slapd.d\n"
						+ "olcArgsFile: " + ldap_args_file + "\n"
						+ "olcAttributeOptions: lang-\n"
						+ "olcAuthzPolicy: none\n"
						+ "olcConcurrency: 0\n"
						+ "olcConnMaxPending: 100\n"
						+ "olcConnMaxPendingAuth: 1000\n"
						+ "olcGentleHUP: FALSE\n"
						+ "olcIdleTimeout: 0\n"
						+ "olcIndexSubstrIfMaxLen: 4\n"
						+ "olcIndexSubstrIfMinLen: 2\n"
						+ "olcIndexSubstrAnyLen: 4\n"
						+ "olcIndexSubstrAnyStep: 2\n"
						+ "olcIndexIntLen: 4\n"
						+ "olcLocalSSF: 71\n"
						+ "olcPidFile: " + ldap_pid_file + "\n"
						+ "olcReadOnly: FALSE\n"
						+ "olcReverseLookup: FALSE\n"
						+ "olcSaslSecProps: noplain,noanonymous\n"
						+ "olcSockbufMaxIncoming: 262143\n"
						+ "olcSockbufMaxIncomingAuth: 16777215\n"
						+ "olcThreads: 16\n"
						+ "olcTLSVerifyClient: never\n"
						+ "olcToolThreads: 1\n"
						+ "olcWriteTimeout: 0\n"
						+ "structuralObjectClass: olcGlobal\n"
						+ "entryUUID: 3ea36e88-928c-1030-8269-d50de8eaff50\n"
						+ "creatorsName: cn=config\n"
						+ "createTimestamp: 20111024130300Z\n"
						+ "entryCSN: 20111024130300.108563Z#000000#000#000000\n"
						+ "modifiersName: cn=config\n"
						+ "modifyTimestamp: 20111024130300Z\n"
				//
		};
	}

	private static final String[] ldap_conf_db_config_file() {

		return new String[] { ldap_conf_cn_dir + "/olcDatabase={0}config.ldif",
				//
				"dn: olcDatabase={0}config\n"
						+ "objectClass: olcDatabaseConfig\n"
						+ "olcDatabase: {0}config\n"
						+ "olcAccess: {0}to *  by * none\n"
						+ "olcAddContentAcl: TRUE\n"
						+ "olcLastMod: TRUE\n"
						+ "olcMaxDerefDepth: 15\n"
						+ "olcReadOnly: FALSE\n"
						// + "olcRootDN: "+TestConfig.ldap_cred+"\n"
						// + "olcRootPW: "+TestConfig.ldap_pass+"\n"
						+ "olcSyncUseSubentry: FALSE\n"
						+ "olcMonitoring: FALSE\n"
						+ "structuralObjectClass: olcDatabaseConfig\n"
						+ "entryUUID: 3ea3de2c-928c-1030-8270-d50de8eaff50\n"
						+ "creatorsName: cn=config\n"
						+ "createTimestamp: 20111024130300Z\n"
						+ "entryCSN: 20111024130300.108563Z#000000#000#000000\n"
						+ "modifiersName: cn=config\n"
						+ "modifyTimestamp: 20111024130300Z\n"
				//
		};
	}

	private static final String[] ldap_conf_db_frontend_file() {

		return new String[] { ldap_conf_cn_dir + "/olcDatabase={-1}frontend.ldif",

				//
				"dn: olcDatabase={-1}frontend\n"
						+ "objectClass: olcDatabaseConfig\n"
						+ "objectClass: olcFrontendConfig\n"
						+ "olcDatabase: {-1}frontend\n"
						+ "olcAddContentAcl: FALSE\n"
						+ "olcLastMod: TRUE\n"
						+ "olcMaxDerefDepth: 0\n"
						+ "olcReadOnly: FALSE\n"
						+ "olcSchemaDN: cn=Subschema\n"
						+ "olcSyncUseSubentry: FALSE\n"
						+ "olcMonitoring: FALSE\n"
						+ "structuralObjectClass: olcDatabaseConfig\n"
						+ "entryUUID: 3ea3db5c-928c-1030-826f-d50de8eaff50\n"
						+ "creatorsName: cn=config\n"
						+ "createTimestamp: 20111024130300Z\n"
						+ "entryCSN: 20111024130300.108563Z#000000#000#000000\n"
						+ "modifiersName: cn=config\n"
						+ "modifyTimestamp: 20111024130300Z\n"
				//
		};
	}

	private static final String[] ldap_conf_db_hdb_file() {

		return new String[] { ldap_conf_cn_dir + "/olcDatabase={1}hdb.ldif",
				//
				"dn: olcDatabase={1}hdb\n"
						+ "objectClass: olcDatabaseConfig\n"
						+ "objectClass: olcHdbConfig\n"
						+ "olcDatabase: {1}hdb\n"
						+ "olcDbDirectory: " + TestConfig.ldap_home + "\n"
						+ "olcSuffix: " + TestConfig.ldap_suffix + "\n"
						+ "olcAccess: {0}to attrs=userPassword,shadowLastChange by self write by anonymou\n"
						+ " s auth by dn=\"" + TestConfig.ldap_cred + "\" write by * none\n"
						+ "olcAccess: {1}to dn.base=\"\" by * read\n"
						+ "olcAccess: {2}to * by self write by dn=\"" + TestConfig.ldap_cred + "\" write by * read\n"
						+ "olcLastMod: TRUE\n"
						+ "olcRootDN: " + TestConfig.ldap_cred + "\n"
						+ "olcRootPW: " + TestConfig.ldap_pass + "\n"
						+ "olcDbCheckpoint: 512 30\n"
						+ "olcDbConfig: {0}set_cachesize 0 2097152 0\n"
						+ "olcDbConfig: {1}set_lk_max_objects 1500\n"
						+ "olcDbConfig: {2}set_lk_max_locks 1500\n"
						+ "olcDbConfig: {3}set_lk_max_lockers 1500\n"
						+ "olcDbIndex: objectClass eq\n"
						+ "structuralObjectClass: olcHdbConfig\n"
						+ "entryUUID: 95a9db0e-7941-1030-99f4-f301eb8bc9b9\n"
						+ "creatorsName: cn=config\n"
						+ "createTimestamp: 20110922083534Z\n"
						+ "entryCSN: 20110922083534.788341Z#000000#000#000000\n"
						+ "modifiersName: cn=config\n"
						+ "modifyTimestamp: 20110922083534Z\n"
				//
		};
	}

	private static final String[] ldap_conf_db_backend_file() {

		return new String[] { ldap_conf_cn_dir + "/olcBackend={0}hdb.ldif",
				//
				"dn: olcBackend={0}hdb\n"
						+ "objectClass: olcBackendConfig\n"
						+ "olcBackend: {0}hdb\n"
						+ "structuralObjectClass: olcBackendConfig\n"
						+ "entryUUID: 95a9d4e2-7941-1030-99f3-f301eb8bc9b9\n"
						+ "creatorsName: cn=config\n"
						+ "createTimestamp: 20110922083534Z\n"
						+ "entryCSN: 20110922083534.788182Z#000000#000#000000\n"
						+ "modifiersName: cn=config\n"
						+ "modifyTimestamp: 20110922083534Z\n"
				//
		};
	}

	private static final String[] ldap_conf_db_mod_file() {

		return new String[] { ldap_conf_cn_dir + "/cn=module{0}.ldif",
				//
				"dn: cn=module{0}\n"
						+ "objectClass: olcModuleList\n"
						+ "cn: module{0}\n"
						+ "olcModulePath: " + ldap_module_path + "\n"
						+ "olcModuleLoad: {0}back_hdb\n"
						+ "structuralObjectClass: olcModuleList\n"
						+ "entryUUID: 95a9a684-7941-1030-99f2-f301eb8bc9b9\n"
						+ "creatorsName: cn=config\n"
						+ "createTimestamp: 20110922083534Z\n"
						+ "entryCSN: 20110922083534.786996Z#000000#000#000000\n"
						+ "modifiersName: cn=config\n"
						+ "modifyTimestamp: 20110922083534Z\n"
				//
		};
	}

}
