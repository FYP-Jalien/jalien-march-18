package alien.test;

import alien.site.Functions;

/**
 * @author ron
 * @since Sep 09, 2011
 */
public class TestBrain {

	/**
	 * location of the bash binary
	 */
	static String cBash = "";

	/**
	 * location of the nohup binary
	 */
	public static String cNohup = "";

	/**
	 * location of the bash binary
	 */
	public static String cKill = "";

	/**
	 * location of the openssl binary
	 */
	public static String cOpenssl = "";

	/**
	 * location of the chmod binary
	 */
	public static String cChmod = "";

	/**
	 * location of the cp binary
	 */
	public static String cCp = "";

	/**
	 * location of the slapd binary
	 */
	public static String cSlapd = "";

	/**
	 * location of the slapd binary
	 */
	public static String cSlappasswd = "";

	/**
	 * location of the mysql binary
	 */
	public static String cMysql = "";

	/**
	 * location of the mysqld_safe binary
	 */
	public static String cMysqldSafe = "";

	/**
	 * location of the mysql_install_db binary
	 */
	public static String cMysqlInstallDB = "";

	/**
	 * @return if we found all commands
	 */
	public static boolean findCommands() {

		boolean state = true;

		cBash = Functions.which("bash");
		if (cBash == null) {
			System.err.println("Couldn't find command: bash");
			state = false;
		}

		cNohup = Functions.which("nohup");
		if (cNohup == null) {
			System.err.println("Couldn't find command: nohup");
			state = false;
		}

		cKill = Functions.which("kill");
		if (cKill == null) {
			System.err.println("Couldn't find command: kill");
			state = false;
		}

		cOpenssl = Functions.which("openssl");
		if (cOpenssl == null) {
			System.err.println("Couldn't find command: openssl");
			state = false;
		}

		cCp = Functions.which("cp");
		if (cCp == null) {
			System.err.println("Couldn't find command: cp");
			state = false;
		}

		cChmod = Functions.which("chmod");
		if (cChmod == null) {
			System.err.println("Couldn't find command: chmod");
			state = false;
		}

		cSlapd = Functions.which("slapd");
		if (cSlapd == null) {
			System.err.println("Couldn't find command: slapd");
			state = false;
		}

		cSlappasswd = Functions.which("slappasswd");
		if (cSlappasswd == null) {
			System.err.println("Couldn't find command: slappasswd");
			state = false;
		}

		cMysql = Functions.which("mysql");
		if (cMysql == null) {
			System.err.println("Couldn't find command: mysql");
			state = false;
		}

		cMysqldSafe = Functions.which("mysqld_safe");
		if (cMysqldSafe == null) {
			System.err.println("Couldn't find command: mysqld_safe");
			state = false;
		}

		cMysqlInstallDB = Functions.which("mysql_install_db");
		if (cMysqlInstallDB == null) {
			System.err.println("Couldn't find command: mysql_install_db");
			state = false;
		}

		return state;

	}

}
