package alien.test.setup;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import alien.site.Functions;
import alien.test.TestBrain;
import alien.test.TestConfig;
import alien.test.utils.TestCommand;
import alien.test.utils.TestException;
import alien.user.UserFactory;

/**
 * @author ron
 * @since October 8, 2011
 */
public class CreateDB {

	/**
	 * the SQL socket
	 */
	public static final String sql_socket = TestConfig.sql_home + "/jalien-mysql.sock";

	/**
	 * the SQL pid file
	 */
	public static final String sql_pid_file = "/tmp/jalien-mysql.pid";

	/**
	 * the SQL log file
	 */
	public static final String sql_log = TestConfig.sql_home + "/jalien-mysql.log";

	/**
	 * the SQL connection
	 */
	public static Connection cn;

	/**
	 * @return state of rampUp
	 * @throws Exception
	 */
	public static boolean rampUpDB() throws Exception {
		// try{
		// stopDatabase();
		// } catch(Exception e){
		// ignore
		// e.printStackTrace();
		// }
		initializeDatabase();
		startDatabase();

		Thread.sleep(6000);
		connect();

		fillDatabase(mysql_passwd);
		createCatalogueDB(TestConfig.systemDB);
		createCatalogueDB(TestConfig.dataDB);
		createCatalogueDB(TestConfig.userDB);

		// fillDatabase(dataDB_content);
		// fillDatabase(userDB_content);

		catalogueInitialDirectories();

		return true;
	}

	/**
	 * @throws Exception
	 */
	public static void stopDatabase() throws Exception {

		String sql_pid = Functions.getFileContent(sql_pid_file);

		TestCommand db = new TestCommand(new String[] { TestBrain.cKill, "-9", sql_pid });
		// db.verbose();
		if (!db.exec()) {
			throw new TestException("Could not stop LDAP: \n STDOUT: " + db.getStdOut() + "\n STDERR: " + db.getStdErr());
		}
	}

	private static void initializeDatabase() throws Exception {

		Functions.writeOutFile(my_cnf, my_cnf_content);

		final TestCommand db = new TestCommand(new String[] { "mysqld", "--defaults-file=" + my_cnf, "--initialize-insecure", "--datadir=" + TestConfig.sql_home + "/data"});
		// db.verbose();
		if (!db.exec()) {
			throw new TestException("Could not initialize MySQL, STDOUT: " + db.getStdOut() + "\n STDERR: " + db.getStdErr());
		}
		// System.out.println("MYSQL install DB STDOUT: " + db.getStdOut());
		// System.out.println("MYSQL install DB STDERR: " + db.getStdErr());
	}

	/**
	 * @throws Exception
	 */
	public static void startDatabase() throws Exception {
		final TestCommand db = new TestCommand(new String[] { TestBrain.cMysqldSafe, "--defaults-file=" + my_cnf });
		db.daemonize();
		// db.verbose();
		if (!db.exec()) {
			throw new TestException("Could not start MySQL, STDOUT: " + db.getStdOut() + "\n STDERR: " + db.getStdErr());
		}
		// System.out.println("MYSQLd safe STDOUT: " + db.getStdOut());
		// System.out.println("MYSQLd safe STDERR: " + db.getStdErr());
	}

	private static void connect() throws Exception {

		Class.forName("com.mysql.jdbc.Driver");
		cn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:" + TestConfig.sql_port + "/mysql", "root", "");
	}

	/**
	 * @param username
	 * @param uid
	 * @throws Exception
	 */
	public static void addUserToDB(final String username, final String uid) throws Exception {

		userAddSubTable(username, uid);
		userAddIndexTable(username, uid);
	}

	/**
	 * @param seName
	 * @param seNumber
	 * @param site
	 * @param iodeamon
	 * @param storedir
	 * @param qos
	 * @param freespace
	 * @throws Exception
	 */
	public static void addSEtoDB(final String seName, final String seNumber, final String site, final String iodeamon, final String storedir, final String qos, final String freespace)
			throws Exception {

		String[] queries = new String[] { "USE `" + TestConfig.systemDB + "`;", "LOCK TABLES `SE` WRITE;",
				"INSERT INTO `SE` VALUES (" + seNumber + ",0,'','" + TestConfig.VO_name + "::" + site + "::" + seName + "','," + qos + ",','" + storedir + "','File',NULL,NULL,'','file://" + iodeamon
						+ "','');",
				"LOCK TABLES `SE_VOLUMES` WRITE;",
				"INSERT INTO `SE_VOLUMES` VALUES ('" + storedir + "'," + seNumber + ",0,'" + TestConfig.VO_name + "::" + site + "::" + seName + "','" + storedir + "',-1,'file://"
						+ iodeamon.substring(0, iodeamon.indexOf(':')) + "','" + freespace + "');",
				"LOCK TABLES `SERanks` WRITE;", "INSERT INTO `SERanks` VALUES ('" + site + "','" + seNumber + "','0','0');", "UNLOCK TABLES;", "USE `" + TestConfig.dataDB + "`;",
				"LOCK TABLES `SE` WRITE;",
				"INSERT INTO `SE` VALUES (" + seNumber + ",0,'','" + TestConfig.VO_name + "::" + site + "::" + seName + "','," + qos + ",','" + storedir + "','File',NULL,NULL,'','file://" + iodeamon
						+ "','');",
				"LOCK TABLES `SE_VOLUMES` WRITE;",
				"INSERT INTO `SE_VOLUMES` VALUES ('" + storedir + "'," + seNumber + ",0,'" + TestConfig.VO_name + "::" + site + "::" + seName + "','" + storedir + "',-1,'file://"
						+ iodeamon.substring(0, iodeamon.indexOf(':')) + "','" + freespace + "');",
				"LOCK TABLES `SERanks` WRITE;", "INSERT INTO `SERanks` VALUES ('" + site + "','" + seNumber + "','0','0');", "UNLOCK TABLES;", "USE `" + TestConfig.userDB + "`;",
				"LOCK TABLES `SE` WRITE;",
				"INSERT INTO `SE` VALUES (" + seNumber + ",0,'','" + TestConfig.VO_name + "::" + site + "::" + seName + "','," + qos + ",','" + storedir + "','File',NULL,NULL,'','file://" + iodeamon
						+ "','');",
				"LOCK TABLES `SE_VOLUMES` WRITE;",
				"INSERT INTO `SE_VOLUMES` VALUES ('" + storedir + "'," + seNumber + ",0,'" + TestConfig.VO_name + "::" + site + "::" + seName + "','" + storedir + "',-1,'file://"
						+ iodeamon.substring(0, iodeamon.indexOf(':')) + "','" + freespace + "');",
				"LOCK TABLES `SERanks` WRITE;", "INSERT INTO `SERanks` VALUES ('" + site + "','" + seNumber + "','0','0');", "UNLOCK TABLES;" };

		fillDatabase(queries);

	}

	private static void fillDatabase(final String[] queries) throws Exception {
		for (int a = 0; a < queries.length; a++)
			if (queries[a] != null) {
				try (Statement s = cn.createStatement()) {
					s.execute(queries[a]);
				}
			}
			else
				System.err.println("Query entry [" + a + "] null!");
	}

	private static String queryDB(final String query, final String column) {
		try (Statement s = cn.createStatement()) {
			try (ResultSet r = s.executeQuery(query)) {
				while (r.next())
					return r.getString(column);
			}
		}
		catch (Exception e) {
			System.out.println("Error in SQL query: " + query);
			e.printStackTrace();
		}

		return "";
	}

	private final static String[] mysql_passwd = { "update mysql.user set authentication_string=PASSWORD('" + TestConfig.sql_pass + "') where User='root';", "delete from mysql.user where user !='root';",
			"GRANT ALL PRIVILEGES ON *.* TO root IDENTIFIED BY '" + TestConfig.sql_pass + "' WITH GRANT OPTION;",
			"GRANT ALL PRIVILEGES ON *.* TO root@localhost IDENTIFIED BY '" + TestConfig.sql_pass + "' WITH GRANT OPTION;", "flush privileges;",

			"CREATE DATABASE IF NOT EXISTS `" + TestConfig.systemDB + "` DEFAULT CHARACTER SET latin1;",

			"CREATE DATABASE IF NOT EXISTS `ADMIN` DEFAULT CHARACTER SET latin1;",

			// "USE  mysql;", "DROP TABLE IF EXISTS `proc`;",
			// "CREATE TABLE `proc` (" + "  `db` char(64) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL DEFAULT ''," + "  `name` char(64) NOT NULL DEFAULT '',"
			// 		+ "  `type` enum('FUNCTION','PROCEDURE') NOT NULL," + "  `specific_name` char(64) NOT NULL DEFAULT ''," + "  `language` enum('SQL') NOT NULL DEFAULT 'SQL',"
			// 		+ "  `sql_data_access` enum('CONTAINS_SQL','NO_SQL','READS_SQL_DATA','MODIFIES_SQL_DATA') NOT NULL DEFAULT 'CONTAINS_SQL',"
			// 		+ "  `is_deterministic` enum('YES','NO') NOT NULL DEFAULT 'NO'," + "  `security_type` enum('INVOKER','DEFINER') NOT NULL DEFAULT 'DEFINER'," + "  `param_list` blob NOT NULL,"
			// 		+ "  `returns` longblob NOT NULL," + "  `body` longblob NOT NULL," + "  `definer` char(77) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL DEFAULT '',"
			// 		+ "  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP," + "  `modified` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',"
			// 		+ "  `sql_mode` set('REAL_AS_FLOAT','PIPES_AS_CONCAT','ANSI_QUOTES','IGNORE_SPACE','NOT_USED','ONLY_FULL_GROUP_BY','NO_UNSIGNED_SUBTRACTION','NO_DIR_IN_CREATE','POSTGRESQL','ORACLE','MSSQL','DB2','MAXDB','NO_KEY_OPTIONS','NO_TABLE_OPTIONS','NO_FIELD_OPTIONS','MYSQL323','MYSQL40','ANSI','NO_AUTO_VALUE_ON_ZERO','NO_BACKSLASH_ESCAPES','STRICT_TRANS_TABLES','STRICT_ALL_TABLES','NO_ZERO_IN_DATE','NO_ZERO_DATE','INVALID_DATES','ERROR_FOR_DIVISION_BY_ZERO','TRADITIONAL','NO_AUTO_CREATE_USER','HIGH_NOT_PRECEDENCE','NO_ENGINE_SUBSTITUTION','PAD_CHAR_TO_FULL_LENGTH') NOT NULL DEFAULT '',"
			// 		+ "  `comment` char(64) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL DEFAULT ''," + "  `character_set_client` char(32) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,"
			// 		+ "  `collation_connection` char(32) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL," + "  `db_collation` char(32) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,"
			// 		+ "  `body_utf8` longblob," + "  PRIMARY KEY (`db`,`name`,`type`)" + ") ENGINE=MyISAM DEFAULT CHARSET=utf8 COMMENT='Stored Procedures';",

			"LOCK TABLES `proc` WRITE;",
			"INSERT INTO `proc` VALUES ('" + TestConfig.systemDB
					+ "','string2binary','FUNCTION','string2binary','SQL','CONTAINS_SQL','YES','INVOKER','my_uuid varchar(36)','binary(16)','return unhex(replace(my_uuid, \\'-\\', \\'\\'))','admin@%','2011-10-06 17:07:18','2011-10-06 17:07:18','','','latin1','latin1_swedish_ci','latin1_swedish_ci','return unhex(replace(my_uuid, \\'-\\', \\'\\'))');",
			"INSERT INTO `proc` VALUES ('" + TestConfig.systemDB
					+ "','binary2string','FUNCTION','binary2string','SQL','CONTAINS_SQL','YES','INVOKER','my_uuid binary(16)','varchar(36) CHARSET latin1','return insert(insert(insert(insert(hex(my_uuid),9,0,\\'-\\'),14,0,\\'-\\'),19,0,\\'-\\'),24,0,\\'-\\')','admin@%','2011-10-06 17:07:18','2011-10-06 17:07:18','','','latin1','latin1_swedish_ci','latin1_swedish_ci','return insert(insert(insert(insert(hex(my_uuid),9,0,\\'-\\'),14,0,\\'-\\'),19,0,\\'-\\'),24,0,\\'-\\')');",
			"INSERT INTO `proc` VALUES ('" + TestConfig.systemDB
					+ "','binary2date','FUNCTION','binary2date','SQL','CONTAINS_SQL','YES','INVOKER','my_uuid binary(16)','char(16) CHARSET latin1','return upper(concat(right(left(hex(my_uuid),16),4), right(left(hex(my_uuid),12),4),left(hex(my_uuid),8)))','admin@%','2011-10-06 17:07:18','2011-10-06 17:07:18','','','latin1','latin1_swedish_ci','latin1_swedish_ci','return upper(concat(right(left(hex(my_uuid),16),4), right(left(hex(my_uuid),12),4),left(hex(my_uuid),8)))');",
			"INSERT INTO `proc` VALUES ('" + TestConfig.systemDB
					+ "','string2date','FUNCTION','string2date','SQL','CONTAINS_SQL','YES','INVOKER','my_uuid varchar(36)','char(16) CHARSET latin1','return upper(concat(right(left(my_uuid,18),4), right(left(my_uuid,13),4),left(my_uuid,8)))','admin@%','2011-10-06 17:07:19','2011-10-06 17:07:19','','','latin1','latin1_swedish_ci','latin1_swedish_ci','return upper(concat(right(left(my_uuid,18),4), right(left(my_uuid,13),4),left(my_uuid,8)))');",

			"INSERT INTO `proc` VALUES ('" + TestConfig.dataDB
					+ "','string2binary','FUNCTION','string2binary','SQL','CONTAINS_SQL','YES','INVOKER','my_uuid varchar(36)','binary(16)','return unhex(replace(my_uuid, \\'-\\', \\'\\'))','admin@%','2011-10-06 17:07:18','2011-10-06 17:07:18','','','latin1','latin1_swedish_ci','latin1_swedish_ci','return unhex(replace(my_uuid, \\'-\\', \\'\\'))');",
			"INSERT INTO `proc` VALUES ('" + TestConfig.dataDB
					+ "','binary2string','FUNCTION','binary2string','SQL','CONTAINS_SQL','YES','INVOKER','my_uuid binary(16)','varchar(36) CHARSET latin1','return insert(insert(insert(insert(hex(my_uuid),9,0,\\'-\\'),14,0,\\'-\\'),19,0,\\'-\\'),24,0,\\'-\\')','admin@%','2011-10-06 17:07:18','2011-10-06 17:07:18','','','latin1','latin1_swedish_ci','latin1_swedish_ci','return insert(insert(insert(insert(hex(my_uuid),9,0,\\'-\\'),14,0,\\'-\\'),19,0,\\'-\\'),24,0,\\'-\\')');",
			"INSERT INTO `proc` VALUES ('" + TestConfig.dataDB
					+ "','binary2date','FUNCTION','binary2date','SQL','CONTAINS_SQL','YES','INVOKER','my_uuid binary(16)','char(16) CHARSET latin1','return upper(concat(right(left(hex(my_uuid),16),4), right(left(hex(my_uuid),12),4),left(hex(my_uuid),8)))','admin@%','2011-10-06 17:07:18','2011-10-06 17:07:18','','','latin1','latin1_swedish_ci','latin1_swedish_ci','return upper(concat(right(left(hex(my_uuid),16),4), right(left(hex(my_uuid),12),4),left(hex(my_uuid),8)))');",
			"INSERT INTO `proc` VALUES ('" + TestConfig.dataDB
					+ "','string2date','FUNCTION','string2date','SQL','CONTAINS_SQL','YES','INVOKER','my_uuid varchar(36)','char(16) CHARSET latin1','return upper(concat(right(left(my_uuid,18),4), right(left(my_uuid,13),4),left(my_uuid,8)))','admin@%','2011-10-06 17:07:19','2011-10-06 17:07:19','','','latin1','latin1_swedish_ci','latin1_swedish_ci','return upper(concat(right(left(my_uuid,18),4), right(left(my_uuid,13),4),left(my_uuid,8)))');",

			"INSERT INTO `proc` VALUES ('" + TestConfig.userDB
					+ "','string2binary','FUNCTION','string2binary','SQL','CONTAINS_SQL','YES','INVOKER','my_uuid varchar(36)','binary(16)','return unhex(replace(my_uuid, \\'-\\', \\'\\'))','admin@%','2011-10-06 17:07:18','2011-10-06 17:07:18','','','latin1','latin1_swedish_ci','latin1_swedish_ci','return unhex(replace(my_uuid, \\'-\\', \\'\\'))');",
			"INSERT INTO `proc` VALUES ('" + TestConfig.userDB
					+ "','binary2string','FUNCTION','binary2string','SQL','CONTAINS_SQL','YES','INVOKER','my_uuid binary(16)','varchar(36) CHARSET latin1','return insert(insert(insert(insert(hex(my_uuid),9,0,\\'-\\'),14,0,\\'-\\'),19,0,\\'-\\'),24,0,\\'-\\')','admin@%','2011-10-06 17:07:18','2011-10-06 17:07:18','','','latin1','latin1_swedish_ci','latin1_swedish_ci','return insert(insert(insert(insert(hex(my_uuid),9,0,\\'-\\'),14,0,\\'-\\'),19,0,\\'-\\'),24,0,\\'-\\')');",
			"INSERT INTO `proc` VALUES ('" + TestConfig.userDB
					+ "','binary2date','FUNCTION','binary2date','SQL','CONTAINS_SQL','YES','INVOKER','my_uuid binary(16)','char(16) CHARSET latin1','return upper(concat(right(left(hex(my_uuid),16),4), right(left(hex(my_uuid),12),4),left(hex(my_uuid),8)))','admin@%','2011-10-06 17:07:18','2011-10-06 17:07:18','','','latin1','latin1_swedish_ci','latin1_swedish_ci','return upper(concat(right(left(hex(my_uuid),16),4), right(left(hex(my_uuid),12),4),left(hex(my_uuid),8)))');",
			"INSERT INTO `proc` VALUES ('" + TestConfig.userDB
					+ "','string2date','FUNCTION','string2date','SQL','CONTAINS_SQL','YES','INVOKER','my_uuid varchar(36)','char(16) CHARSET latin1','return upper(concat(right(left(my_uuid,18),4), right(left(my_uuid,13),4),left(my_uuid,8)))','admin@%','2011-10-06 17:07:19','2011-10-06 17:07:19','','','latin1','latin1_swedish_ci','latin1_swedish_ci','return upper(concat(right(left(my_uuid,18),4), right(left(my_uuid,13),4),left(my_uuid,8)))');",

			"UNLOCK TABLES;",

	};

	private final static String my_cnf = TestConfig.sql_home + "/my.cnf";

	private final static String my_cnf_content = "[mysqld]\n" +
                                               "user="    + UserFactory.getUserName() + "\n" +
                                               "datadir=" + TestConfig.sql_home + "/data" + "\n" +
                                               "port="    + TestConfig.sql_port + "\n" +
                                               "socket="  + sql_socket + "\n" +
                                               "\n" +

                                               "[mysqld_safe]\n" +
                                               "log-error="      + sql_log + "\n" +
                                               "pid-file="       + sql_pid_file + "\n" +
                                               "\n" +

                                               "[client]\n" +
                                               "port="   + TestConfig.sql_port + "\n" +
                                               "user="   + UserFactory.getUserName() + "\n" +
                                               "socket=" + sql_socket + "\n" +
                                               "\n" +

                                               "[mysqladmin]\n" +
                                               "user=root\n" +
                                               "port="       + TestConfig.sql_port + "\n" +
                                               "socket="     + sql_socket + "\n" +
                                               "\n" +

                                               "[mysql]\n" +
                                               "port="     + TestConfig.sql_port + "\n" +
                                               "socket="   + sql_socket + "\n" +
                                               "\n" +

                                               "[mysql_install_db]\n" +
                                               "user="    + UserFactory.getUserName() + "\n" +
                                               "port="    + TestConfig.sql_port + "\n" +
                                               "datadir=" + TestConfig.sql_home + "/data" + "\n" +
                                               "socket="  + sql_socket + "\n" +
                                               "\n" + "\n";

	private static void createCatalogueDB(String catDB) throws Exception {

		fillDatabase(new String[] { "CREATE DATABASE IF NOT EXISTS `" + catDB + "` DEFAULT CHARACTER SET latin1;", "USE `" + catDB + "`;",

        "DROP TABLE IF EXISTS `SEDistance`;",
        "CREATE TABLE `SEDistance` (`sitename` varchar(32) NOT NULL DEFAULT '', "
            + "`senumber` int(11) NOT NULL DEFAULT '0',"
            + "`distance` float DEFAULT NULL,"
            + "`updated` int(11) DEFAULT '0',"
            + "PRIMARY KEY(`sitename`,`senumber`)"
            + ") ENGINE = MyISAM DEFAULT CHARSET = latin1;",


				"DROP TABLE IF EXISTS `ACL`;",
				"CREATE TABLE `ACL` (" + "  `entryId` int(11) NOT NULL AUTO_INCREMENT," + "  `owner` char(10) COLLATE latin1_general_cs NOT NULL," + "  `aclId` int(11) NOT NULL,"
						+ "  `perm` char(4) COLLATE latin1_general_cs NOT NULL," + "  PRIMARY KEY (`entryId`)" + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

				"DROP TABLE IF EXISTS `ACTIONS`;", "CREATE TABLE `ACTIONS` (" + "  `action` char(40) COLLATE latin1_general_cs NOT NULL," + "  `todo` int(1) NOT NULL DEFAULT '0',"
						+ "  PRIMARY KEY (`action`)" + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

				"DROP TABLE IF EXISTS `COLLECTIONS`;", "CREATE TABLE `COLLECTIONS` (" + "  `collectionId` int(11) NOT NULL AUTO_INCREMENT," + "  `collGUID` binary(16) DEFAULT NULL,"
						+ "  PRIMARY KEY (`collectionId`)" + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

				"DROP TABLE IF EXISTS `COLLECTIONS_ELEM`;",
				"CREATE TABLE `COLLECTIONS_ELEM` (" + "  `collectionId` int(11) NOT NULL," + "  `localName` varchar(255) COLLATE latin1_general_cs DEFAULT NULL,"
						+ "  `data` varchar(255) COLLATE latin1_general_cs DEFAULT NULL," + "  `origLFN` varchar(255) COLLATE latin1_general_cs DEFAULT NULL," + "  `guid` binary(16) DEFAULT NULL,"
						+ "  KEY `collectionId` (`collectionId`)" + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

				"DROP TABLE IF EXISTS `CONSTANTS`;", "CREATE TABLE `CONSTANTS` (" + "  `name` varchar(100) COLLATE latin1_general_cs NOT NULL," + "  `value` int(11) DEFAULT NULL,"
						+ "  PRIMARY KEY (`name`)" + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

				"DROP TABLE IF EXISTS `ENVIRONMENT`;", "CREATE TABLE `ENVIRONMENT` (" + "  `userName` char(20) COLLATE latin1_general_cs NOT NULL,"
						+ "  `env` char(255) COLLATE latin1_general_cs DEFAULT NULL," + "  PRIMARY KEY (`userName`)" + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

				"DROP TABLE IF EXISTS `FQUOTAS`;",
				"CREATE TABLE `FQUOTAS` (" + "  `user` varchar(64) COLLATE latin1_general_cs NOT NULL," + "  `maxNbFiles` int(11) NOT NULL DEFAULT '0'," + "  `nbFiles` int(11) NOT NULL DEFAULT '0',"
						+ "  `tmpIncreasedTotalSize` bigint(20) NOT NULL DEFAULT '0'," + "  `maxTotalSize` bigint(20) NOT NULL DEFAULT '0'," + "  `tmpIncreasedNbFiles` int(11) NOT NULL DEFAULT '0',"
						+ "  `totalSize` bigint(20) NOT NULL DEFAULT '0'," + "  PRIMARY KEY (`user`)" + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

				"DROP TABLE IF EXISTS `G0L`;",
				"CREATE TABLE `G0L` (" + "  `guidId` int(11) NOT NULL AUTO_INCREMENT," + "  `ctime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
						+ "  `owner` varchar(20) COLLATE latin1_general_cs DEFAULT NULL," + "  `ref` int(11) DEFAULT '0'," + "  `jobid` bigint DEFAULT NULL,"
						+ "  `seStringlist` varchar(255) COLLATE latin1_general_cs NOT NULL DEFAULT ','," + "  `seAutoStringlist` varchar(255) COLLATE latin1_general_cs NOT NULL DEFAULT ',',"
						+ "  `aclId` int(11) DEFAULT NULL," + "  `expiretime` datetime DEFAULT NULL," + "  `size` bigint(20) NOT NULL DEFAULT '0',"
						+ "  `gowner` varchar(20) COLLATE latin1_general_cs DEFAULT NULL," + "  `guid` binary(16) DEFAULT NULL," + "  `type` char(1) COLLATE latin1_general_cs DEFAULT NULL,"
						+ "  `md5` varchar(32) COLLATE latin1_general_cs DEFAULT NULL," + "  `perm` char(3) COLLATE latin1_general_cs DEFAULT NULL," + "  PRIMARY KEY (`guidId`),"
						+ "  UNIQUE KEY `guid` (`guid`)," + "  KEY `seStringlist` (`seStringlist`)," + "  KEY `ctime` (`ctime`)"
						+ ") ENGINE=MyISAM AUTO_INCREMENT=34 DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

				"DROP TABLE IF EXISTS `G0L_PFN`;", "CREATE TABLE `G0L_PFN` (" + "  `guidId` int(11) NOT NULL," + "  `pfn` varchar(255) COLLATE latin1_general_cs DEFAULT NULL,"
						+ "  `seNumber` int(11) NOT NULL," + "  KEY `guid_ind` (`guidId`)," + "  KEY `seNumber` (`seNumber`)" + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

				"DROP TABLE IF EXISTS `G0L_QUOTA`;", "CREATE TABLE `G0L_QUOTA` (" + "  `user` varchar(64) COLLATE latin1_general_cs NOT NULL," + "  `nbFiles` int(11) NOT NULL,"
						+ "  `totalSize` bigint(20) NOT NULL," + "  KEY `user_ind` (`user`)" + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

				"DROP TABLE IF EXISTS `G0L_REF`;", "CREATE TABLE `G0L_REF` (" + "  `guidId` int(11) NOT NULL," + "  `lfnRef` varchar(20) COLLATE latin1_general_cs NOT NULL,"
						+ "  KEY `guidId` (`guidId`)," + "  KEY `lfnRef` (`lfnRef`)" + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

				"DROP TABLE IF EXISTS `GL_ACTIONS`;",
				"CREATE TABLE `GL_ACTIONS` (" + "  `tableNumber` int(11) NOT NULL," + "  `time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,"
						+ "  `action` char(40) COLLATE latin1_general_cs NOT NULL," + "  `extra` varchar(255) COLLATE latin1_general_cs DEFAULT NULL,"
						+ "  UNIQUE KEY `tableNumber` (`tableNumber`,`action`)" + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

				"DROP TABLE IF EXISTS `GL_STATS`;", "CREATE TABLE `GL_STATS` (" + "  `tableNumber` int(11) NOT NULL," + "  `seNumFiles` bigint(20) DEFAULT NULL," + "  `seNumber` int(11) NOT NULL,"
						+ "  `seUsedSpace` bigint(20) DEFAULT NULL," + "  UNIQUE KEY `tableNumber` (`tableNumber`,`seNumber`)" + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

				"DROP TABLE IF EXISTS `GROUPS`;",
				"CREATE TABLE `GROUPS` (" + "  `Userid` int(11) NOT NULL AUTO_INCREMENT," + "  `PrimaryGroup` int(1) DEFAULT NULL," + "  `Groupname` char(85) COLLATE latin1_general_cs DEFAULT NULL,"
						+ "  `Username` char(20) COLLATE latin1_general_cs NOT NULL," + "  PRIMARY KEY (`Userid`)"
						+ ") ENGINE=MyISAM AUTO_INCREMENT=5 DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

				"DROP TABLE IF EXISTS `GUIDINDEX`;",
				"CREATE TABLE `GUIDINDEX` (" + "  `indexId` int(11) NOT NULL AUTO_INCREMENT," + "  `hostIndex` int(11) DEFAULT NULL," + "  `tableName` int(11) DEFAULT NULL,"
						+ "  `guidTime` char(16) COLLATE latin1_general_cs DEFAULT 'NULL'," + "  `guidTime2` char(8) COLLATE latin1_general_cs DEFAULT 'NULL'," + "  PRIMARY KEY (`indexId`),"
						+ "  UNIQUE KEY `guidTime` (`guidTime`)" + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

				"DROP TABLE IF EXISTS `INDEXTABLE`;",
				"CREATE TABLE `INDEXTABLE` (" + "  `indexId` int(11) NOT NULL AUTO_INCREMENT," + "  `hostIndex` int(11) NOT NULL," + "  `tableName` int(11) NOT NULL,"
						+ "  `lfn` varchar(255) COLLATE latin1_general_cs DEFAULT NULL," + "  PRIMARY KEY (`indexId`)," + "  UNIQUE KEY `lfn` (`lfn`)"
						+ ") ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",
				
				"DROP TABLE IF EXISTS `INDEXTABLE_UPDATE`;",
				"CREATE TABLE `INDEXTABLE_UPDATE` (" + " `entryId` ENUM(" + "`1`) NOT NULL PRIMARY KEY," + " `last_updated` TIMESTAMP);",

				"DROP TABLE IF EXISTS `L0L`;",
				"CREATE TABLE `L0L` (" + "  `entryId` bigint(11) NOT NULL AUTO_INCREMENT," + "  `owner` varchar(20) COLLATE latin1_general_cs NOT NULL,"
						+ "  `replicated` smallint(1) NOT NULL DEFAULT '0'," + "  `ctime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
						+ "  `guidtime` varchar(8) COLLATE latin1_general_cs DEFAULT NULL," + "  `jobid` bigint DEFAULT NULL," + "  `aclId` mediumint(11) DEFAULT NULL,"
						+ "  `lfn` varchar(255) COLLATE latin1_general_cs DEFAULT NULL," + "  `broken` smallint(1) NOT NULL DEFAULT '0'," + "  `expiretime` datetime DEFAULT NULL,"
						+ "  `size` bigint(20) NOT NULL DEFAULT '0'," + "  `dir` bigint(11) DEFAULT NULL," + "  `gowner` varchar(20) COLLATE latin1_general_cs NOT NULL,"
						+ "  `type` char(1) COLLATE latin1_general_cs NOT NULL DEFAULT 'f'," + "  `guid` binary(16) DEFAULT NULL," + "  `md5` varchar(32) COLLATE latin1_general_cs DEFAULT NULL,"
						+ "  `perm` char(3) COLLATE latin1_general_cs NOT NULL," + "  PRIMARY KEY (`entryId`)," + "  UNIQUE KEY `lfn` (`lfn`)," + "  KEY `dir` (`dir`)," + "  KEY `guid` (`guid`),"
						+ "  KEY `type` (`type`)," + "  KEY `ctime` (`ctime`)," + "  KEY `guidtime` (`guidtime`)"
						+ ") ENGINE=MyISAM AUTO_INCREMENT=42 DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

				"DROP TABLE IF EXISTS `L0L_QUOTA`;", "CREATE TABLE `L0L_QUOTA` (" + "  `user` varchar(64) COLLATE latin1_general_cs NOT NULL," + "  `nbFiles` int(11) NOT NULL,"
						+ "  `totalSize` bigint(20) NOT NULL," + "  KEY `user_ind` (`user`)" + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

				"DROP TABLE IF EXISTS `L0L_broken`;",
				"CREATE TABLE `L0L_broken` (" + "  `entryId` bigint(11) NOT NULL," + "  PRIMARY KEY (`entryId`)" + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

				"DROP TABLE IF EXISTS `LFN_BOOKED`;",
				"CREATE TABLE `LFN_BOOKED` (" + "  `lfn` varchar(255) COLLATE latin1_general_cs NOT NULL DEFAULT '',"
						+ "  `owner` varchar(20) CHARACTER SET latin1 COLLATE latin1_general_ci DEFAULT NULL," + "  `quotaCalculated` smallint(6) DEFAULT NULL,"
						+ "  `existing` smallint(1) DEFAULT NULL," + "  `jobid` bigint DEFAULT NULL," + "  `md5sum` varchar(32) COLLATE latin1_general_cs DEFAULT NULL,"
						+ "  `expiretime` int(11) DEFAULT NULL," + "  `size` bigint(20) DEFAULT NULL," + "  `pfn` varchar(255) COLLATE latin1_general_cs NOT NULL DEFAULT '',"
						+ "  `se` varchar(100) CHARACTER SET latin1 COLLATE latin1_general_ci DEFAULT NULL," + "  `gowner` varchar(20) COLLATE latin1_general_cs DEFAULT NULL,"
						+ "  `user` varchar(20) CHARACTER SET latin1 COLLATE latin1_general_ci DEFAULT NULL," + "  `guid` binary(16) NOT NULL DEFAULT '\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0',"
						+ "  PRIMARY KEY (`lfn`,`pfn`,`guid`)," + "  KEY `pfn` (`pfn`)," + "  KEY `guid` (`guid`)," + "  KEY `jobid` (`jobid`)"
						+ ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

				"DROP TABLE IF EXISTS `LFN_UPDATES`;",
				"CREATE TABLE `LFN_UPDATES` (" + "  `guid` binary(16) DEFAULT NULL," + "  `entryId` int(11) NOT NULL AUTO_INCREMENT," + "  `action` char(10) COLLATE latin1_general_cs DEFAULT NULL,"
						+ "  PRIMARY KEY (`entryId`)," + "  KEY `guid` (`guid`)" + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

				"DROP TABLE IF EXISTS `LL_ACTIONS`;",
				"CREATE TABLE `LL_ACTIONS` (" + "  `tableNumber` int(11) NOT NULL," + "  `time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,"
						+ "  `action` char(40) COLLATE latin1_general_cs NOT NULL," + "  `extra` varchar(255) COLLATE latin1_general_cs DEFAULT NULL,"
						+ "  UNIQUE KEY `tableNumber` (`tableNumber`,`action`)" + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

				"DROP TABLE IF EXISTS `LL_STATS`;",
				"CREATE TABLE `LL_STATS` (" + "  `tableNumber` int(11) NOT NULL," + "  `max_time` char(20) COLLATE latin1_general_cs NOT NULL,"
						+ "  `min_time` char(20) COLLATE latin1_general_cs NOT NULL," + "  UNIQUE KEY `tableNumber` (`tableNumber`)"
						+ ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

				"DROP TABLE IF EXISTS `PACKAGES`;",
				"CREATE TABLE `PACKAGES` (" + "  `fullPackageName` varchar(255) COLLATE latin1_general_cs DEFAULT NULL," + "  `lfn` varchar(255) COLLATE latin1_general_cs DEFAULT NULL,"
						+ "  `packageName` varchar(255) COLLATE latin1_general_cs DEFAULT NULL," + "  `username` varchar(20) COLLATE latin1_general_cs DEFAULT NULL,"
						+ "  `size` bigint(20) DEFAULT NULL," + "  `platform` varchar(255) COLLATE latin1_general_cs DEFAULT NULL,"
						+ "  `packageVersion` varchar(255) COLLATE latin1_general_cs DEFAULT NULL" + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

				"DROP TABLE IF EXISTS `SE`;",
				"CREATE TABLE `SE` (" + "  `seNumber` int(11) NOT NULL AUTO_INCREMENT," + "  `seMinSize` int(11) DEFAULT '0',"
						+ "  `seExclusiveWrite` varchar(300) CHARACTER SET latin1 COLLATE latin1_general_ci DEFAULT NULL,"
						+ "  `seName` varchar(60) CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL," + "  `seQoS` varchar(200) CHARACTER SET latin1 COLLATE latin1_general_ci DEFAULT NULL,"
						+ "  `seStoragePath` varchar(255) COLLATE latin1_general_cs DEFAULT NULL," + "  `seType` varchar(60) COLLATE latin1_general_cs DEFAULT NULL,"
						+ "  `seNumFiles` bigint(20) DEFAULT NULL," + "  `seUsedSpace` bigint(20) DEFAULT NULL,"
						+ "  `seExclusiveRead` varchar(300) CHARACTER SET latin1 COLLATE latin1_general_ci DEFAULT NULL," + "  `seioDaemons` varchar(255) COLLATE latin1_general_cs DEFAULT NULL,"
						+ "  `seVersion` varchar(300) COLLATE latin1_general_cs DEFAULT NULL," + "  PRIMARY KEY (`seNumber`)," + "  UNIQUE KEY `seName` (`seName`)"
						+ ") ENGINE=MyISAM AUTO_INCREMENT=6 DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

				"DROP TABLE IF EXISTS `SERanks`;", "CREATE TABLE `SERanks` (" + "  `sitename` varchar(100) CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL," + "  `seNumber` int(11) NOT NULL,"
						+ "  `updated` smallint(1) DEFAULT NULL," + "  `rank` smallint(7) NOT NULL" + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

				"DROP TABLE IF EXISTS `SE_VOLUMES`;",
				"CREATE TABLE `SE_VOLUMES` (" + "  `volume` char(255) COLLATE latin1_general_cs NOT NULL," + "  `volumeId` int(11) NOT NULL AUTO_INCREMENT," + "  `usedspace` bigint(20) DEFAULT NULL,"
						+ "  `seName` char(255) CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL," + "  `mountpoint` char(255) COLLATE latin1_general_cs DEFAULT NULL,"
						+ "  `size` bigint(20) DEFAULT NULL," + "  `method` char(255) COLLATE latin1_general_cs DEFAULT NULL," + "  `freespace` bigint(20) DEFAULT NULL,"
						+ "  PRIMARY KEY (`volumeId`)," + "  KEY `seName` (`seName`)," + "  KEY `volume` (`volume`)"
						+ ") ENGINE=MyISAM AUTO_INCREMENT=5 DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

				"DROP TABLE IF EXISTS `HOSTS`;",
				"CREATE TABLE `HOSTS` (" + "  `hostIndex` int(11) NOT NULL AUTO_INCREMENT," + "  `address` char(50) COLLATE latin1_general_cs," + "  `db` char(40) COLLATE latin1_general_cs,"
						+ "  `driver` char(10) COLLATE latin1_general_cs," + "  `organisation` char(40) COLLATE latin1_general_cs," + "  PRIMARY KEY (`hostIndex`)"
						+ ") ENGINE=MyISAM AUTO_INCREMENT=2 DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		});
	}

	private static void addToINDEXTABLE(final String hostIndex, final String tableName, final String lfn) throws Exception {
		fillDatabase(new String[] { "USE `" + TestConfig.systemDB + "`;", "LOCK TABLES `INDEXTABLE` WRITE;", "INSERT INTO `INDEXTABLE` VALUES (0," + hostIndex + "," + tableName + ",'" + lfn + "')",
				"USE `" + TestConfig.dataDB + "`;", "LOCK TABLES `INDEXTABLE` WRITE;", "INSERT INTO `INDEXTABLE` VALUES (0," + hostIndex + "," + tableName + ",'" + lfn + "')",
				"USE `" + TestConfig.userDB + "`;", "LOCK TABLES `INDEXTABLE` WRITE;", "INSERT INTO `INDEXTABLE` VALUES (0," + hostIndex + "," + tableName + ",'" + lfn + "')", "UNLOCK TABLES;" });
	}

	private static void addToINDEXTABLE_UPDATE() throws Exception {
		fillDatabase(new String[] { "USE `" + TestConfig.systemDB + "`;", "LOCK TABLES `INDEXTABLE_UPDATE` WRITE;", "INSERT INTO `INDEXTABLE_UPDATE` (" + "`last_updated`) VALUES (NOW())",
				"USE `" + TestConfig.dataDB + "`;", "LOCK TABLES `INDEXTABLE_UPDATE` WRITE;", "INSERT INTO `INDEXTABLE_UPDATE` (" + "`last_updated`) VALUES (NOW())",
				"USE `" + TestConfig.userDB + "`;", "LOCK TABLES `INDEXTABLE_UPDATE` WRITE;", "INSERT INTO `INDEXTABLE_UPDATE` (" + "`last_updated`) VALUES (NOW())", "UNLOCK TABLES;" });
	}

	private static void addToHOSTSTABLE(final String hostIndex, final String address, final String db) throws Exception {
		fillDatabase(new String[] { "USE `" + TestConfig.systemDB + "`;", "LOCK TABLES `HOSTS` WRITE;", "INSERT INTO `HOSTS` VALUES (" + hostIndex + ",'" + address + "','" + db + "','mysql',NULL);",
				"USE `" + TestConfig.dataDB + "`;", "LOCK TABLES `HOSTS` WRITE;", "INSERT INTO `HOSTS` VALUES (" + hostIndex + ",'" + address + "','" + db + "','mysql',NULL);",
				"USE `" + TestConfig.userDB + "`;", "LOCK TABLES `HOSTS` WRITE;", "INSERT INTO `HOSTS` VALUES (" + hostIndex + ",'" + address + "','" + db + "','mysql',NULL);", "UNLOCK TABLES;" });
	}

	private static void addToGUIDINDEXTABLE(final String indexId, final String hostIndex, final String tableName, final String guidTime, final String guidTime2) throws Exception {
		fillDatabase(new String[] { "USE `" + TestConfig.systemDB + "`;", "LOCK TABLES `GUIDINDEX` WRITE",
				"INSERT INTO `GUIDINDEX` VALUES (" + indexId + "," + hostIndex + "," + tableName + ",'" + guidTime + "','" + guidTime2 + "');", "UNLOCK TABLES;", "USE `" + TestConfig.dataDB + "`;",
				"LOCK TABLES `GUIDINDEX` WRITE", "INSERT INTO `GUIDINDEX` VALUES (" + indexId + "," + hostIndex + "," + tableName + ",'" + guidTime + "','" + guidTime2 + "');", "UNLOCK TABLES;",
				"USE `" + TestConfig.userDB + "`;", "LOCK TABLES `GUIDINDEX` WRITE",
				"INSERT INTO `GUIDINDEX` VALUES (" + indexId + "," + hostIndex + "," + tableName + ",'" + guidTime + "','" + guidTime2 + "');", "UNLOCK TABLES;" });
	}

	private static void catalogueInitialDirectories() throws Exception {

		addToGUIDINDEXTABLE("1", "1", "0", "", "");
		addToINDEXTABLE("1", "0", "/");
		addToINDEXTABLE("2", "0", TestConfig.base_home_dir);
		addToINDEXTABLE_UPDATE();
		addToHOSTSTABLE("1", TestConfig.VO_name + ":" + TestConfig.sql_port, TestConfig.dataDB);
		addToHOSTSTABLE("2", TestConfig.VO_name + ":" + TestConfig.sql_port, TestConfig.userDB);

		String[] subfolders = TestConfig.base_home_dir.split("/");

		fillDatabase(new String[] { "USE `" + TestConfig.dataDB + "`;", "LOCK TABLES `L0L` WRITE;",
				"INSERT INTO `L0L` VALUES (0,'admin',0,'2011-10-06 17:07:26',NULL,NULL,NULL,'',0,NULL,0,NULL,'admin','d',NULL,NULL,'755');", "UNLOCK TABLES;" });
		String parentDir = queryDB("select entryId from " + TestConfig.dataDB + ".L0L where lfn = '';", "entryId");

		String path = "";
		for (int a = 1; a < subfolders.length; a++) {

			path = path + subfolders[a] + "/";
			fillDatabase(new String[] { "USE `" + TestConfig.dataDB + "`;", "LOCK TABLES `L0L` WRITE;",
					"INSERT INTO `L0L` VALUES (0,'admin',0,'2011-10-06 17:07:26',NULL,NULL,NULL,'" + path + "',0,NULL,0," + parentDir + ",'admin','d',NULL,NULL,'755');", "UNLOCK TABLES;" });
			parentDir = queryDB("select entryId from " + TestConfig.dataDB + ".L0L where lfn = '" + path + "';", "entryId");
		}

		parentDir = queryDB("select entryId from " + TestConfig.dataDB + ".L0L where lfn = '" + path + "';", "entryId");

		fillDatabase(new String[] { "UNLOCK TABLES;", "USE `" + TestConfig.userDB + "`;", "LOCK TABLES `L0L` WRITE;",
				"INSERT INTO `L0L` VALUES (0,'admin',0,'2011-10-06 17:07:26',NULL,NULL,NULL,'',0,NULL,0," + parentDir + ",'admin','d',NULL,NULL,'755');", "UNLOCK TABLES;" });

	}

	private static void userAddSubTable(String username, String uid) throws Exception {

		String parentDir = queryDB("select entryId from " + TestConfig.userDB + ".L0L where lfn = '';", "entryId");

		addToINDEXTABLE("2", uid, CreateLDAP.getUserHome(username));

		fillDatabase(new String[] { "USE `" + TestConfig.userDB + "`;", "LOCK TABLES `L0L` WRITE;",
				"INSERT INTO `L0L` VALUES (0,'admin',0,'2011-10-06 17:07:26',NULL,NULL,NULL,'" + username.substring(0, 1) + "/',0,NULL,0," + parentDir + ",'admin','d',NULL,NULL,'755');",
				"UNLOCK TABLES;", });

		parentDir = queryDB("select entryId from " + TestConfig.userDB + ".L0L where lfn = '" + username.substring(0, 1) + "/';", "entryId");

		fillDatabase(new String[] { "USE `" + TestConfig.userDB + "`;", "LOCK TABLES `L0L` WRITE;", "INSERT INTO `L0L` VALUES (0,'" + username + "',0,'2011-10-06 17:07:26',NULL,NULL,NULL,'"
				+ username.substring(0, 1) + "/" + username + "/',0,NULL,0," + parentDir + ",'admin','d',NULL,NULL,'755');", "UNLOCK TABLES;", });
	}

	private static void userAddIndexTable(String username, String uid) throws Exception {

		String parentDir = queryDB("select entryId from " + TestConfig.userDB + ".L0L where lfn = '" + username.substring(0, 1) + "/';", "entryId");

		fillDatabase(new String[] {

				"USE `" + TestConfig.userDB + "`;", "DROP TABLE IF EXISTS `L" + uid + "L`;",
				"CREATE TABLE `L" + uid + "L` (" + "  `entryId` bigint(11) NOT NULL AUTO_INCREMENT," + "  `owner` varchar(20) COLLATE latin1_general_cs NOT NULL,"
						+ "  `replicated` smallint(1) NOT NULL DEFAULT '0'," + "  `ctime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
						+ "  `guidtime` varchar(8) COLLATE latin1_general_cs DEFAULT NULL," + "  `jobid` bigint DEFAULT NULL," + "  `aclId` mediumint(11) DEFAULT NULL,"
						+ "  `lfn` varchar(255) COLLATE latin1_general_cs DEFAULT NULL," + "  `broken` smallint(1) NOT NULL DEFAULT '0'," + "  `expiretime` datetime DEFAULT NULL,"
						+ "  `size` bigint(20) NOT NULL DEFAULT '0'," + "  `dir` bigint(11) DEFAULT NULL," + "  `gowner` varchar(20) COLLATE latin1_general_cs NOT NULL,"
						+ "  `type` char(1) COLLATE latin1_general_cs NOT NULL DEFAULT 'f'," + "  `guid` binary(16) DEFAULT NULL," + "  `md5` varchar(32) COLLATE latin1_general_cs DEFAULT NULL,"
						+ "  `perm` char(3) COLLATE latin1_general_cs NOT NULL," + "  PRIMARY KEY (`entryId`)," + "  UNIQUE KEY `lfn` (`lfn`)," + "  KEY `dir` (`dir`)," + "  KEY `guid` (`guid`),"
						+ "  KEY `type` (`type`)," + "  KEY `ctime` (`ctime`)," + "  KEY `guidtime` (`guidtime`)"
						+ ") ENGINE=MyISAM AUTO_INCREMENT=6 DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

				"DROP TABLE IF EXISTS `L" + uid + "L_QUOTA`;", "CREATE TABLE `L" + uid + "L_QUOTA` (" + "  `user` varchar(64) COLLATE latin1_general_cs NOT NULL," + "  `nbFiles` int(11) NOT NULL,"
						+ "  `totalSize` bigint(20) NOT NULL," + "  KEY `user_ind` (`user`)" + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

				"DROP TABLE IF EXISTS `L" + uid + "L_broken`;",
				"CREATE TABLE `L" + uid + "L_broken` (" + "  `entryId` bigint(11) NOT NULL," + "  PRIMARY KEY (`entryId`)" + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

				"LOCK TABLES `L" + uid + "L` WRITE;",
				"INSERT INTO `L" + uid + "L` VALUES (0,'" + username + "',0,'2011-10-06 17:07:51',NULL,NULL,NULL,'',0,NULL,0,'" + parentDir + "','admin','d',NULL,NULL,'755');",

				"UNLOCK TABLES;",

		});
	}

}
