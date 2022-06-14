package alien.catalogue;

import java.util.logging.Logger;

import lazyj.DBFunctions;
import lazyj.ExtProperties;
import lazyj.StringFactory;
import alien.config.ConfigUtils;

/**
 * One row from alice_users.HOSTS
 * 
 * @author costing
 * @since Nov 4, 2010
 */
public class Host implements Comparable<Host> {

	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(Host.class.getCanonicalName());

	/**
	 * host index
	 */
	public final int hostIndex;

	/**
	 * machine name:port
	 */
	public final String address;

	/**
	 * database name
	 */
	public final String db;

	/**
	 * driver
	 */
	public final String driver;

	/**
	 * organization
	 */
	public final String organization;

	/**
	 * JDBC URL
	 */
	public final String jdbcURL;

	private final String configFile;

	private ExtProperties dbProperties = null;

	/**
	 * Initialize from one row extracted from alice_users.HOSTS
	 * 
	 * @param db
	 * @see CatalogueUtils#getHost(int)
	 */
	Host(final DBFunctions db) {
		hostIndex = db.geti("hostIndex");
		address = StringFactory.get(db.gets("address"));
		this.db = StringFactory.get(db.gets("db"));
		driver = StringFactory.get(db.gets("driver").toLowerCase());
		organization = StringFactory.get(db.gets("organization"));
		jdbcURL = StringFactory.get(db.gets("jdbcurl"));

		final ExtProperties parent = ConfigUtils.getConfiguration("alice_users");

		if (parent != null)
			dbProperties = new ExtProperties(parent.getProperties());
		else
			dbProperties = new ExtProperties();

		if (jdbcURL.length() > 0) {
			if (jdbcURL.endsWith(".properties"))
				configFile = jdbcURL.substring(0, jdbcURL.indexOf('.'));
			else {
				configFile = null;
				dbProperties.set("url", jdbcURL);
			}
		}
		else
			configFile = null;

		dbProperties.set("database", this.db);
		dbProperties.set("host", address.substring(0, address.indexOf(':')));
		dbProperties.set("port", address.substring(address.indexOf(':') + 1));

		if (driver.indexOf("mysql") >= 0 && dbProperties.gets("driver", "").toLowerCase().indexOf("mysql") < 0)
			dbProperties.set("driver", "com.mysql.jdbc.Driver");
		else if (driver.indexOf("postgres") >= 0)
			dbProperties.set("driver", "org.postgresql.Driver");
	}

	/**
	 * Get a database connection to this host
	 * 
	 * @return a database connection to this host
	 */
	public DBFunctions getDB() {
		if (configFile != null)
			return ConfigUtils.getDB(configFile);

		return new DBFunctions(dbProperties);
	}

	@Override
	public int compareTo(final Host o) {
		return hostIndex - o.hostIndex;
	}

	@Override
	public boolean equals(final Object obj) {
		if (!(obj instanceof Host))
			return false;

		return compareTo((Host) obj) == 0;
	}

	@Override
	public int hashCode() {
		return hostIndex;
	}

	@Override
	public String toString() {
		return "Host: hostIndex: " + hostIndex + "\n" +
				"address\t: " + address + "\n" +
				"database\t: " + db + "\n" +
				"driver\t: " + driver + "\n" +
				"organization\t: " + organization;
	}
}
