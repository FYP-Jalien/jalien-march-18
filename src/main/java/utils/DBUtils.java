package utils;

import java.io.Closeable;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;
import lazyj.DBFunctions.DBConnection;

/**
 * @author ibrinzoi
 * @since 2021-12-08
 */
public final class DBUtils implements Closeable {

	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(DBUtils.class.getCanonicalName());

	private DBConnection dbc = null;
	private ResultSet resultSet = null;
	private Statement stat = null;

	/**
	 * Database connection to work with
	 * 
	 * @param dbc
	 */
	public DBUtils(final DBConnection dbc) {
		this.dbc = dbc;
	}

	private void executeClose() {
		if (resultSet != null) {
			try {
				resultSet.close();
			}
			catch (@SuppressWarnings("unused") final Throwable t) {
				// ignore
			}

			resultSet = null;
		}

		if (stat != null) {
			try {
				stat.close();
			}
			catch (@SuppressWarnings("unused") final Throwable t) {
				// ignore
			}

			stat = null;
		}
	}

	/**
	 * @param query
	 * @return <code>true</code> if the query was successfully executed
	 */
	@SuppressWarnings("resource")
	public boolean executeQuery(final String query) {
		executeClose();

		try {
			stat = dbc.getConnection().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

			if (stat.execute(query, Statement.NO_GENERATED_KEYS)) {
				resultSet = stat.getResultSet();
			}
			else {
				executeClose();
			}

			return true;
		}
		catch (final SQLException e) {
			logger.log(Level.WARNING, "Failed executing this query: `" + query + "`", e);
			return false;
		}
	}

	/**
	 * Disable autocommit and lock the indicated tables (SQL statement format)
	 * 
	 * @param tables
	 */
	public void lockTables(final String tables) {
		executeQuery("SET autocommit = 0;");
		executeQuery("lock tables " + tables + ";");
	}

	/**
	 * Commit and unlock tables
	 */
	public void unlockTables() {
		executeQuery("commit;");
		executeQuery("unlock tables;");
		executeQuery("SET autocommit = 1;");

		executeClose();
	}

	/**
	 * @return the result of the last executed query
	 */
	public ResultSet getResultSet() {
		return resultSet;
	}

	@Override
	public void close() throws IOException {
		dbc.free();
	}
}