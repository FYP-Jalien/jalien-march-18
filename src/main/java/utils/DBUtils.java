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

	private boolean correctlyClosed = true;

	/**
	 * Database connection to work with
	 * 
	 * @param dbc
	 */
	public DBUtils(final DBConnection dbc) {
		this.dbc = dbc;
		dbc.setReadOnly(false);
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
	 * @return <code>true</code> if everything went ok
	 */
	@SuppressWarnings("resource")
	public boolean lockTables(final String tables) {
		try {
			dbc.getConnection().setAutoCommit(false);
		}
		catch (SQLException e) {
			logger.log(Level.WARNING, "Cannot begin transaction", e);
			return false;
		}

		if (!executeQuery("lock tables " + tables + ";"))
			return false;

		correctlyClosed = false;

		return true;
	}

	/**
	 * Commit and unlock tables
	 * 
	 * @return <code>true</code> if everything went ok
	 */
	@SuppressWarnings("resource")
	public boolean unlockTables() {
		try {
			if (!executeQuery("unlock tables;"))
				return false;

			dbc.getConnection().commit();
			dbc.getConnection().setAutoCommit(true);

			correctlyClosed = true;

			return true;
		}
		catch (SQLException e) {
			logger.log(Level.WARNING, "Cannot commit transaction", e);
			return false;
		}
		finally {
			executeClose();
		}
	}

	/**
	 * @return the result of the last executed query
	 */
	public ResultSet getResultSet() {
		return resultSet;
	}

	@SuppressWarnings("resource")
	@Override
	public void close() throws IOException {
		if (!correctlyClosed) {
			logger.log(Level.WARNING, "Detected incomplete transaction, rolling back");

			try {
				dbc.getConnection().setAutoCommit(false);
				dbc.getConnection().rollback();
				executeQuery("unlock tables;");
			}
			catch (SQLException e) {
				logger.log(Level.WARNING, "Cannot rollback transaction", e);
				e.printStackTrace();
			}
		}

		dbc.free();
	}
}