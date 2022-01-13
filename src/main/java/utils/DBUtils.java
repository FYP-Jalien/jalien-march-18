package utils;

import java.io.Closeable;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import lazyj.DBFunctions.DBConnection;

/**
 * @author ibrinzoi
 * @since 2021-12-08
 */
public final class DBUtils implements Closeable {

	private DBConnection dbc = null;
	private ResultSet resultSet = null;
	private Statement stat = null;

	public DBUtils(DBConnection dbc) {
		this.dbc = dbc;
	}

	private final void executeClose() {
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

	public final boolean executeQuery(final String query) {
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
			return false;
		}
	}
	
	public final void lockTables(String tables) {
		executeQuery("SET autocommit = 0;");
		executeQuery("lock tables " + tables + ";");
	}

	public final void unlockTables() {
		executeQuery("commit;");
		executeQuery("unlock tables;");
		executeQuery("SET autocommit = 1;");

		executeClose();
	}

	public ResultSet getResultSet() {
		return resultSet;
	}

	@Override
	public void close() throws IOException {
		dbc.free();
	}
}