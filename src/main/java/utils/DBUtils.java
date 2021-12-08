package utils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import lazyj.DBFunctions.DBConnection;

/**
 * @author ibrinzoi
 * @since 2021-12-08
 */
public final class DBUtils {

    private static ResultSet resultSet = null;
    private static Statement stat = null;

    private static final void executeClose() {
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

    public static final boolean executeQuery(DBConnection dbc, final String query) {
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
    
    public static final void lockTables(DBConnection dbc, String tables) {
        executeQuery(dbc, "SET autocommit = 0;");
        executeQuery(dbc, "lock tables " + tables + ";");
    }

    public static final void unlockTables(DBConnection dbc) {
        executeQuery(dbc, "commit;");
        executeQuery(dbc, "unlock tables;");
        executeQuery(dbc, "SET autocommit = 1;");

        executeClose();
	}

	public static ResultSet getResultSet() {
		return resultSet;
	}
}