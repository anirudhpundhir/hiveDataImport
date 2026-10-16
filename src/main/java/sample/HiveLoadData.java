package sample;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;
import java.util.regex.Pattern;

/**
 * Creates a Hive database (if needed) and loads a local file into a table in it.
 *
 * <p>Usage: {@code java sample.HiveLoadData [file-path]}. The database and table
 * names are read from stdin. Connection settings come from environment variables:
 * {@code HIVE_JDBC_URL} (default {@code jdbc:hive2://localhost:10000/default}),
 * {@code HIVE_USER} (default {@code hive}) and {@code HIVE_PASSWORD} (default empty).
 */
public final class HiveLoadData {

    static final String DEFAULT_URL = "jdbc:hive2://localhost:10000/default";
    static final String DEFAULT_FILE = "/home/sample/Customers.txt";

    // Hive identifiers: letters, digits and underscores. Names come from user input,
    // so they are validated before being concatenated into HiveQL.
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]{0,127}");

    private HiveLoadData() {
    }

    public static void main(String[] args) throws SQLException {
        String filePath = args.length > 0 ? args[0] : DEFAULT_FILE;
        String url = env("HIVE_JDBC_URL", DEFAULT_URL);
        String user = env("HIVE_USER", "hive");
        String password = env("HIVE_PASSWORD", "");

        Scanner in = new Scanner(System.in);
        String databaseName = prompt(in, "Enter the name of database");
        String tableName = prompt(in, "Enter the name of table");

        try (Connection conn = DriverManager.getConnection(url, user, password);
             Statement stmt = conn.createStatement()) {
            stmt.execute(createDatabaseSql(databaseName));
            System.out.println("Database " + databaseName + " is ready.");
            stmt.execute(useDatabaseSql(databaseName));

            String loadQuery = loadDataSql(filePath, tableName);
            System.out.println("Running " + loadQuery);
            stmt.execute(loadQuery);
            System.out.printf("Load data into %s successful%n", tableName);
        }
    }

    static String createDatabaseSql(String databaseName) {
        return "CREATE DATABASE IF NOT EXISTS " + identifier(databaseName);
    }

    static String useDatabaseSql(String databaseName) {
        return "USE " + identifier(databaseName);
    }

    static String loadDataSql(String filePath, String tableName) {
        return "LOAD DATA LOCAL INPATH '" + escapeLiteral(filePath) + "' INTO TABLE " + identifier(tableName);
    }

    static String identifier(String name) {
        if (name == null || !IDENTIFIER.matcher(name).matches()) {
            throw new IllegalArgumentException("Invalid Hive identifier: " + name);
        }
        return name;
    }

    static String escapeLiteral(String value) {
        return value.replace("\\", "\\\\").replace("'", "\\'");
    }

    private static String prompt(Scanner in, String message) {
        System.out.println(message);
        return identifier(in.next());
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
