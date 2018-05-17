package rosscoleman.sql.textdump;

import java.io.PrintStream;
import java.sql.*;
import java.util.List;
import java.util.ArrayList;

public class QueryDump {

    public void queryToText(Connection conn, String select, PrintStream printStream, String delimiter) {
        ResultSetMetaData metaData = null;
        try (PreparedStatement preparedStatement = conn.prepareStatement(select);
             ResultSet rs = preparedStatement.executeQuery();
        ) {
            metaData = preparedStatement.getMetaData();
            int columnCount = metaData.getColumnCount();
            List<String> columnNames = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnName(i);
                columnNames.add(columnName);
            }
            String header = String.join(delimiter, columnNames);
            printStream.println(header);

            List<String> row = null;
            while (rs.next()) {
                row = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    row.add(rs.getString(i));
                }
                String rowStr = String.join(delimiter, row);
                printStream.println(rowStr);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        final String CONN_STRING = "jdbc:postgresql://localhost/world";
        //final String DRIVER = "org.postgresql.Driver";
        final String QUERY = "select id, name, countrycode, district, population from city";

        QueryDump q = new QueryDump();
        try (Connection conn = DriverManager.getConnection(CONN_STRING, "postgres", "")) {
            q.queryToText(conn, QUERY, System.out, "\t");
        } catch (SQLException e) {

        }
    }
}
