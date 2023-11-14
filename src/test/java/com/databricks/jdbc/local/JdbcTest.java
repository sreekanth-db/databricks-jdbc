import java.sql.*;

public class JdbcTest {
  public static void main(String[] args) {
    try {
      // Load the JDBC driver
      Class.forName("com.databricks.jdbc.driver.DatabricksDriver");
      //
      // UC disabled
      // Establish the JDBC connection
      Connection connection =
          DriverManager.getConnection(
              "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/dd43ee29fedd958d;",
              "token",
              "dapi16071cd7b86a37e0c539eaac197b3ce8");
      DatabaseMetaData metaData = connection.getMetaData();
      ResultSet catalogs = metaData.getCatalogs();
      System.out.println(metaData.getCatalogs());
      ResultSet tables = metaData.getTables("tableau", null, null, new String[] {"TABLES"});
      while (tables.next()) {
        String tableName = tables.getString("TABLE_CAT");
        System.out.println(tableName);
      }

      // Get primary key information
      ResultSet primaryKeyResultSet = metaData.getPrimaryKeys("aaa", "aaa", "aaa");

      while (primaryKeyResultSet.next()) {
        String columnName = primaryKeyResultSet.getString("COLUMN_NAME");
        System.out.println("Primary key column: " + columnName);
      }
      // Print connection success message
      System.out.println("Connection successful!");

      // Close the connection
      connection.close();

    } catch (Exception e) {
      // Print connection error message
      System.out.println("Connection failed: " + e.getMessage());
    }
  }
}
