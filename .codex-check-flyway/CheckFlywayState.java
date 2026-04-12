import java.sql.*;

public class CheckFlywayState {
  public static void main(String[] args) throws Exception {
    String url = "jdbc:mysql://localhost:3306/order-service?useUnicode=true&characterEncoding=UTF-8";
    try (Connection c = DriverManager.getConnection(url, "root", "root")) {
      DatabaseMetaData md = c.getMetaData();
      try (ResultSet rs = md.getTables(null, null, "flyway_schema_history", null)) {
        System.out.println("HAS_TABLE=" + rs.next());
      }
      try (Statement st = c.createStatement()) {
        try (ResultSet rs = st.executeQuery("SELECT installed_rank, version, description, script, checksum, success FROM flyway_schema_history ORDER BY installed_rank")) {
          while (rs.next()) {
            System.out.println(
              "ROW rank=" + rs.getInt(1)
              + " version=" + rs.getString(2)
              + " desc=" + rs.getString(3)
              + " script=" + rs.getString(4)
              + " checksum=" + rs.getString(5)
              + " success=" + rs.getBoolean(6)
            );
          }
        } catch (SQLException ex) {
          System.out.println("QUERY_ERROR=" + ex.getMessage());
        }
      }
    }
  }
}
