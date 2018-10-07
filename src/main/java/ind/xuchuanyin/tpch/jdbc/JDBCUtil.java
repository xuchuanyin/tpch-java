package ind.xuchuanyin.tpch.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;

public class JDBCUtil {
  private static final Logger LOGGER = Logger.getLogger(JDBCUtil.class);
  private static final JDBCUtil INSTANCE = new JDBCUtil();

  private JDBCUtil() {
  }

  public static JDBCUtil getInstance() {
    return INSTANCE;
  }

  public ResultSet executeSql(String sqlStatement) throws SQLException {
    Connection connection = ConnectionMgr.getInstance().getConnection();
    LOGGER.info("Start to execute sql " + sqlStatement);

    return connection.prepareStatement(sqlStatement).executeQuery();
  }
}
