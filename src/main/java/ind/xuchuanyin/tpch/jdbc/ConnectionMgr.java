package ind.xuchuanyin.tpch.jdbc;

import java.sql.Connection;

import org.apache.log4j.Logger;

public class ConnectionMgr {
  private static final Logger LOGGER = Logger.getLogger(ConnectionMgr.class);
  private static final ConnectionMgr INSTANCE = new ConnectionMgr();
  private static final String DRIVER_PROPERTIES = "driver.properties";

  private ConnectionMgr() {
  }

  public static ConnectionMgr getInstance() {
    return INSTANCE;
  }

  public Connection getConnection() {
    throw new UnsupportedOperationException("Not impletemented yet");
  }
}
