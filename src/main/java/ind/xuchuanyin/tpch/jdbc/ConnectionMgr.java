package ind.xuchuanyin.tpch.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentLinkedQueue;

import ind.xuchuanyin.tpch.common.Utils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

public class ConnectionMgr {
  private static final Logger LOGGER = Logger.getLogger(ConnectionMgr.class);
  private static final ConnectionMgr INSTANCE = new ConnectionMgr();
  private ConcurrentLinkedQueue<Connection> connections;

  private ConnectionMgr() {
    connections = new ConcurrentLinkedQueue<>();
  }

  public static ConnectionMgr getInstance() {
    return INSTANCE;
  }

  public synchronized void init(String driverName, String url, String user, String pwd, int size) {
    if (!connections.isEmpty()) {
      LOGGER.warn("Connection pool");
    }
    try {
      Class.forName(driverName);
    } catch (ClassNotFoundException e) {
      LOGGER.error("Failed to init driver", e);
      throw new IllegalArgumentException("Invalid driver: " + driverName);
    }

    for (int i = 0; i < size; i++) {
      connections.add(newConnection(url, user, pwd));
    }
  }

  private Connection newConnection(String url, String user, String pwd) {
    LOGGER.info("create new jdbc connection, timestamp is " + System.currentTimeMillis());
    Connection conn = null;
    try {
      if (StringUtils.isBlank(user)) {
        conn = DriverManager.getConnection(url);
      } else {
        conn = DriverManager.getConnection(url, user, pwd);
      }
    } catch (SQLException e) {
      LOGGER.error("Failed to create connection, time stamp is " + System.currentTimeMillis(), e);
    }
    LOGGER.error("Created connection " + conn);
    return conn;
  }

  public Connection borrowConnection() {
    Connection conn = connections.poll();
    LOGGER.error("borrow connection " + conn);
    return conn;
  }

  public void returnConnection(Connection conn) {
    LOGGER.error("return connection " + conn);
    connections.offer(conn);
  }

  public void destroy() {
    for (Connection conn : connections) {
      Utils.closeQuietly(conn);
    }
  }

}
