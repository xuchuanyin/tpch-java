package ind.xuchuanyin.tpch.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import ind.xuchuanyin.tpch.common.Utils;
import org.apache.log4j.Logger;

public class QueryProcessor {
  private static final Logger LOGGER = Logger.getLogger(QueryProcessor.class);
  private static final ConnectionMgr connectionMgr = ConnectionMgr.getInstance();
  private ExecutorService executorService;

  // package private
  QueryProcessor(int size) {
    this.executorService = Executors.newFixedThreadPool(size);
  }

  private QueryResult internalQuery(Connection conn, QuerySlice querySlice)
      throws SQLException {
    ResultSet rs = null;
    PreparedStatement statement = null;
    try {
      statement = conn.prepareStatement(querySlice.getSql());

      long startWatch = System.currentTimeMillis();
      rs = statement.executeQuery();
      int cnt = 0;

      if (querySlice.isConsumeResult()) {
        while (rs.next()) {
          cnt++;
        }
      }
      long stopWatch = System.currentTimeMillis();

      QueryResult queryResult = QueryResult.QueryResultBuilder.aQueryResult()
          .withQuerySlice(querySlice)
          .withDuration(stopWatch - startWatch)
          .withResultSize(cnt)
          .build();
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Query result " + queryResult);
      }
      return queryResult;
    } catch (SQLException e) {
      throw e;
    } finally {
      Utils.closeQuietly(statement);
      Utils.closeQuietly(rs);
    }
  }

  public List<QueryResult> processBatch(List<QuerySlice> querySlices)
      throws InterruptedException {
    List<Future<QueryResult>> executorTaskList = new ArrayList<>();

    for (QuerySlice querySlice : querySlices) {
      QueryWorker queryWorker = new QueryWorker(querySlice);
      executorTaskList.add(executorService.submit(queryWorker));
    }

    List<QueryResult> queryResults = new ArrayList<>();
    for (int i = 0; i < executorTaskList.size(); i++) {
      try {
        queryResults.add(executorTaskList.get(i).get());
      } catch (InterruptedException | ExecutionException e) {
        LOGGER.error("Failed to execute task", e);
      }
    }

    return queryResults;
  }

  private final class QueryWorker implements Callable<QueryResult> {
    private QuerySlice querySlice;

    private QueryWorker(QuerySlice querySlice) {
      this.querySlice = querySlice;
    }

    @Override public QueryResult call() throws Exception {
      Connection conn = connectionMgr.borrowConnection();
      QueryResult result = null;

      try {
        result = internalQuery(conn, querySlice);
      } catch (SQLException e) {
        LOGGER.error("Failed to execute query", e);
      } finally {
        connectionMgr.returnConnection(conn);
      }

      return result;
    }
  }

  public void close() {
    if (null != executorService) {
      executorService.shutdown();
    }
  }
}
