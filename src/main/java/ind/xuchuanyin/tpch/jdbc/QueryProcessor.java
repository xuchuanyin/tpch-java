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
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.Transformer;
import org.apache.log4j.Logger;

public class QueryProcessor {
  private static final Logger LOGGER = Logger.getLogger(QueryProcessor.class);
  private static final ConnectionMgr connectionPool = ConnectionMgr.getInstance();
  private ExecutorService executorService = null;

  // package private
  QueryProcessor(int size) {
    this.executorService = Executors.newFixedThreadPool(size);
  }

  private QueryResult internalQuery(Connection conn, QuerySlice querySlice, String extraInfo)
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

      QueryResult queryResult = new QueryResult();
      queryResult.setDuration(stopWatch - startWatch);
      queryResult.setExtraInfo(extraInfo);
      queryResult.setOthers(String.valueOf(cnt));
      return queryResult;
    } catch (SQLException e) {
      throw e;
    } finally {
      Utils.closeQuietly(statement);
      Utils.closeQuietly(rs);
    }
  }

  public List<QueryResult> processBatch(List<QuerySlice> sqlSlices, List<String> extraInfos,
      int intervalInBatchSec) throws InterruptedException {
    if (extraInfos == null) {
      extraInfos = new ArrayList<>(sqlSlices.size());
    }

    if (extraInfos.size() < sqlSlices.size()) {
      extraInfos.addAll(CollectionUtils
          .collect(sqlSlices.subList(extraInfos.size(), sqlSlices.size()),
              new Transformer<QuerySlice, String>() {
                @Override public String transform(QuerySlice querySlice) {
                  return querySlice.toString();
                }
              }));
    }

    List<Future<QueryResult>> executorTaskList = new ArrayList<>();

    for (int i = 0; i < sqlSlices.size(); i++) {
      QueryWorker queryWorker = new QueryWorker(sqlSlices.get(i), extraInfos.get(i));
      executorTaskList.add(executorService.submit(queryWorker));
      Thread.sleep(intervalInBatchSec * 1000);
    }

    List<QueryProcessor.QueryResult> queryResults = new ArrayList<>();
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
    private String extraInfo;

    private QueryWorker(QuerySlice querySlice, String extraInfo) {
      this.querySlice = querySlice;
      this.extraInfo = extraInfo;
    }

    @Override public QueryProcessor.QueryResult call() throws Exception {
      return processSingle(querySlice, extraInfo);
    }
  }

  private QueryResult processSingle(QuerySlice querySlice, String extraInfo) {
    Connection conn = connectionPool.borrowConnection();
    QueryProcessor.QueryResult result = null;

    try {
      result = internalQuery(conn, querySlice, extraInfo);
    } catch (SQLException e) {
      LOGGER.error("Failed to execute query", e);
    } finally {
      connectionPool.returnConnection(conn);
    }

    return result;
  }

  public final class QueryResult {
    private long duration;
    private String extraInfo;
    private String others;

    @Override public String toString() {
      final StringBuffer sb = new StringBuffer("QueryResult{");
      sb.append("duration=").append(duration);
      sb.append(", extraInfo='").append(extraInfo).append('\'');
      sb.append(", others='").append(others).append('\'');
      sb.append('}');
      return sb.toString();
    }

    public long getDuration() {
      return duration;
    }

    public void setDuration(long duration) {
      this.duration = duration;
    }

    public String getExtraInfo() {
      return extraInfo;
    }

    public void setExtraInfo(String extraInfo) {
      this.extraInfo = extraInfo;
    }

    public String getOthers() {
      return others;
    }

    public void setOthers(String others) {
      this.others = others;
    }
  }
}
