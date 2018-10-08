package ind.xuchuanyin.tpch.tablequery;

import java.io.IOException;
import java.util.List;

import ind.xuchuanyin.tpch.jdbc.QueryClient;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

public class TableQuery {
  private static final Logger LOGGER = Logger.getLogger(TableQuery.class);
  private String loadQueryStatementPath;

  public TableQuery(String loadQueryStatementPath) {
    this.loadQueryStatementPath = loadQueryStatementPath;
  }

  public void ignite() throws Exception {
    QueryClient queryClient = new QueryClient(loadQueryStatementPath);
    queryClient.ignite();
    queryClient.close();
  }
}
