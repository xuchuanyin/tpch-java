package ind.xuchuanyin.tpch.tablecreate;

import ind.xuchuanyin.tpch.jdbc.QueryClient;
import org.apache.log4j.Logger;

public class TableCreator {
  private static final Logger LOGGER = Logger.getLogger(TableCreator.class);
  private String createTableStatementPath;

  public TableCreator(String createTableStatementPath) {
    this.createTableStatementPath = createTableStatementPath;
  }

  public void ignite() throws Exception {
    QueryClient queryClient = new QueryClient(createTableStatementPath);
    queryClient.ignite();
    queryClient.close();
  }
}
