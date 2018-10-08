package ind.xuchuanyin.tpch.dataload;

import ind.xuchuanyin.tpch.jdbc.QueryClient;
import org.apache.log4j.Logger;

public class DataLoader {
  private static final Logger LOGGER = Logger.getLogger(DataLoader.class);
  private String loadDataStatementPath;

  public DataLoader(String loadDataStatementPath) {
    this.loadDataStatementPath = loadDataStatementPath;
  }

  public void ignite() throws Exception {
    QueryClient queryClient = new QueryClient(loadDataStatementPath);
    queryClient.ignite();
    queryClient.close();
  }
}
