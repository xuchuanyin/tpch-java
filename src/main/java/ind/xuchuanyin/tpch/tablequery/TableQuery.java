package ind.xuchuanyin.tpch.tablequery;

import java.io.IOException;
import java.util.List;

import ind.xuchuanyin.tpch.jdbc.JDBCUtil;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

public class TableQuery {
  private static final Logger LOGGER = Logger.getLogger(TableQuery.class);
  private String loadQueryStatementPath;

  public TableQuery(String loadQueryStatementPath) {
    this.loadQueryStatementPath = loadQueryStatementPath;
  }

  public void ignite() throws Exception {
    List<String> sqls = parseStatements();
    // todo: execute these sqls sequentially
    // todo: check how 'hive -e' works
    for (int i = 0; i < sqls.size(); i++) {
      JDBCUtil.getInstance().executeSql(sqls.get(i));
    }
  }

  private List<String> parseStatements() throws IOException {
    try {
      return FileUtils.readLines(FileUtils.getFile(loadQueryStatementPath), "utf-8");
    } catch (IOException e) {
      LOGGER.error("Exception occurs while parsing query statements", e);
      throw e;
    }
  }
}
