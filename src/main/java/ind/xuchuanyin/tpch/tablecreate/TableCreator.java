package ind.xuchuanyin.tpch.tablecreate;

import java.io.IOException;
import java.util.List;

import ind.xuchuanyin.tpch.jdbc.JDBCUtil;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

public class TableCreator {
  private static final Logger LOGGER = Logger.getLogger(TableCreator.class);
  private String createTableStatementPath;

  public TableCreator(String createTableStatementPath) {
    this.createTableStatementPath = createTableStatementPath;
  }

  public void ignite() throws Exception {
    List<String> sqls = parsingStatements();
    // todo: execute these sqls sequentially
    // todo: check how 'hive -e' works
    for (int i = 0; i < sqls.size(); i++) {
      JDBCUtil.getInstance().executeSql(sqls.get(i));
    }
  }

  private List<String> parsingStatements() throws IOException {
    try {
      return FileUtils.readLines(FileUtils.getFile(createTableStatementPath), "utf-8");
    } catch (IOException e) {
      LOGGER.error("Exception occurs while parsing create table statements", e);
      throw e;
    }
  }
}
