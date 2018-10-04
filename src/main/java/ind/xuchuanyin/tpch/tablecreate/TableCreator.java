package ind.xuchuanyin.tpch.tablecreate;

import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

public class TableCreator {
  private static final Logger LOGGER = Logger.getLogger(TableCreator.class);
  private String createTableStatementPath;

  public TableCreator(String createTableStatementPath) {
    this.createTableStatementPath = createTableStatementPath;
  }

  public void ignite() throws IOException {
    List<String> sqls = loadStatements();
    // todo: execute these sqls sequentially

  }

  private List<String> loadStatements() throws IOException {
    try {
      return FileUtils.readLines(FileUtils.getFile(createTableStatementPath), "utf-8");
    } catch (IOException e) {
      LOGGER.error("Exception occurs while loading create table statements", e);
      throw e;
    }
  }
}
