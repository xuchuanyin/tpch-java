package ind.xuchuanyin.tpch.datagen;

import java.util.Collection;
import java.util.List;

import io.airlift.tpch.TpchTable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.Transformer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

public class DataGenModel implements Normalizer {
  private static final Logger LOGGER = Logger.getLogger(DataGenModel.class);

  public static class TableGenModel implements Normalizer {
    private String tpchTableName;
    private long totalRowCnt;
    private int filePartCnt;

    public String getTpchTableName() {
      return tpchTableName;
    }

    public void setTpchTableName(String tpchTableName) {
      this.tpchTableName = tpchTableName;
    }

    public long getTotalRowCnt() {
      return totalRowCnt;
    }

    public void setTotalRowCnt(long totalRowCnt) {
      this.totalRowCnt = totalRowCnt;
    }

    public int getFilePartCnt() {
      return filePartCnt;
    }

    public void setFilePartCnt(int filePartCnt) {
      this.filePartCnt = filePartCnt;
    }

    @Override
    public void normalize() throws IllegalArgumentException {
      // validate tableName
      Collection<String> nativeTpchTables =
          CollectionUtils.collect(TpchTable.getTables(), new Transformer<TpchTable<?>, String>() {
            @Override
            public String transform(TpchTable<?> tpchTable) {
              return tpchTable.getTableName();
            }
          });
      tpchTableName = tpchTableName.toLowerCase();
      if (!nativeTpchTables.contains(tpchTableName)) {
        LOGGER.error(
            String.format("Table %s is not a TPCH native table, Supported TPCH tables are {%s}",
                tpchTableName, StringUtils.join(nativeTpchTables, ", ")));
        throw new IllegalArgumentException("Unsupported table " + tpchTableName + " is found.");
      }

      // todo: validate othes
    }
  }

  private String targetDirectory;
  private List<TableGenModel> tableGenModels;

  public String getTargetDirectory() {
    return targetDirectory;
  }

  public void setTargetDirectory(String targetDirectory) {
    this.targetDirectory = targetDirectory;
  }

  public List<TableGenModel> getTableGenModels() {
    return tableGenModels;
  }

  public void setTableGenModels(List<TableGenModel> tableGenModels) {
    this.tableGenModels = tableGenModels;
  }

  @Override
  public void normalize() throws IllegalArgumentException {
    if (!FileUtils.getFile(targetDirectory).mkdirs()) {
      throw new IllegalArgumentException(
          "Failed to create target directory for data generate: " + targetDirectory);
    }

    for (TableGenModel tableGenModel : tableGenModels) {
      tableGenModel.normalize();
    }
  }
}
