package ind.xuchuanyin.tpch.datagen;

import java.util.Collection;

import io.airlift.tpch.TpchTable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.Transformer;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

public class TableGenModel implements Normalizer {
  private static final Logger LOGGER = Logger.getLogger(TableGenModel.class);
  private String tpchTableName;
  private int scaleupFactor;
  private int filePartCnt;

  public String getTpchTableName() {
    return tpchTableName;
  }

  public void setTpchTableName(String tpchTableName) {
    this.tpchTableName = tpchTableName;
  }

  public int getScaleupFactor() {
    return scaleupFactor;
  }

  public void setScaleupFactor(int scaleupFactor) {
    this.scaleupFactor = scaleupFactor;
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

    // todo: validate others
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("TableGenModel{");
    sb.append("tpchTableName='").append(tpchTableName).append('\'');
    sb.append(", scaleupFactor=").append(scaleupFactor);
    sb.append(", filePartCnt=").append(filePartCnt);
    sb.append('}');
    return sb.toString();
  }

  public static final class TableGenModelBuilder {
    private String tpchTableName;
    private int scaleupFactor;
    private int filePartCnt;

    private TableGenModelBuilder() {
    }

    public static TableGenModelBuilder aTableGenModel() {
      return new TableGenModelBuilder();
    }

    public TableGenModelBuilder withTpchTableName(String tpchTableName) {
      this.tpchTableName = tpchTableName;
      return this;
    }

    public TableGenModelBuilder withScaleupFactor(int scaleupFactor) {
      this.scaleupFactor = scaleupFactor;
      return this;
    }

    public TableGenModelBuilder withFilePartCnt(int filePartCnt) {
      this.filePartCnt = filePartCnt;
      return this;
    }

    public TableGenModel build() {
      TableGenModel tableGenModel = new TableGenModel();
      tableGenModel.setTpchTableName(tpchTableName);
      tableGenModel.setScaleupFactor(scaleupFactor);
      tableGenModel.setFilePartCnt(filePartCnt);
      return tableGenModel;
    }
  }
}
