package ind.xuchuanyin.tpch.datagen;

import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

public class DataGenModel implements Normalizer {
  private static final Logger LOGGER = Logger.getLogger(DataGenModel.class);
  public static final String JSON_FILE = "data_generate_model.json";

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

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("DataGenModel{");
    sb.append("targetDirectory='").append(targetDirectory).append('\'');
    sb.append(", tableGenModels=").append(tableGenModels);
    sb.append('}');
    return sb.toString();
  }

  public static final class DataGenModelBuilder {
    private String targetDirectory;
    private List<TableGenModel> tableGenModels;

    private DataGenModelBuilder() {
    }

    public static DataGenModelBuilder aDataGenModel() {
      return new DataGenModelBuilder();
    }

    public DataGenModelBuilder withTargetDirectory(String targetDirectory) {
      this.targetDirectory = targetDirectory;
      return this;
    }

    public DataGenModelBuilder withTableGenModels(List<TableGenModel> tableGenModels) {
      this.tableGenModels = tableGenModels;
      return this;
    }

    public DataGenModel build() {
      DataGenModel dataGenModel = new DataGenModel();
      dataGenModel.setTargetDirectory(targetDirectory);
      dataGenModel.setTableGenModels(tableGenModels);
      return dataGenModel;
    }
  }
}
