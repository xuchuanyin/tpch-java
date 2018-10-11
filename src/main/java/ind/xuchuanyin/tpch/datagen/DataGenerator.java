package ind.xuchuanyin.tpch.datagen;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

import com.google.gson.Gson;
import ind.xuchuanyin.tpch.Procedure;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

public class DataGenerator implements Procedure {
  private static final Logger LOGGER = Logger.getLogger(DataGenerator.class);
  private String inputFilePath;
  private DataGenModel dataGenModel;

  public DataGenerator() {
  }

  @Override
  public void setInputFiles(String... inputFiles) {
    this.inputFilePath = inputFiles[0];
  }

  @Override
  public void ignite() throws IOException {
    loadDataGenModel();

    dataGenModel.normalize();

    generate();
  }

  private void loadDataGenModel() throws IOException {
    Gson gson = new Gson();
    Reader reader = null;
    File file = FileUtils.getFile(inputFilePath);
    try {
      reader = new FileReader(file);
      dataGenModel = gson.fromJson(reader, DataGenModel.class);
      LOGGER.info("Loaded data generator model: " + dataGenModel);
    } catch (IOException e) {
      LOGGER.error(String.format(
          "Failed to load model for data generator from path %s, absolute path is %s",
          inputFilePath, file.getAbsolutePath()), e);
      throw e;
    } finally {
      if (null != reader) {
        reader.close();
      }
    }
  }

  private void generate() {
    LOGGER.info("Start to generate data for " + dataGenModel.getTableGenModels().size() + " tables");
    for (TableGenModel tableGenModel : dataGenModel.getTableGenModels()) {
      try {
        generateData4Table(tableGenModel);
      } catch (IOException e) {
        LOGGER.error("Failed to generate data for table: " + tableGenModel.getTpchTableName(), e);
        try {
          clearData(tableGenModel.getTpchTableName());
        } catch (IOException ex) {
          LOGGER.error("Failed to clear up tpch table: " + tableGenModel.getTpchTableName(), ex);
        }
      }
    }
    LOGGER.info("Succeed to generate data for " + dataGenModel.getTableGenModels().size() + " tables");
  }

  private void generateData4Table(TableGenModel tableGenModel) throws IOException {
    String tpchTableName = tableGenModel.getTpchTableName();
    int partCnt = tableGenModel.getFilePartCnt();
    int scaleupFactor = tableGenModel.getScaleupFactor();

    FileUtils.forceMkdir(
        FileUtils.getFile(dataGenModel.getTargetDirectory() + File.separator + tpchTableName));
    LOGGER.info("Start to generate data for table " + tpchTableName);
    for (int part = 1; part <= partCnt; part++) {
      AirliftTpchUtil.getInstance().generateData4PerPart(
          dataGenModel.getTargetDirectory(), tpchTableName, scaleupFactor, part, partCnt);
    }
    LOGGER.info("Succeed to generate data for table " + tpchTableName);
  }

  private void clearData(String tpchTableName) throws IOException {
    FileUtils.deleteDirectory(
        FileUtils.getFile(dataGenModel.getTargetDirectory() + File.separator + tpchTableName));
  }

  private String summary() {
    return "Not implemented yet";
  }

  @Override
  public void close() {

  }
}
