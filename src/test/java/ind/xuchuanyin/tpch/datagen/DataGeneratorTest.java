package ind.xuchuanyin.tpch.datagen;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DataGeneratorTest {
  private static final Logger LOGGER = Logger.getLogger(DataGeneratorTest.class);
  private String dataGeneratorMetaPath = "data_gen_meta_path";
  private String dataGeneratorTargetPath = "data_gen_target_path";

  @Before
  public void setUp() throws Exception {
    prepareGeneratorModel();
    FileUtils.deleteQuietly(FileUtils.getFile(dataGeneratorTargetPath));
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteQuietly(FileUtils.getFile(dataGeneratorMetaPath));
    FileUtils.deleteQuietly(FileUtils.getFile(dataGeneratorTargetPath));
  }

  private void prepareGeneratorModel() throws IOException {
    TableGenModel tableGenModel1 = TableGenModel.TableGenModelBuilder
        .aTableGenModel()
        .withTpchTableName("customer")
        .withScaleupFactor(1)
        .withFilePartCnt(2)
        .build();
    TableGenModel tableGenModel2 = TableGenModel.TableGenModelBuilder
        .aTableGenModel()
        .withTpchTableName("lineitem")
        .withScaleupFactor(1)
        .withFilePartCnt(3)
        .build();
    List<TableGenModel> tableGenModelList = new ArrayList<>();
    tableGenModelList.add(tableGenModel1);
    tableGenModelList.add(tableGenModel2);

    DataGenModel dataGenModel = DataGenModel.DataGenModelBuilder
        .aDataGenModel()
        .withTargetDirectory(dataGeneratorTargetPath)
        .withTableGenModels(tableGenModelList)
        .build();
    LOGGER.info("Prepare data generator model: " + dataGenModel);
    Gson gson = new Gson();
    String jsonStr = gson.toJson(dataGenModel);
    LOGGER.info("Write data generator model in json format: " + jsonStr);
    File file = FileUtils.getFile(dataGeneratorMetaPath + File.separator + DataGenModel.JSON_FILE);
    FileUtils.forceMkdirParent(file);
    if (file.exists()) {
      FileUtils.forceDelete(file);
    }
    FileUtils.touch(file);
    FileUtils.write(file, jsonStr, "utf-8");
  }

  @Test
  public void testDataGenerator() throws Exception {
    DataGenerator dataGenerator = new DataGenerator(dataGeneratorMetaPath);
    dataGenerator.ignite();
  }
}