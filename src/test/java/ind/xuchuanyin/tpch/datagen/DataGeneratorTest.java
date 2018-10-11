package ind.xuchuanyin.tpch.datagen;

import java.io.IOException;

import com.google.gson.Gson;
import ind.xuchuanyin.tpch.common.TestUtil;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DataGeneratorTest {
  private static final Logger LOGGER = Logger.getLogger(DataGeneratorTest.class);
  private String dataGeneratorMetaPath = "data_gen_meta.json";
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
    DataGenModel dataGenModel = TestUtil.prepareGeneratorModel(dataGeneratorTargetPath);
    LOGGER.info("Prepare data generator model: " + dataGenModel);
    Gson gson = new Gson();
    String jsonStr = gson.toJson(dataGenModel);
    LOGGER.info("Write data generator model in json format: " + jsonStr);
    TestUtil.writeToFile(jsonStr, dataGeneratorMetaPath);
  }

  @Test
  public void testDataGenerator() throws Exception {
    DataGenerator dataGenerator = new DataGenerator();
    dataGenerator.setInputFiles(dataGeneratorMetaPath);
    dataGenerator.ignite();
    dataGenerator.close();
  }
}
