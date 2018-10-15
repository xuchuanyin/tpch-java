package ind.xuchuanyin.tpch.datagen;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.gson.Gson;
import ind.xuchuanyin.tpch.Procedure;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

public class DataGenerator implements Procedure {
  private static final Logger LOGGER = Logger.getLogger(DataGenerator.class);
  private String inputFilePath;
  private DataGenModel dataGenModel;
  private ExecutorService executorService;
  private List<Future<?>> executorTasks;

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

    executorService = Executors.newCachedThreadPool();
    executorTasks = new ArrayList<>();

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
    for (int i = 0; i < executorTasks.size(); i++) {
      try {
        executorTasks.get(i).get();
      } catch (Exception e) {
        LOGGER.error("Failed to execute task", e);
      }
    }
    LOGGER.info("Succeed to generate data for " + dataGenModel.getTableGenModels().size() + " tables");
  }

  private void generateData4Table(TableGenModel tableGenModel) throws IOException {
    String tpchTableName = tableGenModel.getTpchTableName();
    int partCnt = tableGenModel.getFilePartCnt();
    double scaleupFactor = tableGenModel.getScaleupFactor();

    FileUtils.forceMkdir(
        FileUtils.getFile(dataGenModel.getTargetDirectory() + File.separator + tpchTableName));
    LOGGER.info("Start to generate data for table " + tpchTableName);
    for (int part = 1; part <= partCnt; part++) {
      executorTasks.add(executorService.submit(new GeneratorWorker(
          dataGenModel.getTargetDirectory(), tpchTableName, scaleupFactor, part, partCnt)));
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
    if (null != executorService) {
      executorService.shutdown();
    }
  }

  private final class GeneratorWorker implements Runnable {
    private String targetDirectory;
    private String tpchTableName;
    private double scaleupFactor;
    private int part;
    private int partCnt;

    public GeneratorWorker(String targetDirectory, String tpchTableName, double scaleupFactor,
        int part, int partCnt) {
      this.targetDirectory = targetDirectory;
      this.tpchTableName = tpchTableName;
      this.scaleupFactor = scaleupFactor;
      this.part = part;
      this.partCnt = partCnt;
    }

    @Override
    public void run() {
      try {
        AirliftTpchUtil.getInstance()
            .generateData4PerPart(targetDirectory, tpchTableName, scaleupFactor, part, partCnt);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
