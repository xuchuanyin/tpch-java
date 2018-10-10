package ind.xuchuanyin.tpch.datagen;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

import io.airlift.tpch.TpchEntity;
import io.airlift.tpch.TpchTable;
import org.apache.log4j.Logger;

public class AirliftTpchUtil {
  private static final Logger LOGGER = Logger.getLogger(AirliftTpchUtil.class);
  private static final AirliftTpchUtil INSTANCE = new AirliftTpchUtil();

  private AirliftTpchUtil() {
  }

  public static AirliftTpchUtil getInstance() {
    return INSTANCE;
  }

  public void generateData4PerPart(String baseDirectory, String tpchTableName, double scalupFactor,
      int part, int partCnt) throws IOException {
    LOGGER.info(
        String.format("Start to generate data for table#%s-part#%d/%d in base directory %s",
            tpchTableName, part, partCnt, baseDirectory));
    Writer writer = null;
    try {
      String destPath = String.format("%s%s%s%s%s-part-%d.dat",
          baseDirectory, File.separator, tpchTableName, File.separator, tpchTableName, part);
      writer = new FileWriter(destPath);

      Iterable<? extends TpchEntity> iterable =
          TpchTable.getTable(tpchTableName).createGenerator(scalupFactor, part, partCnt);
      for (TpchEntity entity : iterable) {
        writer.write(entity.toLine());
        writer.write('\n');
      }
    } catch (IOException e) {
      LOGGER.error("Failed to generate data for table " + tpchTableName);
      throw e;
    } finally {
      if (writer != null) {
        try {
          writer.close();
        } catch (IOException e) {
          LOGGER.error("Failed to close writer for table " + tpchTableName);
        }
      }
    }
    LOGGER.info(
        String.format("Succeed to generate data for table#%s-part#%d/%d in base directory %s",
            tpchTableName, part, partCnt, baseDirectory));
  }
}
