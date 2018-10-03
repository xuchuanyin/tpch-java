package ind.xuchuanyin.tpch.datagen;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

import io.airlift.tpch.CustomerGenerator;
import io.airlift.tpch.OrderGenerator;
import io.airlift.tpch.PartGenerator;
import io.airlift.tpch.SupplierGenerator;
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

  public int getScaleupFactor(String tpchTableName) {
    if (tpchTableName.equals(TpchTable.CUSTOMER.getTableName())) {
      return CustomerGenerator.SCALE_BASE;
    } else if (tpchTableName.equals(TpchTable.LINE_ITEM.getTableName())) {
      return OrderGenerator.SCALE_BASE;
    } else if (tpchTableName.equals(TpchTable.NATION.getTableName())) {
      return 1;
    } else if (tpchTableName.equals(TpchTable.ORDERS.getTableName())) {
      return OrderGenerator.SCALE_BASE;
    } else if (tpchTableName.equals(TpchTable.PART.getTableName())) {
      return PartGenerator.SCALE_BASE;
    } else if (tpchTableName.equals(TpchTable.PART_SUPPLIER.getTableName())) {
      return PartGenerator.SCALE_BASE;
    } else if (tpchTableName.equals(TpchTable.REGION.getTableName())) {
      return 1;
    } else if (tpchTableName.equals(TpchTable.SUPPLIER.getTableName())) {
      return SupplierGenerator.SCALE_BASE;
    } else {
      throw new IllegalArgumentException("Unsupported tpch generator for table " + tpchTableName);
    }
  }

  public void generateData4PerPart(String baseDirectory, String tpchTableName, double scalupFactor,
      int part, int partCnt) throws IOException {
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
  }

  public Iterable<? extends TpchEntity> getTpchGenerator(String tpchTableName,
      double scalupFactor, int part, int partCnt) {
    if (tpchTableName.equals(TpchTable.CUSTOMER.getTableName())) {
      return TpchTable.CUSTOMER.createGenerator(scalupFactor, part, partCnt);
    } else if (tpchTableName.equals(TpchTable.LINE_ITEM.getTableName())) {
      return TpchTable.LINE_ITEM.createGenerator(scalupFactor, part, partCnt);
    } else if (tpchTableName.equals(TpchTable.NATION.getTableName())) {
      return TpchTable.NATION.createGenerator(scalupFactor, part, partCnt);
    } else if (tpchTableName.equals(TpchTable.ORDERS.getTableName())) {
      return TpchTable.ORDERS.createGenerator(scalupFactor, part, partCnt);
    } else if (tpchTableName.equals(TpchTable.PART.getTableName())) {
      return TpchTable.PART.createGenerator(scalupFactor, part, partCnt);
    } else if (tpchTableName.equals(TpchTable.PART_SUPPLIER.getTableName())) {
      return TpchTable.PART_SUPPLIER.createGenerator(scalupFactor, part, partCnt);
    } else if (tpchTableName.equals(TpchTable.REGION.getTableName())) {
      return TpchTable.REGION.createGenerator(scalupFactor, part, partCnt);
    } else if (tpchTableName.equals(TpchTable.SUPPLIER.getTableName())) {
      return TpchTable.SUPPLIER.createGenerator(scalupFactor, part, partCnt);
    } else {
      throw new IllegalArgumentException("Unsupported tpch generator for table " + tpchTableName);
    }
  }
}
