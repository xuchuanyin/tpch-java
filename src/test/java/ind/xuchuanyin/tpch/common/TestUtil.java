package ind.xuchuanyin.tpch.common;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import ind.xuchuanyin.tpch.datagen.DataGenModel;
import ind.xuchuanyin.tpch.datagen.TableGenModel;
import ind.xuchuanyin.tpch.jdbc.QueryModel;
import ind.xuchuanyin.tpch.jdbc.QuerySlice;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

public class TestUtil {
  private static final Logger LOGGER = Logger.getLogger(TestUtil.class);

  public static DataGenModel prepareGeneratorModel(String dataGenTargetPath) throws IOException {
    TableGenModel tableGenModel1 = TableGenModel.TableGenModelBuilder
        .aTableGenModel()
        .withTpchTableName("customer")
        .withScaleupFactor(0.1)
        .withFilePartCnt(2)
        .build();
    TableGenModel tableGenModel2 = TableGenModel.TableGenModelBuilder
        .aTableGenModel()
        .withTpchTableName("lineitem")
        .withScaleupFactor(0.1)
        .withFilePartCnt(3)
        .build();
    List<TableGenModel> tableGenModelList = new ArrayList<>();
    tableGenModelList.add(tableGenModel1);
    tableGenModelList.add(tableGenModel2);

    DataGenModel dataGenModel = DataGenModel.DataGenModelBuilder
        .aDataGenModel()
        .withTargetDirectory(dataGenTargetPath)
        .withTableGenModels(tableGenModelList)
        .build();
    return dataGenModel;
  }

  public static QueryModel prepareQueryModel(String reportStore) throws Exception {
    QuerySlice querySlice1 = QuerySlice.QuerySliceBuilder.aQuerySlice()
        .withId("id1")
        .withSql("show tables")
        .withThreads(1)
        .withIsConsumeResult(true)
        .withIsCountInStatistics(true)
        .withType("type1")
        .build();
    QuerySlice querySlice2 = QuerySlice.QuerySliceBuilder.aQuerySlice()
        .withId("id2")
        .withSql("show databases")
        .withThreads(5)
        .withIsConsumeResult(true)
        .withIsCountInStatistics(true)
        .withType("type2")
        .build();
    QuerySlice querySlice3 = QuerySlice.QuerySliceBuilder.aQuerySlice()
        .withId("id3")
        .withSql("show tables;show databases")
        .withThreads(2)
        .withIsConsumeResult(true)
        .withIsCountInStatistics(false)
        .withType("type3")
        .build();
    List<QuerySlice> querySliceList = new ArrayList<>();
    querySliceList.add(querySlice1);
    querySliceList.add(querySlice2);
    querySliceList.add(querySlice3);
    QueryModel queryModel = QueryModel.QueryModelBuilder.aQueryModel()
        .withJdbcDriver("org.h2.Driver")
        .withJdbcUrl("jdbc:h2:~/test")
        .withJdbcUser("sa")
        .withJdbcPwd("")
        .withJdbcPoolSize(3)
        .withExecInterval(2)
        .withExecInterval(2)
        .withExecConcurrentSize(2)
        .withShuffleExecute(true)
        .withReportStore(reportStore)
        .withQuerySlices(querySliceList)
        .build();

    return queryModel;
  }

  public static void writeToFile(String content, String filePath) throws IOException {
    File file = FileUtils.getFile(filePath);
    FileUtils.forceMkdirParent(file);
    if (file.exists()) {
      FileUtils.forceDelete(file);
    }
    FileUtils.touch(file);
    FileUtils.write(file, content, "utf-8");
  }
}
