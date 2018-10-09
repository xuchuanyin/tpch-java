package ind.xuchuanyin.tpch.jdbc;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;
import ind.xuchuanyin.tpch.report.HistogramReporter;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class QueryClientTest {
  private static final Logger LOGGER = Logger.getLogger(QueryClientTest.class);
  private String sqlMetaFile = "sql_meta_test.txt";
  private String rptStorePath = "report";
  private String mergedTargetrPath = "merged_stat";

  @Before
  public void setUp() throws Exception {
    for (String path : new String[] { sqlMetaFile, rptStorePath, mergedTargetrPath }) {
      FileUtils.deleteQuietly(FileUtils.getFile(path));
    }
    prepareQueryModel(sqlMetaFile);
  }

  @After
  public void tearDown() throws Exception {
    for (String path : new String[] { sqlMetaFile, rptStorePath, mergedTargetrPath }) {
      FileUtils.deleteQuietly(FileUtils.getFile(path));
    }
  }

  private void prepareQueryModel(String queryModelMetaPath) throws Exception {
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
        .withReportStore(rptStorePath)
        .withQuerySlices(querySliceList)
        .build();

    LOGGER.info("Prepare query model: " + queryModel);
    Gson gson = new Gson();
    String jsonStr = gson.toJson(queryModel);
    LOGGER.info("Write query model in json format: " + jsonStr);
    File file = FileUtils.getFile(queryModelMetaPath);
    LOGGER.info("Target file is " + file.getAbsolutePath());
    FileUtils.forceMkdirParent(file);
    if (file.exists()) {
      FileUtils.forceDelete(file);
    }
    FileUtils.touch(file);
    FileUtils.write(file, jsonStr, "utf-8");
  }

  @Test
  public void testQueryClient() throws Exception {
    QueryClient queryClient = new QueryClient(sqlMetaFile);
    queryClient.ignite();
    queryClient.close();
  }

  @Test
  public void testMergeStatistic() throws Exception {
    QueryClient queryClient = new QueryClient(sqlMetaFile);
    // query 2 times will result in 2 statistic file
    queryClient.ignite();
    queryClient.ignite();

    File[] jsonRptFiles = FileUtils.getFile(rptStorePath).listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return pathname.getName().endsWith(HistogramReporter.JSON_RPT_SUFFIX);
      }
    });
    assert jsonRptFiles != null;

    String[] jsonRptFilePaths = new String[jsonRptFiles.length];
    for (int i = 0; i < jsonRptFiles.length; i++) {
      jsonRptFilePaths[i] = jsonRptFiles[i].getAbsolutePath();
    }
    String mergedResult =
        HistogramReporter.mergeStatisticFromFile(mergedTargetrPath, jsonRptFilePaths);
    LOGGER.info(mergedResult);

    queryClient.close();
  }
}