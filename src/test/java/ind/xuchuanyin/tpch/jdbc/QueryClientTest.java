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

  @Before
  public void setUp() throws Exception {
    FileUtils.deleteQuietly(FileUtils.getFile(sqlMetaFile));
    prepareQueryModel();
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteQuietly(FileUtils.getFile(sqlMetaFile));
    File[] reportFiles = FileUtils.getFile(rptStorePath).listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return pathname.getName().endsWith(HistogramReporter.JSON_RPT_SUFFIX) ||
            pathname.getName().endsWith(HistogramReporter.TBL_RPT_SUFFIX);
      }
    });
    if (reportFiles != null) {
      for (File file : reportFiles) {
        FileUtils.deleteQuietly(file);
      }
    }
  }

  private void prepareQueryModel() throws Exception {
    QuerySlice querySlice1 =
        QuerySlice.QuerySliceBuilder.aQuerySlice().withId("id1").withSql("show tables")
            .withThreads(1).withIsConsumeResult(true).withIsCountInStatistics(true)
            .withType("type1").build();
    QuerySlice querySlice2 =
        QuerySlice.QuerySliceBuilder.aQuerySlice().withId("id2").withSql("show databases")
            .withThreads(5).withIsConsumeResult(true).withIsCountInStatistics(true)
            .withType("type2").build();
    List<QuerySlice> querySliceList = new ArrayList<>();
    querySliceList.add(querySlice1);
    querySliceList.add(querySlice2);
    QueryModel queryModel =
        QueryModel.QueryModelBuilder.aQueryModel().withJdbcDriver("org.h2.Driver")
            .withJdbcUrl("jdbc:h2:~/test").withJdbcUser("sa").withJdbcPwd("").withJdbcPoolSize(3)
            .withExecInterval(2).withExecInterval(2).withExecConcurrentSize(2)
            .withShuffleExecute(true).withReportStore(rptStorePath).withQuerySlices(querySliceList)
            .build();

    LOGGER.info("Prepare query model: " + queryModel);
    Gson gson = new Gson();
    String jsonStr = gson.toJson(queryModel);
    LOGGER.info("Write query model in json format: " + jsonStr);
    File file = FileUtils.getFile(sqlMetaFile);
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
}