package ind.xuchuanyin.tpch.jdbc;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.google.gson.Gson;
import ind.xuchuanyin.tpch.Procedure;
import ind.xuchuanyin.tpch.common.Utils;
import ind.xuchuanyin.tpch.report.HistogramReporter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

public class QueryClient implements Procedure {
  private static final Logger LOGGER = Logger.getLogger(QueryClient.class);
  private String inputFile;
  private QueryModel queryModel;
  private QueryProcessor queryProcessor;

  public QueryClient() {
  }

  @Override
  public void setInputFiles(String... inputFiles) {
    this.inputFile = inputFiles[0];
  }

  private void loadQueryModel(File file) throws IOException {
    Gson gson = new Gson();
    Reader reader = null;
    try {
      reader = new FileReader(file);
      queryModel = gson.fromJson(reader, QueryModel.class);
      LOGGER.info("Load sql execution model: " + file.getAbsolutePath());
    } catch (IOException e) {
      LOGGER.error("Failed to load model for sql execution from path " + file.getAbsolutePath(), e);
      throw e;
    } finally {
      if (null != reader) {
        reader.close();
      }
    }
  }

  private void validate() throws IllegalArgumentException {
    if (StringUtils.isBlank(queryModel.getJdbcUrl())) {
      throw new IllegalArgumentException("'jdbcUrl' is required in QueryModel");
    }
    if (StringUtils.isBlank(queryModel.getJdbcDriver())) {
      throw new IllegalArgumentException("'jdbcDriver' is required in QueryModel");
    }
    if (CollectionUtils.isEmpty(queryModel.getQuerySlices())) {
      throw new IllegalArgumentException("'querySlices' is required in QueryModel");
    }
  }

  @Override
  public void ignite() throws Exception {
    File file = FileUtils.getFile(inputFile);
    loadQueryModel(file);
    validate();

    ConnectionMgr.getInstance()
        .init(queryModel.getJdbcDriver(), queryModel.getJdbcUrl(), queryModel.getJdbcUser(),
            queryModel.getJdbcPwd(), queryModel.getJdbcPoolSize());
    queryProcessor = new QueryProcessor(queryModel.getExecConcurrentSize());

    int iteration = queryModel.getExecIteration();
    List<String> allReports = new ArrayList<>(iteration);
    List<QueryResult> allResults4Stat = new ArrayList<>();
    List<Long> end2EndDuration = new ArrayList<>(iteration);
    for (int i = 0; i < iteration; i++) {
      long start = System.currentTimeMillis();

      List<QueryResult> cycleResult = query(queryModel.getQuerySlices());

      end2EndDuration.add(System.currentTimeMillis() - start);
      List<QueryResult> results4Stat = cycleResult.stream()
          .filter(r -> r.getQuerySlice().isCountInStatistics())
          .filter(r -> r.getDuration() >= 0)
          .collect(Collectors.toList());
      if (cycleResult.size() > results4Stat.size()) {
        LOGGER.info(String.format(
            "Skip %d results for statistic in iteration %d due to execlusion or failure",
            cycleResult.size() - results4Stat.size(), i + 1));
      }

      // do statistics for each iteration
      if (results4Stat.size() != 0) {
        String eachReport = HistogramReporter.statistic(results4Stat, queryModel.getReportStore());
        LOGGER.info(
            String.format("Query statistics(ms) for iteration %d: %s", i + 1, eachReport));
        allResults4Stat.addAll(results4Stat);
        allReports.add(eachReport);
      } else {
        LOGGER.warn(String.format("Skip statistic in iteration %d due to empty results", i + 1));
      }

      if (i < iteration - 1 && queryModel.getExecInterval() > 0) {
        LOGGER.info(String.format("Waiting %ss for the next iteration of query",
            queryModel.getExecInterval()));
        Thread.sleep(1000 * queryModel.getExecInterval());
      }
    }

    // this is for final output, we will print statistics for each iteration
    for (int i = 0; i < allReports.size(); i++) {
      LOGGER.info(String.format(
          "Query statistics(ms) for iteration %d: %s", i + 1, allReports.get(i)));
    }
    // statistic over all iterations
    if (allResults4Stat.size() != 0 && queryModel.getExecIteration() > 1) {
      String mergedResports = HistogramReporter.statistic(
          allResults4Stat, queryModel.getReportStore());
      LOGGER.info(String.format("Query statics(ms) for all iterations: %s", mergedResports));
    } else {
      LOGGER.warn("Skip statistic for all iterations due to empty query results");
    }

    LOGGER.info("End2End time(ms): " + StringUtils.join(end2EndDuration, ","));
  }

  private List<QueryResult> query(List<QuerySlice> querySlices)
      throws InterruptedException {
    if (CollectionUtils.isEmpty(querySlices)) {
      return new ArrayList<>();
    }

    List<QuerySlice> threadQuerySlices = new ArrayList<>(querySlices.size());
    for (QuerySlice querySlice : querySlices) {
      for (int i = 0; i < querySlice.getThreads(); i++) {
        threadQuerySlices.add(querySlice);
      }
    }

    List<String> originSeq = threadQuerySlices.stream()
        .map(QuerySlice::getId)
        .collect(Collectors.toList());
    LOGGER.info("Original query sequence: " + StringUtils.join(originSeq, ","));
    if (queryModel.isShuffleExecute()) {
      // shuffle the queries
      QuerySlice[] simpleQuerySliceArray = threadQuerySlices.toArray(new QuerySlice[0]);
      Utils.shuffleArray(simpleQuerySliceArray);
      List<String> shuffledSeq = Arrays.stream(simpleQuerySliceArray)
          .map(QuerySlice::getId)
          .collect(Collectors.toList());
      LOGGER.info("Shuffled query sequence: " + StringUtils.join(shuffledSeq, ","));
      querySlices = Arrays.asList(simpleQuerySliceArray);
    }

    List<QueryResult> queryResults = queryProcessor.processBatch(querySlices);

    List<Long> timeSeq = queryResults.stream()
        .map(QueryResult::getDuration)
        .collect(Collectors.toList());
    LOGGER.info("Time taken sequence(ms): " + StringUtils.join(timeSeq, ","));
    return queryResults;
  }

  @Override
  public void close() {
    ConnectionMgr.getInstance().close();
    queryProcessor.close();
  }
}
