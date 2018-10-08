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
import ind.xuchuanyin.tpch.common.Utils;
import ind.xuchuanyin.tpch.report.HistogramReporter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

public class QueryClient {
  private static final Logger LOGGER = Logger.getLogger(QueryClient.class);
  private static QueryModel queryModel;

  public QueryClient(String queryModelMetaPath) throws Exception {
    File file = FileUtils.getFile(queryModelMetaPath);
    loadQueryModel(file);

    ConnectionMgr.getInstance()
        .init(queryModel.getJdbcDriver(), queryModel.getJdbcUrl(), queryModel.getJdbcUser(),
            queryModel.getJdbcPwd(), queryModel.getJdbcPoolSize());
  }

  private void loadQueryModel(File file) throws IOException {
    Gson gson = new Gson();
    Reader reader = null;
    try {
      reader = new FileReader(file);
      queryModel = gson.fromJson(reader, QueryModel.class);
      LOGGER.info("Load data generator model: " + file.getAbsolutePath());
    } catch (IOException e) {
      LOGGER.error("Failed to load model for data query from path " + file.getAbsolutePath(), e);
      throw e;
    } finally {
      if (null != reader) {
        reader.close();
      }
    }
  }

  public void ignite() throws Exception {
    int iteration = queryModel.getExecIteration();
    List<String> outputList = new ArrayList<>(iteration);
    List<QueryProcessor.QueryResult> allResults = new ArrayList<>();
    List<Long> end2EndDuration = new ArrayList<>(iteration);
    while (iteration-- > 0) {
      long start = System.currentTimeMillis();

      List<QueryProcessor.QueryResult> cycleResult = query(queryModel.getQuerySlices());

      end2EndDuration.add(System.currentTimeMillis() - start);

      // do statistics for each iteration
      outputList.add(HistogramReporter.statistic(cycleResult, queryModel.isPrettyRpt()));
      allResults.addAll(cycleResult);

      if (iteration > 0 && queryModel.getExecInterval() > 0) {
        LOGGER
            .info("Waiting for the next iteration of query: " + queryModel.getExecInterval() + "s");
        Thread.sleep(1000 * queryModel.getExecInterval());
      }
    }

    if (queryModel.getExecIteration() > 1) {
      // statistic over all iterations
      outputList.add(HistogramReporter.statistic(allResults, queryModel.isPrettyRpt()));
    }

    LOGGER.info("Query statistics(ms): " + System.lineSeparator() + StringUtils
        .join(outputList, System.lineSeparator() + "*****" + System.lineSeparator()));

    LOGGER.info("End2End time(ms): " + StringUtils.join(end2EndDuration, ","));
  }

  private List<QueryProcessor.QueryResult> query(List<QuerySlice> querySlices)
      throws InterruptedException {
    List<String> extraInfos = new ArrayList<>(querySlices.size());

    List<QuerySlice> atomicQuerySlices = new ArrayList<>(querySlices.size() * 2);
    for (QuerySlice querySlice : querySlices) {
      for (int i = 0; i < querySlice.getThreads(); i++) {
        atomicQuerySlices.add(querySlice);
      }
    }

    if (queryModel.isShuffleExecute()) {
      List<String> originSeq = atomicQuerySlices.stream().map(m -> m.getType() + '-' + m.getId())
          .collect(Collectors.toList());
      // shuffle the queries
      QuerySlice[] simpleQuerySliceArray = atomicQuerySlices.toArray(new QuerySlice[0]);
      Utils.shuffleArray(simpleQuerySliceArray);
      List<String> shuffledSeq =
          Arrays.stream(simpleQuerySliceArray).map(m -> m.getType() + '-' + m.getId())
              .collect(Collectors.toList());
      LOGGER.info("OriginQuerySeq: " + StringUtils.join(originSeq, ","));
      LOGGER.info("ShuffledQuerySeq: " + StringUtils.join(shuffledSeq, ","));
      querySlices = Arrays.asList(simpleQuerySliceArray);
    }

    QueryProcessor queryProcessor = new QueryProcessor(queryModel.getExecConcurrentSize());

    List<QueryProcessor.QueryResult> queryResults =
        queryProcessor.processBatch(querySlices, extraInfos, queryModel.getExecInterval());

    List<Long> timeSeq =
        queryResults.stream().map(r -> r.getDuration()).collect(Collectors.toList());
    LOGGER.info("TimeTakenSeq: " + StringUtils.join(timeSeq, ","));

    return queryResults;
  }

  public void close() {
    ConnectionMgr.getInstance().destroy();
  }
}
