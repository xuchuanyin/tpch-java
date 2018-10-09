package ind.xuchuanyin.tpch.report;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import ind.xuchuanyin.tpch.jdbc.QueryResult;
import org.apache.log4j.Logger;

public class HistogramReporter {
  private static final Logger LOGGER = Logger.getLogger(HistogramReporter.class);

  public static String statistic(List<QueryResult> results, boolean isPrettyOut) {
    Map<String, MyHistogram> histogramMap = new HashMap<>();

    Set<String> queryTypes = results.stream()
        .map(r -> r.getQuerySlice().getType())
        .collect(Collectors.toSet());
    queryTypes.add("ALL");
    // for each type of query, get the query statistic
    for (String type : queryTypes) {
      List<Long> durations = results.stream()
          .filter(r -> type.equals("ALL") || r.getQuerySlice().getType().equals(type))
          .map(QueryResult::getDuration)
          .collect(Collectors.toList());
      MyHistogram histogram = MyHistogram.statisticList(type, durations);
      histogramMap.put(type, histogram);
    }

    List<Map.Entry<String, MyHistogram>> listed = new ArrayList<>(histogramMap.entrySet());
    listed.sort(new Comparator<Map.Entry<String, MyHistogram>>() {
      @Override
      public int compare(Map.Entry<String, MyHistogram> o1, Map.Entry<String, MyHistogram> o2) {
        return o1.getKey().compareToIgnoreCase(o2.getKey());
      }
    });

    TableFormatter tableFormatter = new TableFormatter(true);
    tableFormatter.setTitle(MyHistogram.getTitle());

    for (Map.Entry<String, MyHistogram> entry : listed) {
      tableFormatter.addRow(entry.getValue().getRawValue());
    }

    return tableFormatter.toPrettyString();
  }
}
