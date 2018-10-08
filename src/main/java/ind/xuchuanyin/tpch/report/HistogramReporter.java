package ind.xuchuanyin.tpch.report;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import ind.xuchuanyin.tpch.jdbc.QueryProcessor;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

public class HistogramReporter {
  private static final Logger LOGGER = Logger.getLogger(HistogramReporter.class);

  public static String statistic(List<QueryProcessor.QueryResult> results, boolean isPrettyOut) {
    Map<String, MyHistogram> histogramMap = new HashMap<>();

    Set<String> queryTypes =
        results.stream().map(r -> r.getExtraInfo()).collect(Collectors.toSet());
    queryTypes.add("ALL");
    // for each type of query, get the query statistic
    for (String type : queryTypes) {
      List<Long> durations =
          results.stream().filter(r -> type.equals("ALL") || r.getExtraInfo().equals(type))
              .map(r -> r.getDuration()).collect(Collectors.toList());
      MyHistogram histogram = MyHistogram.statisticList(durations);
      histogramMap.put(type, histogram);
    }

    List<Map.Entry<String, MyHistogram>> listed =
        histogramMap.entrySet().stream().collect(Collectors.toList());

    listed.sort(new Comparator<Map.Entry<String, MyHistogram>>() {
      @Override
      public int compare(Map.Entry<String, MyHistogram> o1, Map.Entry<String, MyHistogram> o2) {
        return o1.getKey().compareToIgnoreCase(o2.getKey());
      }
    });

    List<String> output = listed.stream().map(map -> isPrettyOut ?
        map.getValue().toPrettyString(map.getKey()) :
        map.getValue().toCompactString(map.getKey())).collect(Collectors.toList());

    return StringUtils.join(output, System.lineSeparator() + System.lineSeparator());
  }
}
