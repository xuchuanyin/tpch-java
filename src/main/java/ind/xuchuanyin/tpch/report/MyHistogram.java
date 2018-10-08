package ind.xuchuanyin.tpch.report;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public final class MyHistogram<T> {
  private static final String VERTICAL = "|";
  private static final String PLUS = "+";
  private int size;
  private T total;
  private T min;
  private T max;
  private double avg;
  private T quarter;
  private T half;
  private T three_quarters;
  private T ninety;
  private T ninety_five;

  @Override public String toString() {
    final StringBuffer sb = new StringBuffer("MyHistogram{");
    sb.append("size=").append(size);
    sb.append(", total=").append(total);
    sb.append(", min=").append(min);
    sb.append(", max=").append(max);
    sb.append(", avg=").append(avg);
    sb.append(", 25%=").append(quarter);
    sb.append(", 50%=").append(half);
    sb.append(", 75%=").append(three_quarters);
    sb.append(", 90%=").append(ninety);
    sb.append(", 95%=").append(ninety_five);
    sb.append('}');
    return sb.toString();
  }

  public String toPrettyString(String header) {
    List<String> title = new ArrayList<>(10);
    title.add("size");
    title.add("total");
    title.add("min");
    title.add("max");
    title.add("avg");
    title.add("25%");
    title.add("50%");
    title.add("75%");
    title.add("90%");
    title.add("95%");

    List<String> value = new ArrayList<>(10);
    value.add(String.valueOf(this.size));
    value.add(String.valueOf(this.total));
    value.add(String.valueOf(this.min));
    value.add(String.valueOf(this.max));
    value.add(String.valueOf(this.avg));
    value.add(String.valueOf(this.quarter));
    value.add(String.valueOf(this.half));
    value.add(String.valueOf(this.three_quarters));
    value.add(String.valueOf(this.ninety));
    value.add(String.valueOf(this.ninety_five));

    int titleMaxWidth = 5;
    int valueMaxWidth = value.stream().map(v -> v.length()).max(new Comparator<Integer>() {
      @Override public int compare(Integer o1, Integer o2) {
        return o1.compareTo(o2);
      }
    }).get();

    int maxWidth = titleMaxWidth >= valueMaxWidth ? titleMaxWidth : valueMaxWidth;
    maxWidth = maxWidth + 2;

    List<String> sep = new ArrayList<>(value.size());
    for (int i = 0; i < value.size(); i++) {
      sep.add(genChars("-", maxWidth));
    }

    String titleLine = StringUtils.join(padStr(title, maxWidth), VERTICAL);
    String valueLine = StringUtils.join(padStr(value, maxWidth), VERTICAL);
    String sepLine = StringUtils.join(sep, PLUS);

    final StringBuffer sb = new StringBuffer();
    sb.append(header).append(System.lineSeparator()).append(VERTICAL).append(sepLine)
        .append(VERTICAL).append(System.lineSeparator()).append(VERTICAL).append(titleLine)
        .append(VERTICAL).append(System.lineSeparator()).append(VERTICAL).append(sepLine)
        .append(VERTICAL).append(System.lineSeparator()).append(VERTICAL).append(valueLine)
        .append(VERTICAL).append(System.lineSeparator()).append(VERTICAL).append(sepLine)
        .append(VERTICAL).append(System.lineSeparator());
    return sb.toString();
  }

  public String toCompactString(String header) {
    List<String> value = new ArrayList<>(10);
    value.add(String.valueOf(this.size));
    value.add(String.valueOf(this.total));
    value.add(String.valueOf(this.min));
    value.add(String.valueOf(this.max));
    value.add(String.valueOf(this.avg));
    value.add(String.valueOf(this.quarter));
    value.add(String.valueOf(this.half));
    value.add(String.valueOf(this.three_quarters));
    value.add(String.valueOf(this.ninety));
    value.add(String.valueOf(this.ninety_five));
    return header + ":" + StringUtils.join(value, ",");
  }

  private List<String> padStr(List<String> input, int width) {
    return input.stream().map(s -> {
      int leftPad = (width - s.length()) / 2;
      int rightPad = width - leftPad - s.length();

      return genChars(" ", leftPad) + s + genChars(" ", rightPad);
    }).collect(Collectors.toList());
  }

  private String genChars(String ch, int num) {
    if (num <= 0) {
      return "";
    }

    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < num; i++) {
      sb.append(ch);
    }
    return sb.toString();
  }

  public static final class MyHistogramBuilder<T> {
    private int size;
    private T total;
    private T min;
    private T max;
    private double avg;
    private T quarter;
    private T half;
    private T three_quarters;
    private T ninety;
    private T ninety_five;

    private MyHistogramBuilder() {
    }

    public static MyHistogramBuilder createBuilder() {
      return new MyHistogramBuilder();
    }

    public MyHistogramBuilder withSize(int size) {
      this.size = size;
      return this;
    }

    public MyHistogramBuilder withTotal(T total) {
      this.total = total;
      return this;
    }

    public MyHistogramBuilder withMin(T min) {
      this.min = min;
      return this;
    }

    public MyHistogramBuilder withMax(T max) {
      this.max = max;
      return this;
    }

    public MyHistogramBuilder withAvg(double avg) {
      this.avg = avg;
      return this;
    }

    public MyHistogramBuilder withQuarter(T quarter) {
      this.quarter = quarter;
      return this;
    }

    public MyHistogramBuilder withHalf(T half) {
      this.half = half;
      return this;
    }

    public MyHistogramBuilder withThree_quarters(T three_quarters) {
      this.three_quarters = three_quarters;
      return this;
    }

    public MyHistogramBuilder withNinety(T ninety) {
      this.ninety = ninety;
      return this;
    }

    public MyHistogramBuilder withNinety_five(T ninety_five) {
      this.ninety_five = ninety_five;
      return this;
    }

    public MyHistogram build() {
      MyHistogram myHistogram = new MyHistogram();
      myHistogram.ninety = this.ninety;
      myHistogram.half = this.half;
      myHistogram.ninety_five = this.ninety_five;
      myHistogram.max = this.max;
      myHistogram.total = this.total;
      myHistogram.min = this.min;
      myHistogram.quarter = this.quarter;
      myHistogram.three_quarters = this.three_quarters;
      myHistogram.avg = this.avg;
      myHistogram.size = this.size;
      return myHistogram;
    }
  }

  public static MyHistogram statisticList(List<Long> list) {
    list.sort(new Comparator<Long>() {
      @Override public int compare(Long o1, Long o2) {
        return o1.compareTo(o2);
      }
    });
    //size
    int size = list.size();
    //total
    long total = 0;
    for (Long l : list) {
      total += l;
    }
    //min
    long min = list.get(0);
    //max
    long max = list.get(size - 1);
    //avg
    double avg = total / size;
    //25%
    long quarter = list.get(size / 4);
    //50%
    long half = list.get(size / 2);
    //75%
    long three_quarters = list.get(size * 3 / 4);
    //90%
    long ninety = list.get(size * 9 / 10);
    //95%
    long ninety_five = list.get(size * 95 / 100);

    MyHistogram.MyHistogramBuilder myHistogramBuilder =
        MyHistogram.MyHistogramBuilder.createBuilder();

    return myHistogramBuilder.withSize(size).withTotal(total).withMin(min).withMax(max).withAvg(avg)
        .withQuarter(quarter).withHalf(half).withThree_quarters(three_quarters).withNinety(ninety)
        .withNinety_five(ninety_five).build();
  }
}
