package ind.xuchuanyin.tpch.report;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public final class MyHistogram<T> {
  private static final List<String> HISTOGRAM_TITLE = new ArrayList<String>() {{
    add("query");
    add("size");
    add("total");
    add("min");
    add("max");
    add("avg");
    add("25%");
    add("50%");
    add("75%");
    add("90%");
    add("95%");
  }};
  private String query;
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
    sb.append("query='").append(query).append("'");
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

  public static List<String> getTitle() {
    return HISTOGRAM_TITLE;
  }

  public List<String> getRawValue() {
    List<String> value = new ArrayList<>(10);
    value.add(String.valueOf(this.query));
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

    return value;
  }
  public static final class MyHistogramBuilder<T> {
    private String query;
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

    public MyHistogramBuilder withQuery(String query) {
      this.query = query;
      return this;
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
      myHistogram.query = this.query;
      return myHistogram;
    }
  }

  public static MyHistogram statisticList(String query, List<Long> list) {
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

    return myHistogramBuilder
        .withQuery(query)
        .withSize(size)
        .withTotal(total)
        .withMin(min)
        .withMax(max)
        .withAvg(avg)
        .withQuarter(quarter)
        .withHalf(half)
        .withThree_quarters(three_quarters)
        .withNinety(ninety)
        .withNinety_five(ninety_five)
        .build();
  }
}
