package ind.xuchuanyin.tpch.jdbc;

public class QuerySlice {
  private String id = "q" + System.nanoTime();
  private String sql;
  private String type = "unclassified";
  private int threads = 1;
  private boolean isSelectQuery = true;
  private boolean isConsumeResult = true;
  private boolean isCountInStatistics = true;

  public QuerySlice() {
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("QuerySlice{");
    sb.append("id='").append(id).append('\'');
    sb.append(", sql='").append(sql).append('\'');
    sb.append(", type='").append(type).append('\'');
    sb.append(", threads=").append(threads);
    sb.append(", isSelectQuery=").append(isSelectQuery);
    sb.append(", isConsumeResult=").append(isConsumeResult);
    sb.append(", isCountInStatistics=").append(isCountInStatistics);
    sb.append('}');
    return sb.toString();
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getSql() {
    return sql;
  }

  public void setSql(String sql) {
    this.sql = sql;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public int getThreads() {
    return threads;
  }

  public void setThreads(int threads) {
    this.threads = threads;
  }

  public boolean isSelectQuery() {
    return isSelectQuery;
  }

  public void setSelectQuery(boolean selectQuery) {
    isSelectQuery = selectQuery;
  }

  public boolean isConsumeResult() {
    return isConsumeResult;
  }

  public void setConsumeResult(boolean consumeResult) {
    isConsumeResult = consumeResult;
  }

  public boolean isCountInStatistics() {
    return isCountInStatistics;
  }

  public void setCountInStatistics(boolean countInStatistics) {
    isCountInStatistics = countInStatistics;
  }

  public static final class QuerySliceBuilder {
    private String id = "q" + System.nanoTime();
    private String sql;
    private String type = "unclassfied";
    private int threads = 1;
    private boolean isSelectQuery = true;
    private boolean isConsumeResult = true;
    private boolean isCountInStatistics = true;

    private QuerySliceBuilder() {
    }

    public static QuerySliceBuilder aQuerySlice() {
      return new QuerySliceBuilder();
    }

    public QuerySliceBuilder withId(String id) {
      this.id = id;
      return this;
    }

    public QuerySliceBuilder withSql(String sql) {
      this.sql = sql;
      return this;
    }

    public QuerySliceBuilder withType(String type) {
      this.type = type;
      return this;
    }

    public QuerySliceBuilder withThreads(int threads) {
      this.threads = threads;
      return this;
    }

    public QuerySliceBuilder withIsSelectQuery(boolean isSelectQuery) {
      this.isSelectQuery = isSelectQuery;
      return this;
    }

    public QuerySliceBuilder withIsConsumeResult(boolean isConsumeResult) {
      this.isConsumeResult = isConsumeResult;
      return this;
    }

    public QuerySliceBuilder withIsCountInStatistics(boolean isCountInStatistics) {
      this.isCountInStatistics = isCountInStatistics;
      return this;
    }

    public QuerySlice build() {
      QuerySlice querySlice = new QuerySlice();
      querySlice.setId(id);
      querySlice.setSql(sql);
      querySlice.setType(type);
      querySlice.setThreads(threads);
      querySlice.isSelectQuery = this.isSelectQuery;
      querySlice.isConsumeResult = this.isConsumeResult;
      querySlice.isCountInStatistics = this.isCountInStatistics;
      return querySlice;
    }
  }
}
