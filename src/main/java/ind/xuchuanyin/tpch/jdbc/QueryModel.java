package ind.xuchuanyin.tpch.jdbc;

import java.util.List;

import org.apache.log4j.Logger;

public class QueryModel {
  private static final Logger LOGGER = Logger.getLogger(QueryModel.class);
  // common properties
  private String jdbcUrl;
  private String jdbcUser;
  private String jdbcPwd;
  private String jdbcDriver;
  private int jdbcPoolSize = 20;
  private int execIteration = 1;
  private int execInterval = 1;
  private int execConcurrentSize = 1;
  private boolean shuffleExecute = false;
  private String reportStore;

  private List<QuerySlice> querySlices;

  public QueryModel() {
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("QueryModel{");
    sb.append("jdbcUrl='").append(jdbcUrl).append('\'');
    sb.append(", jdbcUser='").append(jdbcUser).append('\'');
    sb.append(", jdbcPwd='").append(jdbcPwd).append('\'');
    sb.append(", jdbcDriver='").append(jdbcDriver).append('\'');
    sb.append(", jdbcPoolSize=").append(jdbcPoolSize);
    sb.append(", execIteration=").append(execIteration);
    sb.append(", execInterval=").append(execInterval);
    sb.append(", execConcurrentSize=").append(execConcurrentSize);
    sb.append(", shuffleExecute=").append(shuffleExecute);
    sb.append(", reportStore='").append(reportStore).append('\'');
    sb.append(", querySlices=").append(querySlices);
    sb.append('}');
    return sb.toString();
  }

  public String getJdbcUrl() {
    return jdbcUrl;
  }

  public void setJdbcUrl(String jdbcUrl) {
    this.jdbcUrl = jdbcUrl;
  }

  public String getJdbcUser() {
    return jdbcUser;
  }

  public void setJdbcUser(String jdbcUser) {
    this.jdbcUser = jdbcUser;
  }

  public String getJdbcPwd() {
    return jdbcPwd;
  }

  public void setJdbcPwd(String jdbcPwd) {
    this.jdbcPwd = jdbcPwd;
  }

  public String getJdbcDriver() {
    return jdbcDriver;
  }

  public void setJdbcDriver(String jdbcDriver) {
    this.jdbcDriver = jdbcDriver;
  }

  public int getJdbcPoolSize() {
    return jdbcPoolSize;
  }

  public void setJdbcPoolSize(int jdbcPoolSize) {
    this.jdbcPoolSize = jdbcPoolSize;
  }

  public int getExecIteration() {
    return execIteration;
  }

  public void setExecIteration(int execIteration) {
    this.execIteration = execIteration;
  }

  public int getExecInterval() {
    return execInterval;
  }

  public void setExecInterval(int execInterval) {
    this.execInterval = execInterval;
  }

  public int getExecConcurrentSize() {
    return execConcurrentSize;
  }

  public void setExecConcurrentSize(int execConcurrentSize) {
    this.execConcurrentSize = execConcurrentSize;
  }

  public boolean isShuffleExecute() {
    return shuffleExecute;
  }

  public void setShuffleExecute(boolean shuffleExecute) {
    this.shuffleExecute = shuffleExecute;
  }

  public String getReportStore() {
    return reportStore;
  }

  public void setReportStore(String reportStore) {
    this.reportStore = reportStore;
  }

  public List<QuerySlice> getQuerySlices() {
    return querySlices;
  }

  public void setQuerySlices(List<QuerySlice> querySlices) {
    this.querySlices = querySlices;
  }

  public static final class QueryModelBuilder {
    // common properties
    private String jdbcUrl;
    private String jdbcUser;
    private String jdbcPwd;
    private String jdbcDriver;
    private int jdbcPoolSize = 20;
    private int execIteration = 1;
    private int execInterval = 1;
    private int execConcurrentSize = 1;
    private boolean shuffleExecute = false;
    private String reportStore;
    private List<QuerySlice> querySlices;

    private QueryModelBuilder() {
    }

    public static QueryModelBuilder aQueryModel() {
      return new QueryModelBuilder();
    }

    public QueryModelBuilder withJdbcUrl(String jdbcUrl) {
      this.jdbcUrl = jdbcUrl;
      return this;
    }

    public QueryModelBuilder withJdbcUser(String jdbcUser) {
      this.jdbcUser = jdbcUser;
      return this;
    }

    public QueryModelBuilder withJdbcPwd(String jdbcPwd) {
      this.jdbcPwd = jdbcPwd;
      return this;
    }

    public QueryModelBuilder withJdbcDriver(String jdbcDriver) {
      this.jdbcDriver = jdbcDriver;
      return this;
    }

    public QueryModelBuilder withJdbcPoolSize(int jdbcPoolSize) {
      this.jdbcPoolSize = jdbcPoolSize;
      return this;
    }

    public QueryModelBuilder withExecIteration(int execIteration) {
      this.execIteration = execIteration;
      return this;
    }

    public QueryModelBuilder withExecInterval(int execInterval) {
      this.execInterval = execInterval;
      return this;
    }

    public QueryModelBuilder withExecConcurrentSize(int execConcurrentSize) {
      this.execConcurrentSize = execConcurrentSize;
      return this;
    }

    public QueryModelBuilder withShuffleExecute(boolean shuffleExecute) {
      this.shuffleExecute = shuffleExecute;
      return this;
    }

    public QueryModelBuilder withReportStore(String reportStore) {
      this.reportStore = reportStore;
      return this;
    }

    public QueryModelBuilder withQuerySlices(List<QuerySlice> querySlices) {
      this.querySlices = querySlices;
      return this;
    }

    public QueryModel build() {
      QueryModel queryModel = new QueryModel();
      queryModel.setJdbcUrl(jdbcUrl);
      queryModel.setJdbcUser(jdbcUser);
      queryModel.setJdbcPwd(jdbcPwd);
      queryModel.setJdbcDriver(jdbcDriver);
      queryModel.setJdbcPoolSize(jdbcPoolSize);
      queryModel.setExecIteration(execIteration);
      queryModel.setExecInterval(execInterval);
      queryModel.setExecConcurrentSize(execConcurrentSize);
      queryModel.setShuffleExecute(shuffleExecute);
      queryModel.setReportStore(reportStore);
      queryModel.setQuerySlices(querySlices);
      return queryModel;
    }
  }
}
