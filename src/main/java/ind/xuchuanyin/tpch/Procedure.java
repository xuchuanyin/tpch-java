package ind.xuchuanyin.tpch;

public interface Procedure {
  void setInputFiles(String... inputFiles);

  void ignite() throws Exception;

  void close();
}
