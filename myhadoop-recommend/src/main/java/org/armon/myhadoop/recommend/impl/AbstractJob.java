package org.armon.myhadoop.recommend.impl;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.armon.myhadoop.recommend.MyJob;
import org.armon.myhadoop.util.myHaoopUtil;

public class AbstractJob implements MyJob {
  
  private Configuration conf;
  
  public AbstractJob(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void run(final Map<String, String> path) throws Exception {
    
  }
  
  protected void prepareJob(Path inputPath, Path outputPath,
      Class<? extends InputFormat> inputFormat, 
      Class<? extends Mapper> mapper,
      Class<? extends Writable> mapperKey,
      Class<? extends Writable> mapperValue,
      Class<? extends Reducer> reducer,
      Class<? extends Writable> reducerKey,
      Class<? extends Writable> reducerValue,
      Class<? extends OutputFormat> outputFormat, 
      Configuration conf) throws IOException {
    myHaoopUtil.prepareJob(inputPath, outputPath, inputFormat, mapper, 
        mapperKey, mapperValue, reducer, reducerKey, reducerValue, outputFormat, conf);
  }
  
  public Configuration getConf() {
    if (conf == null) {
      conf = myHaoopUtil.getConf();
    }
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

}