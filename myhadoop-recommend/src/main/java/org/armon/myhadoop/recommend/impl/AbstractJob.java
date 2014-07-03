package org.armon.myhadoop.recommend.impl;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.armon.myhadoop.recommend.MyJob;
import org.armon.myhadoop.util.myHaoopUtil;

public class AbstractJob implements MyJob {
  
  private Configuration conf;
  protected Job job;
  private Thread jobThread;
  private boolean isJobComplete;
  
  public AbstractJob(Configuration conf) {
    this.conf = conf;
  }
  
  class JobThread implements Runnable {
    private Job job;
    
    public JobThread(Job job) {
      this.job = job;
      isJobComplete = false;
    }

    @Override
    public void run() {
      boolean result = false;
      try {
        result = job.waitForCompletion(true);
      } catch (ClassNotFoundException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      isJobComplete = result;
    }   
  }

  protected Job initilizeJob(Map<String, String> path) throws Exception {
    return null;
  }

  @Override
  public boolean completeJob() throws Exception {
    isJobComplete = job.waitForCompletion(true);
    return isJobComplete;
  }
  
  @Override
  public String getName() {
    return getClass().getSimpleName();
  }
  
  @Override
  public void threadCompleteJob() throws Exception {
    JobThread jt = new JobThread(job);
    jobThread = new Thread(jt);
    jobThread.start();
  }
  
  @Override
  public void waitJobComplete() throws IOException {
    int i = 0;
    while (jobThread != null && jobThread.isAlive()) {
      try {
        System.out.println("Waiting for " + getName() +  " to complete...");
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
      if (++i > 30) {
        System.err.println("Timed out waiting for " + getName() + " to complete.");
        throw new IOException("Timed out waiting for " + getName() + " to complete.");
      } 
    }
    if (i < 30) {
      System.out.println(getName() +  " complete, result is " + isJobComplete);
    }
  }
  
  protected Job prepareJob(
      Class<? extends Mapper> mapper,
     Class<? extends Writable> mapperKey,
     Class<? extends Writable> mapperValue,
     Class<? extends InputFormat> inputFormat,
     Class<? extends OutputFormat> outputFormat, 
     Configuration conf,
     Path outputPath,
     Path... inputPaths) throws IOException {
    return myHaoopUtil.prepareJob(mapper, mapperKey, mapperValue, 
        inputFormat, outputFormat, conf,
        outputPath, inputPaths);
  }
  
  protected Job prepareJob(
      Class<? extends Mapper> mapper,
      Class<? extends Writable> mapperKey,
      Class<? extends Writable> mapperValue,
      Class<? extends Reducer> reducer,
      Class<? extends Writable> reducerKey,
      Class<? extends Writable> reducerValue,
      Class<? extends InputFormat> inputFormat,
      Class<? extends OutputFormat> outputFormat, 
      Configuration conf,
      Path outputPath,
      Path... inputPaths) throws IOException {
    return myHaoopUtil.prepareJob(mapper, mapperKey, mapperValue, 
        reducer, reducerKey, reducerValue, 
        inputFormat, outputFormat, conf,
        outputPath, inputPaths);
  }
  
  protected Job prepareJob(
      Class<? extends Reducer> reducer,
      Class<? extends Writable> reducerKey,
      Class<? extends Writable> reducerValue,
      Class<? extends InputFormat> inputFormat,
      Class<? extends OutputFormat> outputFormat, 
      Configuration conf,
      Path outputPath) throws IOException {
    return myHaoopUtil.prepareJob(reducer, reducerKey, reducerValue, 
        inputFormat, outputFormat, conf,
        outputPath);
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
