package org.armon.myhadoop.recommend.impl;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.armon.myhadoop.recommend.Step;

public class AbstractStep implements Step {
  
  private JobConf conf;
  protected RunningJob job;
  
  public AbstractStep(JobConf conf) {
    this.conf = conf;
  }

  @Override
  public void run(Map<String, String> path) throws IOException {
    
  }
  
  @Override
  public void waitJobFinish() throws IOException {
    while (!job.isComplete()) {
      job.waitForCompletion();
    }
  }

  public JobConf getConf() {
    return conf;
  }

  public void setConf(JobConf conf) {
    this.conf = conf;
  }

  public RunningJob getJob() {
    return job;
  }

  public void setJob(RunningJob job) {
    this.job = job;
  }
  
}
