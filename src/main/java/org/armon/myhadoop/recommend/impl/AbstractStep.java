package org.armon.myhadoop.recommend.impl;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.armon.myhadoop.recommend.Step;
import org.armon.myhadoop.util.myHaoopUtil;

public class AbstractStep implements Step {
  
  private Configuration conf;
  
  public AbstractStep(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void run(Map<String, String> path) throws IOException {
    
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
