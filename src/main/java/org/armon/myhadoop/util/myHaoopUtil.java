package org.armon.myhadoop.util;

import org.apache.hadoop.mapred.JobConf;

public class myHaoopUtil {
  
  public static JobConf getJobConf(String className) throws ClassNotFoundException {
    Class<?> clazz = Class.forName(className);
    JobConf conf = new JobConf(clazz);
    conf.setJobName(clazz.getSimpleName());
    conf.addResource("classpath:/hadoop/core-site.xml");
    conf.addResource("classpath:/hadoop/hdfs-site.xml");
    conf.addResource("classpath:/hadoop/mapred-site.xml");
    return conf;
  }
  
  public static JobConf makeCopy(JobConf original) {
    if(original == null)
      return null;

    return new JobConf(original);
  }
}
