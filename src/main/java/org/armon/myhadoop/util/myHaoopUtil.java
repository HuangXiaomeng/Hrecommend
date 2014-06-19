package org.armon.myhadoop.util;

import org.apache.hadoop.conf.Configuration;

public class myHaoopUtil {
  
  public static Configuration getConf() {
    Configuration conf = new Configuration();
    conf.addResource("classpath:/hadoop/core-site.xml");
    conf.addResource("classpath:/hadoop/hdfs-site.xml");
    conf.addResource("classpath:/hadoop/mapred-site.xml");
    return conf;
  }
  
  public static Configuration makeConfCopy(Configuration original) {
    if(original == null)
      return null;

    return new Configuration(original);
  }
}
