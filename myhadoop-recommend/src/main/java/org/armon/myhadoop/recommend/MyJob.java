package org.armon.myhadoop.recommend;

import java.util.Map;

public interface MyJob {
  
  public void run(Map<String, String> path) throws Exception;
  
}
