package org.armon.myhadoop.recommend;

import java.io.IOException;
import java.util.Map;

public interface Step {
  
  public void run(Map<String, String> path) throws IOException;
  
}
