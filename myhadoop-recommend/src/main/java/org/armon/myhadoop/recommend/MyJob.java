package org.armon.myhadoop.recommend;

import java.io.IOException;

public interface MyJob {
  
  public boolean completeJob() throws Exception;
  
  public void threadCompleteJob() throws Exception;
  
  public void waitJobComplete() throws IOException ;
  
  public String getName();
  
}
