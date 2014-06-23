package org.armon.myhadoop.recommend.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.armon.myhadoop.recommend.MyJob;
import org.armon.myhadoop.util.myHaoopUtil;

public class RecommendMain {

  public static final String HDFS = "hdfs://localhost:9000";
  public static final Pattern DELIMITER = Pattern.compile("[\t,]");

  public static void main(String[] args) throws Exception {
    Map<String, String> path = new HashMap<String, String>();
    path.put("data", "testdata/recommend/small.csv");
    path.put("Step1Input", HDFS + "/user/hdfs/recommend");
    path.put("Step1Output", path.get("Step1Input") + "/step1");
    path.put("Step2Input", path.get("Step1Output"));
    path.put("Step2Output", path.get("Step1Input") + "/step2");
    path.put("Step3Input", path.get("Step1Output"));
    path.put("Step3Output", path.get("Step1Input") + "/step3");

    path.put("Step4Input1", path.get("Step3Output"));
    path.put("Step4Input2", path.get("Step2Output"));
    path.put("Step4Output", path.get("Step1Input") + "/step4");

    path.put("Step5Input", path.get("Step4Output"));
    path.put("Step5Output", path.get("Step1Input") + "/step5");
    
    Configuration conf = myHaoopUtil.getConf();
    // step 1
    MyJob job1 = new UserVectorJob(myHaoopUtil.makeConfCopy(conf));
    job1.run(path);
    
    // step 2
    MyJob job2 = new CooccurrenceMatrixJob(myHaoopUtil.makeConfCopy(conf));
    job2.run(path);
    
    // step 3
    MyJob job3 = new ItemMatrixJob(myHaoopUtil.makeConfCopy(conf));
    job3.run(path);
    
    // step 4
    MyJob job4 = new PartialMultiplyJob(myHaoopUtil.makeConfCopy(conf));
    job4.run(path);
    
    // step 5
    MyJob job5 = new CalcRecommendJob(myHaoopUtil.makeConfCopy(conf));
    job5.run(path);
    
    System.out.println("recommend finish!");
    System.exit(0);
  }
}
