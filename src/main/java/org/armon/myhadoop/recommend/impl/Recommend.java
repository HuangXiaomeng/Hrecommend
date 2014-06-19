package org.armon.myhadoop.recommend.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.mapred.JobConf;
import org.armon.myhadoop.recommend.Step;
import org.armon.myhadoop.util.myHaoopUtil;

public class Recommend {

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

    path.put("Step5Input1", path.get("Step3Output"));
    path.put("Step5Input2", path.get("Step2Output"));
    path.put("Step5Output", path.get("Step1Input") + "/step5");

    path.put("Step6Input", path.get("Step5Output"));
    path.put("Step6Output", path.get("Step1Input") + "/step6");
    
    JobConf conf = myHaoopUtil.getJobConf(Recommend.class.getName());
    Step step1 = new Step1(myHaoopUtil.makeCopy(conf));
    Step step2 = new Step2(myHaoopUtil.makeCopy(conf));
    Step step3 = new Step3(myHaoopUtil.makeCopy(conf));
    Step step4_1 = new Step4_Update(myHaoopUtil.makeCopy(conf));
    Step step4_2 = new Step4_Update2(myHaoopUtil.makeCopy(conf));
    
    step1.run(path);
    step1.waitJobFinish();
    
    step2.run(path);
    step2.waitJobFinish();
    
    step3.run(path);
    step3.waitJobFinish();
    
    step4_1.run(path);
    
    step4_2.run(path);
    
    // Step4.run(path);

    // // hadoop fs -cat /user/hdfs/recommend/step4/part-00000
    // JobConf conf = config();
    // HdfsDAO hdfs = new HdfsDAO(HDFS, conf);
    // hdfs.cat("/user/hdfs/recommend/step4/part-00000");
    
    System.out.println("recommend finish!");
    System.exit(0);
  }
}
