package org.armon.myhadoop.recommend.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.armon.myhadoop.hdfs.HdfsDAO;
import org.armon.myhadoop.recommend.impl.PartialMultiplyJob.Step4_AggregateReducer;
import org.armon.myhadoop.recommend.impl.PartialMultiplyJob.Step4_PartialMultiplyMapper;

/****************************************************************
 * Step5
 *****************************************************************/
public class CalcRecommendJob extends AbstractJob {
  
  public CalcRecommendJob(Configuration conf) {
    super(conf);
  }

  public static class Step5_RecommendMapper extends
      Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text values, Context context)
        throws IOException, InterruptedException {
      String[] tokens = RecommendMain.DELIMITER.split(values.toString());
      Text k = new Text(tokens[0]);
      Text v = new Text(tokens[1] + "," + tokens[2]);
      context.write(k, v);
    }
  }

  public static class Step5_RecommendReducer extends
      Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      System.out.println(key.toString() + ":");
      Map<String, Double> map = new HashMap<String, Double>();// 结果

      for (Text line : values) {
        System.out.println(line.toString());
        String[] tokens = RecommendMain.DELIMITER.split(line.toString());
        String itemID = tokens[0];
        Double score = Double.parseDouble(tokens[1]);

        if (map.containsKey(itemID)) {
          map.put(itemID, map.get(itemID) + score);// 矩阵乘法求和计算
        } else {
          map.put(itemID, score);
        }
      }

      Iterator<String> iter = map.keySet().iterator();
      while (iter.hasNext()) {
        String itemID = iter.next();
        double score = map.get(itemID);
        Text v = new Text(itemID + "," + score);
        context.write(key, v);
      }
    }
  }

  public void run(Map<String, String> path) throws Exception {
    Configuration conf = getConf();

    String input = path.get("Step5Input");
    String output = path.get("Step5Output");

    HdfsDAO hdfs = new HdfsDAO(conf);
    hdfs.rmr(output);

    Job job = prepareJob(Step5_RecommendMapper.class, Text.class, Text.class, 
        Step5_RecommendReducer.class, Text.class, Text.class, 
        TextInputFormat.class, TextOutputFormat.class, conf,
        new Path(output), new Path(input));

    job.waitForCompletion(true);
  }
}
