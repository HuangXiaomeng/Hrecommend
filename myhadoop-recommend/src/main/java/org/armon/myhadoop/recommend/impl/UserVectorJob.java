package org.armon.myhadoop.recommend.impl;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.armon.myhadoop.hdfs.HdfsDAO;

/****************************************************************
 * Step1
 *****************************************************************/
public class UserVectorJob extends AbstractJob {
  
  public UserVectorJob(Configuration conf, Map<String, String> path) throws Exception {
    super(conf);
    job = initilizeJob(path);
  }

  public static class Step1_ToItemPreMapper extends
      Mapper<Object, Text, IntWritable, Text> {

    @Override
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      String[] tokens = RecommendMain.DELIMITER.split(value.toString());
      int userID = Integer.parseInt(tokens[0]);
      String itemID = tokens[1];
      String pref = tokens[2];
      IntWritable k = new IntWritable(userID);
      Text v = new Text(itemID + ":" + pref);
      context.write(k, v);
    }
  }

  public static class Step1_ToUserVectorReducer extends
      Reducer<IntWritable, Text, IntWritable, Text> {

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      StringBuilder sb = new StringBuilder();
      for (Text line: values) {
        sb.append("," + line);
      }
      Text v = new Text(sb.toString().replaceFirst(",", ""));
      context.write(key, v);
    }
  }

  @Override
  protected Job initilizeJob(Map<String, String> path) throws Exception {
    Configuration conf = getConf();

    String input = path.get("Step1Input");
    String output = path.get("Step1Output");

    HdfsDAO hdfs = new HdfsDAO(conf);
    hdfs.rmr(output);
    hdfs.rmr(input);
    hdfs.mkdirs(input);
    hdfs.copyFile(path.get("data"), input);
    
    Job job = prepareJob(Step1_ToItemPreMapper.class, IntWritable.class, Text.class, 
        Step1_ToUserVectorReducer.class, IntWritable.class, Text.class, 
        TextInputFormat.class, TextOutputFormat.class, conf,
        new Path(output), new Path(input));
    
    return job;
    
//    hdfs.cat(output + "/part-00000");
  }

}
