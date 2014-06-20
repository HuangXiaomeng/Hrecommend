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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.armon.myhadoop.hdfs.HdfsDAO;

public class Step1 extends AbstractStep {
  
  public Step1(Configuration conf) {
    super(conf);
  }

  public static class Step1_ToItemPreMapper extends
      Mapper<Object, Text, IntWritable, Text> {

    @Override
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      String[] tokens = Recommend.DELIMITER.split(value.toString());
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
  public void run(Map<String, String> path) throws IOException {
    Configuration conf = getConf();

    String input = path.get("Step1Input");
    String output = path.get("Step1Output");

    HdfsDAO hdfs = new HdfsDAO(conf);
    hdfs.rmr(output);
    hdfs.rmr(input);
    hdfs.mkdirs(input);
    hdfs.copyFile(path.get("data"), input);
    
    Job job = new Job(conf);
    job.setJarByClass(Step1.class);
    
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class); 

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(Step1_ToItemPreMapper.class);
    job.setCombinerClass(Step1_ToUserVectorReducer.class);
    job.setReducerClass(Step1_ToUserVectorReducer.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.setInputPaths(job, new Path(input));
    FileOutputFormat.setOutputPath(job, new Path(output));

    try {
      job.waitForCompletion(true);
    } catch (ClassNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
//    hdfs.cat(output + "/part-00000");
  }

}
