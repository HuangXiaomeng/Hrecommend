package org.armon.myhadoop.recommend.impl;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.armon.myhadoop.hdfs.HdfsDAO;

public class ItemMatrixJob extends AbstractJob {
  
  public ItemMatrixJob(Configuration conf) {
    super(conf);
  }

  public static class Step3_UserVectorSplitterMapper extends
      Mapper<LongWritable, Text, IntWritable, Text> {
    private final static IntWritable k = new IntWritable();
    private final static Text v = new Text();

    @Override
    public void map(LongWritable key, Text values, Context context)
        throws IOException, InterruptedException {
      String[] tokens = Recommend.DELIMITER.split(values.toString());
      for (int i = 1; i < tokens.length; i++) {
        String[] vector = tokens[i].split(":");
        int itemID = Integer.parseInt(vector[0]);
        String pref = vector[1];

        k.set(itemID);
        v.set(tokens[0] + ":" + pref);
        context.write(k, v);
      }
    }
  }

  @Override
  public void run(Map<String, String> path) throws Exception {
    Configuration conf = getConf();

    String input = path.get("Step3Input");
    String output = path.get("Step3Output");

    HdfsDAO hdfs = new HdfsDAO(conf);
    hdfs.rmr(output);
    
    Job job = new Job(conf);
    
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setMapperClass(Step3_UserVectorSplitterMapper.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.setInputPaths(job, new Path(input));
    FileOutputFormat.setOutputPath(job, new Path(output));
    
    
    job.waitForCompletion(true);
  }
}
