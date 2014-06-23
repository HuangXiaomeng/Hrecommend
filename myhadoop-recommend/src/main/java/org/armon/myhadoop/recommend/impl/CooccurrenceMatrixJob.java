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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.armon.myhadoop.hdfs.HdfsDAO;

/****************************************************************
 * Step2
 *****************************************************************/
public class CooccurrenceMatrixJob extends AbstractJob {
  
  public CooccurrenceMatrixJob(Configuration conf) {
    super(conf);
  }

  public static class Step2_UserVectorToCooccurrenceMapper extends
      Mapper<LongWritable, Text, Text, IntWritable> {
    private final static Text k = new Text();
    private final static IntWritable v = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text values, Context context)
        throws IOException, InterruptedException {
      String[] tokens = RecommendMain.DELIMITER.split(values.toString());
      for (int i = 1; i < tokens.length; i++) {
        String itemID = tokens[i].split(":")[0];
        for (int j = 1; j < tokens.length; j++) {
          String itemID2 = tokens[j].split(":")[0];
          k.set(itemID + ":" + itemID2);
          context.write(k, v);
        }
      }
    }
  }

  public static class Step2_UserVectorToConoccurrenceReducer extends
      Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable i : values) {
        sum += i.get();
      }
      IntWritable v = new IntWritable(sum);
      context.write(key, v);
    }
  }
  
  @Override
  public void run(Map<String, String> path) throws Exception {
    Configuration conf = getConf();

    String input = path.get("Step2Input");
    String output = path.get("Step2Output");

    HdfsDAO hdfs = new HdfsDAO(conf);
    hdfs.rmr(output);

    Job job = prepareJob(Step2_UserVectorToCooccurrenceMapper.class, Text.class, IntWritable.class, 
        Step2_UserVectorToConoccurrenceReducer.class, Text.class, IntWritable.class, 
        TextInputFormat.class, TextOutputFormat.class, conf,
        new Path(output), new Path(input));
    
    job.waitForCompletion(true);
    
//    hdfs.cat(output + "/part-00000");
  }
}
