package org.armon.myhadoop.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;

public class myHaoopUtil {

  public static Configuration getConf() {
    Configuration conf = new Configuration();
    conf.addResource("hadoop/core-site.xml");
    conf.addResource("hadoop/hdfs-site.xml");
    // conf.addResource("hadoop/mapred-site.xml");
    return conf;
  }

  public static Configuration makeConfCopy(Configuration original) {
    if (original == null)
      return null;

    return new Configuration(original);
  }

  public static Job prepareJob(Path inputPath, Path outputPath,
      Class<? extends InputFormat> inputFormat, 
      Class<? extends Mapper> mapper,
      Class<? extends Writable> mapperKey,
      Class<? extends Writable> mapperValue,
      Class<? extends Reducer> reducer,
      Class<? extends Writable> reducerKey,
      Class<? extends Writable> reducerValue,
      Class<? extends OutputFormat> outputFormat, 
      Configuration conf) throws IOException {

    Job job = new Job(new Configuration(conf));
    Configuration jobConf = job.getConfiguration();

    if (reducer.equals(Reducer.class)) {
      if (mapper.equals(Mapper.class)) {
        throw new IllegalStateException(
            "Can't figure out the user class jar file from mapper/reducer");
      }
      job.setJarByClass(mapper);
    } else {
      job.setJarByClass(reducer);
    }

    job.setInputFormatClass(inputFormat);
    jobConf.set("mapred.input.dir", inputPath.toString());

    job.setMapperClass(mapper);
    if (mapperKey != null) {
      job.setMapOutputKeyClass(mapperKey);
    }
    if (mapperValue != null) {
      job.setMapOutputValueClass(mapperValue);
    }

    jobConf.setBoolean("mapred.compress.map.output", true);

    job.setReducerClass(reducer);
    job.setOutputKeyClass(reducerKey);
    job.setOutputValueClass(reducerValue);

    job.setOutputFormatClass(outputFormat);
    jobConf.set("mapred.output.dir", outputPath.toString());

    return job;
  }

  public static String getCustomJobName(String className, JobContext job,
      Class<? extends Mapper> mapper, Class<? extends Reducer> reducer) {
    StringBuilder name = new StringBuilder(100);
    String customJobName = job.getJobName();
    if (customJobName == null || customJobName.trim().isEmpty()) {
      name.append(className);
    } else {
      name.append(customJobName);
    }
    name.append('-').append(mapper.getSimpleName());
    name.append('-').append(reducer.getSimpleName());
    return name.toString();
  }
}
