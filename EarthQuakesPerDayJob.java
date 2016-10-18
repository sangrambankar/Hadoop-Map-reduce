import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EarthQuakesPerDayJob {

 public static void main(String[] args) throws Throwable {

 JobConf conf = new JobConf(EarthQuakesPerDayJob.class);
 conf.setJobName("EarthQuakesPerDayJob");

 conf.setNumMapTasks(4);
 conf.setNumReduceTasks(2);
 

 
  Job job = new Job(conf);
  job.setJarByClass(EarthQuakesPerDayJob.class);
  FileInputFormat.addInputPath(job, new Path(args[0]));

  FileSystem fs = FileSystem.get(new URI(args[1]),conf);
  if(fs.exists(new Path(args[1])))
     fs.delete(new Path(args[1]),true);
  
  FileOutputFormat.setOutputPath(job, new Path(args[1]));
  
  
  job.setMapperClass(EarthQuakesPerDateMapper.class);
  job.setReducerClass(EarthQuakesPerDateReducer.class);
  job.setOutputKeyClass(Text.class);
  job.setOutputValueClass(Text.class);
  
  System.exit(job.waitForCompletion(true) ? 0 : 1);
 }
}