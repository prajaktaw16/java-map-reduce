import java.io.IOException;
import java.util.Scanner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Twitter {
    public static class MyMapper extends Mapper<Object,Text,IntWritable,IntWritable> {
        @Override
        public void map (Object key, Text value, Context context)
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int user_ID = s.nextInt();
            int follower_ID = s.nextInt();
            context.write(new IntWritable(follower_ID),new IntWritable(user_ID));
            //emit(follower_ID,user_ID);
            s.close();
        }
    }

    public static class MyReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
        @Override
        public void reduce ( IntWritable key, Iterable<IntWritable> values, Context context )
                           throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable v: values) {
                count++;
            };
            context.write(key,new IntWritable(count));
        }
    }
    
    public static class MyMapperTwo extends Mapper<Object,Text,IntWritable,IntWritable> {
        @Override
        public void map (Object key, Text value, Context context)
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter("\t");
            int follower_ID = s.nextInt();
            int count = s.nextInt();
            context.write(new IntWritable(count),new IntWritable(1));
            //emit(follower_ID,user_ID);
            s.close();
        }
    }

    public static class MyReducerTwo extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
        @Override
        public void reduce ( IntWritable count, Iterable<IntWritable> values, Context context )
                           throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v: values) {
            	sum =sum+ v.get();
            };
            context.write(count,new IntWritable(sum));
        }
    }

    public static void main ( String[] args ) throws Exception {
        Job job = Job.getInstance();
        job.setJobName("MyJob");
        job.setJarByClass(Twitter.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        job.waitForCompletion(true);
        
        Job job1 = Job.getInstance();
        job1.setJobName("MyJob 2");
        job1.setJarByClass(Twitter.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setMapperClass(MyMapperTwo.class);
        job1.setReducerClass(MyReducerTwo.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job1,new Path(args[1]));
        FileOutputFormat.setOutputPath(job1,new Path(args[2]));
        job1.waitForCompletion(true);
    }
}
