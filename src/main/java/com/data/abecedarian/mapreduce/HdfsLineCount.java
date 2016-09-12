package com.data.abecedarian.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by tianle.li on 2016/9/12.
 * <p/>
 * This class is used to count the number of lines file,Especially large hdfs files.
 */
public class HdfsLineCount extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println(args.length);
            for (String arg : args) {
                System.err.println(arg);
            }
            System.err.println("please input a params: <input>");
            System.exit(1);
        }
        int status = ToolRunner.run(new HdfsLineCount(), args);
        System.exit(status);
    }

    @Override
    public int run(String[] args) throws Exception {
        String inputPath = args[0];
        Configuration conf = this.getConf();
        conf.set("mapred.job.queue.name", "wirelessdev");
        conf.set("mapred.job.priority", JobPriority.VERY_HIGH.name());

        Job job = Job.getInstance(conf);
        job.setJobName("HdfsLineCount.By.tianle.li");
        job.setJarByClass(HdfsLineCount.class);
        job.setMapperClass(LineCountMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        boolean success = job.waitForCompletion(true);
        if (success) {
            System.out.println("file total line count:" + job.getCounters().findCounter(LineCountMapper.FileRecorder.TotalRecorder).getValue());
        }
        return success ? 0 : 1;
    }

    public static class LineCountMapper extends Mapper<LongWritable, Text, LongWritable, IntWritable> {
        public enum FileRecorder {
            TotalRecorder
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.getCounter(FileRecorder.TotalRecorder).increment(1);
        }
    }

}
