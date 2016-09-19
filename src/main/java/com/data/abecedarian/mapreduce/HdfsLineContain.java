package com.data.abecedarian.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by tianle.li on 2016/9/12.
 * <p/>
 * This class is used to find content that contains specific conditions.
 */
public class HdfsLineContain extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println(args.length);
            for (String arg : args) {
                System.err.println(arg);
            }
            System.err.println("please input four params: <inputPattern>,<output>,<condition>,<reduceNum>");
            System.exit(1);
        }
        int status = ToolRunner.run(new HdfsLineContain(), args);
        System.exit(status);

    }

    public static String readInputPath(Configuration conf, Path pathPattern) throws IOException {
        String path = null;
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] fileStatuses = fs.globStatus(pathPattern);
        for (FileStatus fss : fileStatuses) {
            if (path == null) {
                path = fss.getPath().toString();
            } else {
                path = fss.getPath().toString() + "," + path;
            }
        }
        return path;

    }

    @Override
    public int run(String[] args) throws Exception {
        String inputPath = args[0];
        String outputPath = args[1];
        String condition = args[2];
        String reduceNum = args[3];

        Configuration conf = this.getConf();
        Path dstPath = new Path(outputPath);
        FileSystem dhfs = dstPath.getFileSystem(conf);
        if (dhfs.exists(dstPath)) {
            dhfs.delete(dstPath, true);
        }
//        conf.set("mapred.job.queue.name", "wirelessdev");
//        conf.set("mapred.job.priority", JobPriority.VERY_HIGH.name());
        conf.set("mapreduce.job.name", "HdfsLineContain.By.tianle.li");
        conf.setLong("mapreduce.task.timeout", 6000000);
        conf.setLong("mapred.task.timeout", 6000000);

        conf.setBoolean("mapred.compress.map.output", true);
        conf.setClass("mapred.map.output.compression.codec", Lz4Codec.class, CompressionCodec.class);
        conf.setBoolean("mapred.output.compress", true);
        conf.setClass("mapred.output.compression.codec", GzipCodec.class, CompressionCodec.class);

        conf.set("condition", condition);

        Job job = Job.getInstance(conf);
        job.setJarByClass(HdfsLineContain.class);

        job.setMapperClass(LineContainMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(LineContainReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(Integer.parseInt(reduceNum));
        FileInputFormat.setInputPaths(job, readInputPath(conf, new Path(inputPath)));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        boolean success = job.waitForCompletion(true);
        if (success) {
            System.out.println("result are saved to:" + new Path(outputPath).toString());
        }
        return success ? 0 : 1;
    }


    public static class LineContainMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        String condition = "";

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            condition = conf.get("condition");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.contains(condition)) {
                context.write(key, value);
            }
        }

    }

    public static class LineContainReducer extends Reducer<LongWritable, Text, NullWritable, Text> {
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(NullWritable.get(), value);
            }
        }

    }

}
