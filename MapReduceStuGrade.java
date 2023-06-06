package com.org.blue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MapReduceStuGrade {
    public static class Mapper1 extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>{
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            String[] split = value.toString().split(",");
            String stuName = split[1];
            String stuClassType = split[3];
            String stuGrade = split[4];
            if (stuClassType.equals("必修")){
                context.write(new Text(stuName), new Text(stuGrade));
            }        

        }
    }
    public static class Reducer1 extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>{
        @Override
        protected void reduce(Text k2, Iterable<Text> v2s,
                              Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            int count = 0;
            double sum = 0.0;
            for (Text v2 : v2s) {
                count++;
                sum += Double.parseDouble(v2.toString());
            }
            double avg = sum / count;
            String avgStr = String.format("%.2f", avg);
            context.write(k2, new Text(avgStr));
        }
    }
    public static void main(String[] args)throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, MapReduceStuGrade.class.getSimpleName());
        job.setJarByClass(MapReduceStuGrade.class);
        FileInputFormat.setInputPaths(job, args[0]);
        job.setMapperClass(Mapper1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(Reducer1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
   
}
