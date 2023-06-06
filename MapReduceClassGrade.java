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
// 按科目统计每个班的平均成绩
import java.io.IOException;
public class MapReduceClassGrade {

    public static class Mapper2 extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>{
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            String[] spilt = value.toString().split(",");
            String ClassID = spilt[0];
            String CourseName = spilt[2];
            String Grade = spilt[4];
            context.write(new Text(ClassID + "," + CourseName), new Text(Grade));
            // map 输出的key是班级号和科目名，value是成绩
        }
    }

    public static class Reducer2 extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>{
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
            // reduce 输出的key是班级号和科目名，value是平均成绩
        }
    }
    public static void main(String[] args)throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, MapReduceClassGrade.class.getSimpleName());
        job.setJarByClass(MapReduceClassGrade.class);
        FileInputFormat.setInputPaths(job, args[0]);
        job.setMapperClass(Mapper2.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(Reducer2.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
 
}
