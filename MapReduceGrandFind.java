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
// 从child-parent 关系找出 grandchild-grandparent 关系
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class MapReduceGrandFind {
    public static class Mapper3 extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>{
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            String[] spilt = value.toString().split(",");
            String child = spilt[0].trim();
            String parent = spilt[1].trim();
            context.write(new Text(child), new Text("p:" + parent));
            context.write(new Text(parent), new Text("c:" + child));
            // map 输出的key是孩子，value是父母，输出的key是父母，value是孩子
        }
    }

    public static class Reducer3 extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>{
//         private boolean first = true;
        
        
//         @Override
//         protected void setup(Context context) throws IOException, InterruptedException {
//             super.setup(context);
//             if(first){
//                 context.write(new Text("grandchild"), new Text("grandparent"));
//                 first = false;
//             }
//         }



        @Override
        protected void reduce(Text k2, Iterable<Text> v2s,
                              Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            Set<String> grandchilds = new HashSet<>(); //HashSet为了去重,不过题目设定似乎不需要
            Set<String> grandparents = new HashSet<>();
                     
            for(Text v2:v2s){
                String name =v2.toString();
                if(name.startsWith("p:")){
                    grandparents.add(name.substring(2));
                }else if(name.startsWith("c:")){
                    grandchilds.add(name.substring(2));
                }
            }
            // 组合输出
            for(String grandchild:grandchilds){
                for(String grandparent:grandparents){
                    context.write(new Text(grandchild), new Text(grandparent));
                }
            }
        }
    }
    public static void main(String[] args)throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, MapReduceGrandFind.class.getSimpleName());
        job.setJarByClass(MapReduceGrandFind.class);
        FileInputFormat.setInputPaths(job, args[0]);
        job.setMapperClass(Mapper3.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(Reducer3.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }


}
