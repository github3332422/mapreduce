package com.serialization;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @program: FristDemo
 * @description: 词频统计
 * @author: 张清
 * @create: 2019-04-03 10:18
 **/
public class WordCount {
    //map 将文本内容转化为字符串
    //map 的输入值，输出值，输出键，输出值
    public static class ForMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
        private Text okey=new Text();
        private IntWritable ovalue=new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String str []= value.toString().split(" ");
            for(String s:str){
                okey.set(s);
                context.write(okey,ovalue);
            }
        }
    }
    //reduce
    public static class ForReduce extends Reducer<Text, IntWritable,Text,IntWritable>{
//        private IntWritable ovalue=new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            //对每一个值进行处理
            for (IntWritable i:values){
//                sum ++;
                sum += i.get();
            }
//            ovalue.set(sum);
            context.write(key,new IntWritable(sum));
//            context.write(key,ovalue);
        }
    }

    //job
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance();
        job.setJarByClass(WordCount.class);
        job.setMapperClass(ForMapper.class);
        job.setReducerClass(ForReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path path = new Path(args[1]);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileSystem fs = FileSystem.get(new Configuration());
        if(fs.exists(path)){
            fs.delete(path,true);
        }
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
//        FileInputFormat.setInputPaths(job,new Path("D://zq.txt"));
//        FileOutputFormat.setOutputPath(job,new Path("D://12"));
        job.waitForCompletion(true);
    }
}
