package com.serialization;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;


import java.io.*;

/**
 * @program: FristDemo
 * @description: 中文的词频统计
 * @author: 张清
 * @create: 2019-04-03 19:25
 **/
public class WordCountCn {
    public static class ForMapper extends Mapper<Object,Text, Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            byte[] bt = value.getBytes();
            InputStream ip = new ByteArrayInputStream(bt);
            Reader reader = new InputStreamReader(ip);
            IKSegmenter iks = new IKSegmenter(reader,true);
            Lexeme t;
            while((t = iks.next()) != null){
                word.set(t.getLexemeText());
                context.write(word,one);
            }
        }
    }

    public static class ForReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        private IntWritable result = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val:values){
                sum += val.get();
            }
            result.set(sum);
            context.write(key,result);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
//        Job job = Job.getInstance();
        Job job = new Job(conf,"word count");
        job.setJarByClass(WordCountCn.class);
        job.setMapperClass(ForMapper.class);
        job.setReducerClass(ForReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        FileInputFormat.setInputPaths(job,new Path("D://zhangqing.txt"));
        FileOutputFormat.setOutputPath(job,new Path("D://1"));
        job.waitForCompletion(true);
    }
}
