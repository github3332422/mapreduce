package com.serialization;

import org.apache.hadoop.conf.Configuration;
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
import java.util.StringTokenizer;

/**
 * @program: FristDemo
 * @description: 词频排序
 * @author: 张清
 * @create: 2019-04-03 21:47
 **/
public class WordSort {
    public static class SortIntValueMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        private final static IntWritable wordCount = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken().trim());
                wordCount.set(Integer.valueOf(tokenizer.nextToken().trim()));
                context.write(wordCount, word);//<k,v>互换
            }
        }
    }

    public static class SortIntValueReduce extends Reducer<IntWritable, Text, Text, IntWritable> {
        private Text result = new Text();

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                result.set(val.toString());
                context.write(result, key);//<k,v>互换
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
//        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//        if (otherArgs.length != 2) {
//            System.err.println("Usage: wordsort <in> <out>");
//            System.exit(2);
//        }

        Job job = new Job(conf, "word sort");
        job.setJarByClass(WordSort.class);

        job.setMapperClass(SortIntValueMapper.class);
        job.setReducerClass(SortIntValueReduce.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("D://part-r-00000.txt"));

        FileOutputFormat.setOutputPath(job, new Path("D://part"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
