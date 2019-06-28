package keyvalueFile;

import flowsort.FlowBeanSort;
import flowsort.FlowSortDriver;
import flowsort.FlowSortMapper;
import flowsort.FlowSortReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @program: FristDemo
 * @description: driver
 * @author: 张清
 * @create: 2019-05-15 10:47
 **/
public class KVDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR," ");
        Job job = Job.getInstance(configuration);

        job.setJarByClass(KVDriver.class);
        //map|reduce关联
        job.setMapperClass(KVMapper.class);
        job.setReducerClass(KVReduce.class);

        //map输出|总输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //使用KeyValueTextInputFormat
        //默认TextInputFormat
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        //输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //提交作业
        job.waitForCompletion(true);
    }
}
