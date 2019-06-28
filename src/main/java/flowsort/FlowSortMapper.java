package flowsort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @program: FristDemo
 * @description: Mapper
 * @author: 张清
 * @create: 2019-05-08 11:38
 **/
public class FlowSortMapper extends Mapper<LongWritable, Text, FlowBeanSort, Text> {
    FlowBeanSort flowBeanSort = new FlowBeanSort();
    Text v = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");
        String phone = fields[0];
        long upFlow = Long.parseLong(fields[1]);
        long downFlow = Long.parseLong(fields[2]);
        flowBeanSort.set(upFlow,downFlow);
        v.set(phone);
        context.write(flowBeanSort,v);
    }
}
