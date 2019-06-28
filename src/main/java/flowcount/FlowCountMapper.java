package flowcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @program: FristDemo
 * @description: map
 * @author: 张清
 * @create: 2019-04-24 11:08
 **/
public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    Text k = new Text();
    FlowBean v = new FlowBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] files = line.split("\t");
        String phone = files[1];
        long upFlow = Long.parseLong(files[files.length-3]);
        long downFlow = Long.parseLong(files[files.length-2]);
        v.set(upFlow,downFlow);
        k = new Text(phone);
        v = new FlowBean(upFlow,downFlow);
        context.write(k, v);
    }
}
