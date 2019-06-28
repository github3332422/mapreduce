package keyvalueFile;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @program: FristDemo
 * @description: map
 * @author: 张清
 * @create: 2019-05-15 10:35
 **/
public class KVMapper extends Mapper<Text, Text, Text, LongWritable> {
    final Text k = new Text();
    LongWritable v = new LongWritable();
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        k.set(key);
        v.set(1);
        context.write(k,v);
    }



}
