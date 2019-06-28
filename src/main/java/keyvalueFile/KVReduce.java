package keyvalueFile;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @program: FristDemo
 * @description: reduce
 * @author: 张清
 * @create: 2019-05-15 10:41
 **/
public class KVReduce extends Reducer<Text, LongWritable,Text,LongWritable>{
    LongWritable v = new LongWritable();
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count = 0;
        for (LongWritable value : values) {
            count += value.get();
        }
        v.set(count);
        context.write(key, v);
    }
}
