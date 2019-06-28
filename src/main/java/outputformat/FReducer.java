package outputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @program: FristDemo
 * @description: reducer
 * @author: 张清
 * @create: 2019-05-15 11:44
 **/
public class FReducer extends Reducer<Text, LongWritable,Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        String k = key.toString();
        context.write(new Text(k),NullWritable.get());
    }
}
