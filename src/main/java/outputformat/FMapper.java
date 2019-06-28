package outputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @program: FristDemo
 * @description: mapper
 * @author: 张清
 * @create: 2019-05-15 11:42
 **/
public class FMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    Text k = new Text();
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        k.set(line);
        context.write(k, NullWritable.get());
    }

}
