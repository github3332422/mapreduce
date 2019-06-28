package flowsort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * @program: FristDemo
 * @description: reduce
 * @author: 张清
 * @create: 2019-05-08 11:45
 **/
public class FlowSortReducer extends Reducer <FlowBeanSort, Text, Text, FlowBeanSort>{
    @Override
    protected void reduce(FlowBeanSort key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text text : values) {
            context.write(text, key);
        }
    }
}
