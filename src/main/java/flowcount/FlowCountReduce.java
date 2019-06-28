package flowcount;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @program: FristDemo
 * @description: reduce
 * @author: 张清
 * @create: 2019-04-24 11:20
 **/
public class FlowCountReduce extends Reducer<Text, FlowBean, Text, FlowBean> {
    long sum_upFlow = 0;
    long sum_downFlow = 0;
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        for (FlowBean flowBean : values) {
             sum_upFlow += flowBean.getUpFlow();
             sum_downFlow += flowBean.getDownFlow();
        }
        FlowBean resultBean = new FlowBean(sum_upFlow,sum_downFlow);
        context.write(key, resultBean);
    }
}
