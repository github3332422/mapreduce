package outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @program: FristDemo
 * @description:
 * @author: 张清
 * @create: 2019-05-15 11:28
 **/
public class FRecorWriter extends RecordWriter<Text, NullWritable> {
    FSDataOutputStream atguigu = null;
    FSDataOutputStream other = null;
    public FRecorWriter(TaskAttemptContext  job){
        FileSystem fs;
        try {
            fs = FileSystem.get(job.getConfiguration());
            Path atguiguPath = new Path("F:\\data\\atguigu.log");
            Path otherPath = new Path("F:\\data\\other.log");
            atguigu =fs.create(atguiguPath);
            other =fs.create(otherPath);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void write(Text key, NullWritable value) throws IOException{
        if (key.toString().contains("atguigu")) {
            atguigu.write(key.toString().getBytes());
            atguigu.write('\n');
        } else {
            other.write(key.toString().getBytes());
            other.write('\n');
        }
    }
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        if (atguigu != null) {
            atguigu.close();
        }

        if (other != null) {
            other.close();
        }
    }
}
