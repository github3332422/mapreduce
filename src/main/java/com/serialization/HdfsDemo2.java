package com.serialization;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Random;

/**
 * @program: FristDemo
 * @description: 测试类
 * @author: 张清
 * @create: 2019-03-27 16:03
 **/
public class HdfsDemo2 {
    static int index = 0;
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        String url = "hdfs://192.168.96.129:8020";
        FileSystem fileSystem = FileSystem.get(new URI(url),conf,"hadoop");

        StringBuffer sb = new StringBuffer();
        //创建辅助可变字符串
        Random rand = new Random();
        for (int i = 0; i < 9999999; i++) {       //随机写入字符
            sb.append((char) rand.nextInt(100));
        }
        final byte[] buff = sb.toString().getBytes();      //生成字符数组
        Path path = new Path("/zq/write.txt");     //创建路径
//        if(fileSystem.isFile(path) == false){
//            fileSystem.mkdirs(path);
//        }

//        使用该方法可以不用提前建立文件夹
        final FSDataOutputStream fsout = fileSystem.create(path, new Progressable() { //创建写出流
            public void progress() {       //默认的实用方法
                System.out.println(++index);     //打印出序列号
            }
        });
        fsout.write(buff);         //开始写出操作
        IOUtils.closeStream(fsout);       //关闭写出流
    }
}
