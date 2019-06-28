package com.serialization;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @program: FristDemo
 * @description: HDFS写入文件
 * @author: 张清
 * @create: 2019-03-27 10:39
 **/
public class HdfsWrite {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs ;
        String url = "hdfs://192.168.96.129:8020";
        fs = FileSystem.get(new URI(url),conf,"hadoop");
//        创建文件
//        fs.mkdirs(new Path("/2016021214"));

//        写入文件
//        FSDataOutputStream fsDataOutputStream = fs.create(new Path("/myhdfs/a.txt"));
//        byte[] buffer = "Hello World".getBytes();
//        fsDataOutputStream.write(buffer);
//        fsDataOutputStream.close();
//        IOUtils.closeStream(fsDataOutputStream);//关闭写出流

//        读出文件
//        FSDataInputStream fsDataInputStream = fs.open(new Path("/zq/3.txt"));
//        byte[] buff1 = new byte[512];
//        int length = 0;
//        while((length = fsDataInputStream.read(buff1,0,512)) != -1){
//            System.out.println(new String(buff1,0,length));
//        }

//        IO流写入文件
        // 1 创建输入流
        FileInputStream fileInputStream = new FileInputStream("F:/3.txt");
        // 2 获取输出流
        FSDataOutputStream fsDataOutputStream = fs.create(new Path("/zq/3.txt"));
        // 3 流的对拷
        IOUtils.copyBytes(fileInputStream, fsDataOutputStream, new Configuration());
        // 4 关闭资源
        IOUtils.closeStream(fileInputStream);
        IOUtils.closeStream(fsDataOutputStream);
        System.out.println("文件上传成功");
//        IO流读出文件
        // 1 获取输入流
//        FSDataInputStream fsDataInputStream = fs.open(new Path("/zq/1.txt"));
//        // 2 获取输出流
//        FileOutputStream fileOutputStream = new FileOutputStream(new File("D:/1.txt"));
//        // 3 流的对拷
//        IOUtils.copyBytes(fsDataInputStream, fileOutputStream, new Configuration());
//        // 4 关闭资源
//        IOUtils.closeStream(fsDataInputStream);
//        IOUtils.closeStream(fileOutputStream);
//        关闭资源
        fs.close();
    }
}
