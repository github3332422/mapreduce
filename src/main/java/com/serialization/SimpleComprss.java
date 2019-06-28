package com.serialization;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;

import java.io.IOException;


/**
 * @program: FristDemo
 * @description: ...
 * @author: 张清
 * @create: 2019-04-17 10:37
 **/
public class SimpleComprss {
    public static void main(String[] args) throws IOException {
        //生成环境变量
        Configuration conf = new Configuration();
        //获取文件系统
        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(args[0]);
        FSDataOutputStream os = fs.create(outputPath);
        //生成压缩格式实例
        CompressionCodec codec = new GzipCodec();
        byte[] buff = "hello world hello hadoop".getBytes();
        CompressionOutputStream outputStream = codec.createOutputStream(os);
        outputStream.write(buff);
        outputStream.close();
    }
}
