package com.serialization;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;

import java.io.IOException;

/**
 * @program: FristDemo
 * @description:
 * @author: 张清
 * @create: 2019-04-17 11:26
 **/
public class ucCompress1 {
    public static void main(String[] args) throws IOException {
        //生成环境变量
        Configuration conf = new Configuration();
        //获取文件系统
        FileSystem fs = FileSystem.get(conf);

        //CompressionCodec gzipCodec = ReflectionUtils.newInstance(GzipCodec.class, conf);
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        Path inputPath = new Path(args[0]);
        CompressionCodec codec=factory.getCodec(inputPath);

        FSDataInputStream open = fs.open(inputPath);
        CompressionInputStream inputStream = codec.createInputStream(open);
        byte[] buff=new byte[128];
        int length=0;
        while ((length=inputStream.read(buff,0,128))!=-1){
            System.out.println(new String(buff,0,length));
        }
    }

}
