package com.serialization;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @program: FristDemo
 * @description: 测试类
 * @author: 张清
 * @create: 2019-03-27 16:21
 **/
public class HdfsDemo3 {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        FileSystem fs;
        String url = "hdfs://192.168.96.129:8020";
        FileSystem fileSystem = FileSystem.get(new URI(url),new Configuration(),"hadoop");

        Path path = new Path("/zq/1.txt");
        FSDataInputStream fsin = fileSystem.open(path);
        byte[] buff = new byte[128];
        int length = 0;
        while ((length = fsin.read(buff, 0, 128)) != -1) {
            //将数据读入缓存数组
            System.out.println(new String(buff, 0, length));
        }
        System.out.println("length =" + fsin.getPos());

        fsin.seek(0);
        while ((length=fsin.read(buff,0,128))!=-1){
            System.out.println(new String(buff,0,length));
        }

        fsin.seek(5);
        byte[] buff2=new byte[128];
        fsin.read(buff2,0,128);
        System.out.println("buff2 = "+new String(buff2));
        System.out.println(buff2.length);
    }
}
