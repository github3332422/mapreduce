package com.serialization;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * @program: FristDemo
 * @description: 写入文件
 * @author: 张清
 * @create: 2019-04-03 10:51
 **/
public class FileWrite {
    public static void main(String[] args) throws IOException {
        String textFile = "hello World \nhello hadoop " + "\nhello zq hello word ";
        //获取环境变量
        Configuration configuration = new Configuration();
        //生成文件系统
        FileSystem fileSystem = FileSystem.get(configuration);
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("D://zhangqing.txt"));
        fsDataOutputStream.write(textFile.getBytes());
        fsDataOutputStream.close();
        fileSystem.close();
    }
}
