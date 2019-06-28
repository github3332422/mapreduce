package com.serialization;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @program: FristDemo
 * @description: 测试类
 * @author: 张清
 * @create: 2019-03-27 15:34
 **/
public class HdfsDemo1 {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        String url = "hdfs://192.168.96.129:8020";
        FileSystem fileSystem = FileSystem.get(new URI(url),conf,"hadoop");
//        判断文件是否存在
//        Path path = new Path("/zq/2.txt");
//        System.out.println(fileSystem.isFile(path));

//        判断文件夹是否存在
//        Path path = new Path("/zq1");
//        System.out.println(fileSystem.isDirectory(path));

        Path dir = new Path("/zq");       //设置一个目录路径
        Path file = new Path(dir +"/2.txt");      //在设置目录下创建文件
        if(fileSystem.isFile(file) == true) {
            fileSystem.mkdirs(file);
        }
        fileSystem.copyFromLocalFile(new Path("F:/1.txt"),file);
        FileStatus status = fileSystem.getFileStatus(file);      //获取文件状态
        System.out.println(status.getPath());      //获取绝对路径
        System.out.println(status.getPath().toUri().getPath());  //获取相对路径
        System.out.println(status.getBlockSize());     //获取当前 Block 大小
        System.out.println(status.getGroup());     //获取所属组
        System.out.println(status.getOwner());     //获取所有者
//        fileSystem.delete(file, true);         //删除文件目录
        System.out.println(fileSystem.isFile(file));       //确认删除结果
    }
}
