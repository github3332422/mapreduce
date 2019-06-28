package com.serialization;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class hdfsDemo {

    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        FileSystem fs;
        String url = "hdfs://192.168.96.129:8020";
        fs = FileSystem.get(new URI(url),new Configuration(),"hadoop");
        /**
         * 创建文件夹
         * */
//        fs.mkdirs(new Path("/zq"));

        /**
         * 删除文件或文件夹
         * recursive,删除文件置为false
         * */
//        fs.delete(new Path("/zq/writeSample.txt"),false);
//        System.out.println("文件删除成功");

        /**
         *上传文件
         * */
//        fs.copyFromLocalFile(new Path("F:/1.txt"),new Path("/zq/"));

        /**
         *下载文件
         * */
//        fs.copyToLocalFile(new Path("/zq/1.txt"),new Path("D:/1.txt"));

        /**
         * 拷贝文件
         * 拷贝的时候，会把本来的文件给删了
         * */
//        fs.rename(new Path("/forCL/example2.txt"), new Path("/zq/1.txt"));

        /**
         *写入文件
         * */
//        FSDataOutputStream outputStream = fs.create(new Path("/zq/1.txt"));
////        for (int i = 0; i < 100; i++) {
////            outputStream.write(("hello hadoop" + i + "\n").getBytes());
////        }
////        outputStream.flush();
////        outputStream.close();

        /**
         * 查看文件
         * */
        FSDataInputStream inputStream = fs.open(new Path("/forCL/test.txt"));
        byte[] temp = new byte[512];
        int len;
        while ((len = inputStream.read(temp)) != -1)
            System.out.println(new String(temp, 0, len));

        /**
         * 查看目录的使用情况
         * */
//        FileStatus fileStatus = fs.getFileStatus(new Path("/forCL/test.txt"));
//        System.out.println("AccessTime:"+fileStatus.getAccessTime());
//        System.out.println("Replication:"+fileStatus.getReplication());
//        System.out.println("getLen:"+fileStatus.getLen());
//        System.out.println("Owner:"+fileStatus.getOwner());
//        System.out.println("getGroup:"+fileStatus.getGroup());
//        System.out.println("BlockSize:"+fileStatus.getBlockSize());

        /**
         * 查看文件的情况
         * */
//        FsStatus fsStatus = fs.getStatus(new Path("/"));
//        System.out.println(fsStatus.getUsed());
//        System.out.println(fsStatus.getCapacity());
//        System.out.println(fsStatus.getRemaining());

        /**
         * 关闭文件
         * */
        fs.close();
    }
}
