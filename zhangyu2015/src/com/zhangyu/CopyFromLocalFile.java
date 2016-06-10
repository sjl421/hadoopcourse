package com.zhangyu;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CopyFromLocalFile {

	public static void main(String[] args) throws IOException {
		
		Configuration conf = new Configuration() ;
		FileSystem hdfs = FileSystem.get(conf) ;
		
		if(args.length != 2){
			System.out.println("Usage:inPutFilePath outPutFilePath");
			System.exit(1);
		}
		//D:\data\zhangyu-data.txt
		Path src = new Path(args[0]) ;
		//hdfs://CDH1:8020/zhangyu/outdata/
		Path dst = new Path(args[1]) ;
		
		hdfs.copyFromLocalFile(src, dst);
		
		System.out.println("上传文件到 "+conf.get("fs.default.name"));
		FileStatus[] files = hdfs.listStatus(dst) ;
		for(FileStatus file : files){
			System.out.println(file.getPath());
		}

	}

}
