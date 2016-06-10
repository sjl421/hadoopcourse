package com.zhangyu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class WriteFile {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration() ;
		FileSystem hdfs = FileSystem.get(conf) ;
		
		if(args.length != 1){
			System.out.println("Usage: FilePath");
			System.exit(1);
		}
		//hdfs://CDH1:8020/zhangyu/data/zhangyu-createData.txt
		Path path = new Path(args[0]) ;
		
		byte[] buff = "hello zhangyu...write...".getBytes() ;
		
		FSDataOutputStream create = hdfs.create(path) ;
		create.write(buff, 0, buff.length);
		
		System.out.println("成功写入内容");

	}

}
