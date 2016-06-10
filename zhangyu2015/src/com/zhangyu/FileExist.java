package com.zhangyu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileExist {

	public static void main(String[] args) throws Exception {

		if (args.length != 1) {
			System.err.println("Usage: inFilePath");
			System.exit(1);
		}
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		// 文件路径 hdfs://CDH1:8020/zhangyu
		Path src = new Path(args[0]);
		boolean file = hdfs.exists(src);
		if(file){
			System.out.println(src.getName()+ " 已存在");
		}else{
			System.out.println(src.getName()+ " 不存在");
		}
	}

}
