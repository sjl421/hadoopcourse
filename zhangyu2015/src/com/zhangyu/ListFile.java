package com.zhangyu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class ListFile {
	public static void main(String[] args) throws Exception {
		//基本配置
		Configuration conf = new Configuration() ;
		FileSystem hdfs = FileSystem.get(conf) ;
		//args[0]=hdfs://CDH1:8020/zhangyu
		if (args.length != 1) {
			System.err.println("Usage: <inFilePath>") ;
			System.exit(1) ;
		}	
		//获取某个目录下所有文件的各种信息
		FileStatus[] files = hdfs.listStatus(new Path(args[0])) ;	
		for(FileStatus file:files){
			System.out.println("file = "+file) ;
		}
		//获取某个目录下所有文件的路径
		Path[] listPaths = FileUtil.stat2Paths(files) ;		
		for(Path path:listPaths){
			System.out.println("path = "+path) ;
		}
	}
}
