package com.zhangyu;

import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class CatFile {
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration() ;
		//args[0]=hdfs://CDH1:8020/zhangyu/data/wordcount
		if (args.length != 1) {
			System.err.println("Usage: <inFilePath>") ;
			System.exit(1) ;
		}	
		Path path = new Path(args[0]) ;
		FileSystem hdfs = FileSystem.get(URI.create(args[0]),conf) ;
		InputStream in = hdfs.open(path) ;
		/**
		 * copyBytes((in, out, buffSize, close)
		 * in	是原文件路径
		 * out	是hdfs输出路径，或输出控制台
		 * buffSize 是缓冲大小
		 * close	是否关闭流
		 */
		IOUtils.copyBytes(in, System.out, 1024, true);
	}
}
