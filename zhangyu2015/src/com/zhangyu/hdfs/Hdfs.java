package com.zhangyu.hdfs;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumFileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;

public class Hdfs {
	
	public static void main(String[] args) throws IOException {
//		Configuration conf = new Configuration() ;
//		FileSystem hdfs = FileSystem.get(conf) ;
//		Path p = new Path(args[0]) ;
//		hdfs.create(p) ;
//		
//		Path path = new Path(args[0]) ;
//		FSDataOutputStream out = hdfs.create(path) ;
//		out.write("content".getBytes("UTF-8"));
//		out.flush();
//		out.sync() ;
//		out.close();
//		
//		hdfs.mkdirs(p) ;
		
		Configuration conf = new Configuration() ;
		FileSystem fs = new RawLocalFileSystem();
		fs.initialize(null, conf);
		
		
		FileSystem fss = new RawLocalFileSystem();
		FileSystem cfs = new ChecksumFileSystem(fss) {
		};
		
		
	}

}
