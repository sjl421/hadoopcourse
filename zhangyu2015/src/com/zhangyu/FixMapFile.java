package com.zhangyu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;

public class FixMapFile {

	@SuppressWarnings("all")
	public static void main(String[] args) throws Exception {

		if(args.length != 1){
			System.out.println("Usage: inFilePath");
			System.exit(1);
		}
		Configuration conf = new Configuration() ;
		FileSystem hdfs = FileSystem.get(conf) ;
		//args[0] hdfs://CDH1:8020/zhangyu/data/zhangyu-MapFile/
		Path src = new Path(args[0]) ;
		Path mapData = new Path(src,MapFile.DATA_FILE_NAME) ;
		SequenceFile.Reader reader = new SequenceFile.Reader(hdfs, mapData,conf) ;
		Class keyClass = reader.getKeyClass() ;
		Class valueClass = reader.getValueClass() ;
		reader.close(); 
		long entry = MapFile.fix(hdfs, src, keyClass, valueClass, false, conf) ;
		System.out.printf("创建一个mapfile %s 和 %d 的索引\n",src,entry);
		
	}

}
