package com.zhangyu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.common.IOUtils;

public class ReaderMapFile {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {

		if(args.length != 1){
			System.out.println("Usage: inFilePath");
			System.exit(1);
		}
		Configuration conf = new Configuration() ;
		FileSystem hdfs = FileSystem.get(conf) ;
		MapFile.Reader reader = null ;
		//args[0] hdfs://CDH1:8020/zhangyu/data/zhangyu-MapFile/
		try {
			reader = new MapFile.Reader(hdfs,args[0],conf) ;
			Text value = new Text() ;
			int key = 1 ;
			while(reader.next(new IntWritable(key),value)){
				System.out.printf("%s\t%s\n",key,value);
				key++ ;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			IOUtils.closeStream(reader);
		}
	}

}
