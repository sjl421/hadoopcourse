package com.zhangyu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class ReaderSequenceFiles {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {

		if(args.length != 2){
			System.out.println("Usage: inFilePath  outFilePath");
			System.exit(1);
		}
		Configuration conf = new Configuration() ;
		FileSystem hdfs = FileSystem.get(conf) ;
		//args[0] hdfs://CDH1:8020/zhangyu/data/zhangyu-data.txt
		//args[1] hdfs://CDH1:8020/zhangyu/outdata/zhangyu-readerSeq.txt
		Path src = new Path(args[0]) ;
		Path dst = new Path(args[1]) ;
		SequenceFile.Reader reader = null ;
		FSDataOutputStream create = hdfs.create(dst) ;
		try {
			reader = new SequenceFile.Reader(hdfs,src,conf) ;
			Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf) ;
			Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf) ;
			long position = reader.getPosition() ;
			while(reader.next(key,value)){
				String sync = reader.syncSeen() ? "*" :"" ;
				System.out.printf("[%s%s]\t%s\t%s\n",position,sync,key,value);
				position = reader.getPosition() ;
				//写入到文本文件中
				Text v = (Text) value ;
				byte[] buff = v.getBytes() ;
				create.write(buff, 0, buff.length);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
