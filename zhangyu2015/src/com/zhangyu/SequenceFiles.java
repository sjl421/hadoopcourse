package com.zhangyu;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class SequenceFiles {


	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		
		String[] data = {
				"one,two,buklet my shoe",
				"three,four,shut the door",
				"file,six,pick up sticks",
				"seven,eight,lay then straight",
				"nine,ten,a big fat then"
		} ;
		Configuration conf = new Configuration() ;
		FileSystem hdfs = FileSystem.get(conf) ;
		// hdfs://CDH1:8020/zhangyu/data/zhangyu-Data.txt 
		Path src = new Path(args[0]) ;
		SequenceFile.Writer writer = null ;
		IntWritable key = new IntWritable() ;
		Text value = new Text() ;
		try {
			writer = SequenceFile
					.createWriter(hdfs, conf, src, key.getClass(), value.getClass()) ;
			for(int i=0; i<100; i++){
				key.set(100-i);
				value.set(data[i%data.length]);
				System.out.printf("[%s]\t%s\t%s\n",writer.getLength(),key,value);
				writer.append(key, value) ;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			IOUtils.closeStream(writer);
		}
	}
}
