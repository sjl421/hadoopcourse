package com.zhangyu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;

public class MapFiles {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		
		if(args.length != 1){
			System.out.println("Usage: outFilePath");
			System.exit(1);
		}
		String[] data = {
				"java,scala,python",
				"hadoop,hive,hbase",
				"spark rdd,spark sql,spark streaming",
				"spark,spark mllib,spark Graphx",
				"zk,storm,yarn"
		} ;
		Configuration conf = new Configuration() ;
		FileSystem hdfs = FileSystem.get(conf) ;
		//args[0] hdfs://CDH1:8020/zhangyu/data/zhangyu-MapFile
		MapFile.Writer writer = null ;
		IntWritable key = new IntWritable() ;
		Text value = new Text() ;
		try {
			writer = new MapFile
					.Writer(conf,hdfs , args[0], key.getClass(), value.getClass()) ;
			for(int i=0; i<100; i++){
				key.set(i+1);
				value.set(data[i%data.length]);
				System.out.printf("%s\t%s\n",key,value);
				writer.append(key, value) ;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			IOUtils.closeStream(writer);
		}
	}
}
