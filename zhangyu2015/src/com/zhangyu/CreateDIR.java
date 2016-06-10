package com.zhangyu;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
public class CreateDIR {
	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			System.err.println("Usage: <CreateFilePath>") ;
			System.exit(1) ;
		}	
		Configuration conf = new Configuration() ;
		FileSystem hdfs = FileSystem.get(conf) ;
		//hdfs://CDH1:8020/zhangyu/zhangyu-dir
		Path path = new Path(args[0]) ;
		boolean mkdir = hdfs.mkdirs(path) ;
		if(mkdir){
			System.out.println("成功创建目录："+ path.getName());
		}else{
			System.out.println("创建目录没有成功!!!");
		}
	}
}
