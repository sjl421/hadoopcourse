package com.zhangyu;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
public class DeleteFile {
	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			System.err.println("Usage: delFilePath") ;
			System.exit(1) ;
		}	
		Configuration conf = new Configuration() ;
		FileSystem hdfs = FileSystem.get(conf) ;
		//删除文件路径	hdfs://CDH1:8020/zhangyu/zhangyu-rename
		Path src = new Path(args[0]) ;
		boolean delete = hdfs.delete(src, false) ;
		if(delete){
			System.out.println("成功删除文件： "+ src.getName());
		}else{
			System.out.println("删除文件没有成功!!!");
		}
	}
}
