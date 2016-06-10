package com.zhangyu;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
public class RenameFile {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: inFilePath renameFilePath") ;
			System.exit(1) ;
		}	
		Configuration conf = new Configuration() ;
		FileSystem hdfs = FileSystem.get(conf) ;
		//原文件路径          hdfs://CDH1:8020/zhangyu/zhangyu-dir
		//重命名后路径     hdfs://CDH1:8020/zhangyu/zhangyu-rename
		Path src = new Path(args[0]) ;
		Path dst = new Path(args[1]) ;
		boolean rename = hdfs.rename(src, dst) ;
		if(rename){
			System.out.println("系统将原文件 "+ src.getName()+
					" 重命名为 "+dst.getName());
		}else{
			System.out.println("重命名没有成功!!!");
		}
	}
}
