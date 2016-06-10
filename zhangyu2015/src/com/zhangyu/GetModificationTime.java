package com.zhangyu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class GetModificationTime {

	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			System.err.println("Usage: inFilePath") ;
			System.exit(1) ;
		}	
		Configuration conf = new Configuration() ;
		FileSystem hdfs = FileSystem.get(conf) ;
		//�ļ�·��	hdfs://CDH1:8020/zhangyu/
		System.out.println("�鿴���ļ�·�� "+args[0]);
		Path src = new Path(args[0]) ;
		
		FileStatus[] fileStatus = hdfs.listStatus(src) ;
		for (FileStatus file : fileStatus) {
			System.out.println("�ļ����� "+file.getPath()
					+" �޸�ʱ�䣺"+file.getModificationTime());
		}
	}

}
