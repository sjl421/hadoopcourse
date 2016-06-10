package com.zhangyu;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
public class FileLocation {
	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			System.err.println("Usage: inFilePath");
			System.exit(1);
		}
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		// 文件路径 hdfs://CDH1:8020/zhangyu/outdata/zhangyu-data.txt
		Path src = new Path(args[0]);
		FileStatus file = hdfs.getFileStatus(src) ;
		long start = 0 ;
		long len = file.getLen() ;
		BlockLocation[] blockLocations = hdfs.getFileBlockLocations(file, start, len) ;
		int blen = blockLocations.length ;
		System.out.println("blen : "+blen);
		for (int i=0; i<blen; i++) {
			String[] hosts = blockLocations[i].getHosts() ;
			for(int j=0; j<hosts.length; j++){
				System.out.println("block "+i+" location:"+hosts[j] );
			}
		}
	}
}
