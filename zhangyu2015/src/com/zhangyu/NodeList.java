package com.zhangyu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

public class NodeList {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration() ;
		DistributedFileSystem hdfs = (DistributedFileSystem) FileSystem.get(conf) ;
		DatanodeInfo[] nodes = hdfs.getDataNodeStats() ;
		String[] names = new String[nodes.length] ;
		for(int i=0; i<names.length; i++){
			names[i] = nodes[i].getHostName() ;
			System.out.println("dataNode "+i+" 名称是："+names[i]);
		}
	}
}
