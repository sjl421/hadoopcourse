package com.test;

import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

public class CompressiomCodecFile {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration() ;
		FileSystem hdfs = FileSystem.get(conf) ;
		/*
		 *  args[0]	org.apache.hadoop.io.compress.GzipCodec
		 *  args[1]	hdfs://CDH1:8020/zhangyu/data/zhangyu-createData.txt
		 *  args[2]	hdfs://CDH1:8020/zhangyu/outdata/zhangyu-createData.txt.gzip
		 */
		if(args.length != 3){
			System.out.println("Usage: codecClassFormat inFile outFile");
			System.exit(1);
		}
		//ָ��ѹ����ʽ
		String codecClassFormat = args[0] ;
		Class<?> codecClass = Class.forName(codecClassFormat) ;
		//δѹ�����ļ�
		Path src = new Path(args[1]) ;
		//ѹ�������ɵ��ļ�
		Path dst = new Path(args[2]) ;
		//ѹ��ʵ��������
		CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf) ;
		
		InputStream in = null ;
		FSDataOutputStream fsOut = null ;
		CompressionOutputStream cOut = null ;
		try {
			//����ѹ��д����
			fsOut = hdfs.create(dst) ;
			cOut = codec.createOutputStream(fsOut) ;
			//��ȡδѹ���ļ����Ĵ���
			in = hdfs.open(src) ;
			int length = 0 ;
			FileStatus[] files = hdfs.listStatus(src) ;
			//��ȡ�ļ�����
			for(FileStatus file:files){
				length = (int)file.getLen() ;
			}
			IOUtils.copyBytes(in, cOut, length, false) ;
			cOut.flush() ;
			System.out.println("�ɹ�����ѹ���ļ�");
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			cOut.flush() ;
			IOUtils.closeStream(in);
			IOUtils.closeStream(fsOut);
		}
	}
}
