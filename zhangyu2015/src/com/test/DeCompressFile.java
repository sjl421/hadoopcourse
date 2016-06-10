package com.test;

import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

public class DeCompressFile {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		/*
		 *  args[0]	org.apache.hadoop.io.compress.GzipCodec
		 *  args[1]	hdfs://CDH1:8020/zhangyu/outdata/zhangyu-createData.gzip
		 */
		if(args.length != 2){
			System.out.println("Usage: FilePath");
			System.exit(1);
		}
		//����ѹ���ļ�
		Path src = new Path(args[0]) ;
		//�ж�ѹ������
		CompressionCodecFactory factory = new CompressionCodecFactory(conf) ;
		CompressionCodec codec = factory.getCodec(src) ;
		if(codec == null){
			System.out.println("���ļ��Ƿ�ѹ����ʽ");
			return ;
		}
		//ɾ����׺��
		String outPutFile = CompressionCodecFactory
				.removeSuffix(args[0], codec.getDefaultExtension()) ;
		
		FSDataInputStream inputStream = hdfs.open(new Path(args[0]));
		// ��ѹ���ļ��ﵽ���ݽ�ѹ��Ȼ�����������̨
		InputStream in = codec.createInputStream(inputStream);
		OutputStream out = hdfs.create(new Path(args[1]+outPutFile)) ;
		IOUtils.copyBytes(in, out, conf);
		IOUtils.closeStream(in);
	}

}
