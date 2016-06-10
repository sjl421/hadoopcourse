package com.zhangyu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

public class CompressCodecFile {

	public static void main(String[] args) throws Exception {
		
		if(args.length != 3){
			System.out.println("Usage: CodecClass inFilePath outFilePath");
			System.exit(1);
		}
        Configuration conf = new Configuration();
        Class<?> codecClass = Class.forName(args[0]);
        FileSystem fs = FileSystem.get(conf);
        Path src = new Path(args[1]) ;
        Path dst = new Path(args[2]) ;
        CompressionCodec codec = (CompressionCodec) ReflectionUtils
        		.newInstance(codecClass, conf);
        // 输入和输出均为hdfs路径
        FSDataInputStream in = fs.open(src);
        FSDataOutputStream outputStream = fs.create(dst);
          
        System.out.println("compress start !");
        // 创建压缩输出流
        CompressionOutputStream out = codec.createOutputStream(outputStream);
        IOUtils.copyBytes(in, out, conf);
        IOUtils.closeStream(in);
        IOUtils.closeStream(out);
        System.out.println("compress ok !");

	}

}
