package com.zhangyu;

import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;

public class UnCompressCodecFile {
	public static void main(String[] args) throws Exception {
		if(args.length != 2){
			System.out.println("Usage: CodecClass outFilePath");
			System.exit(1);
		}
        Configuration conf = new Configuration();
        Class<?> codecClass = Class.forName(args[0]);
        FileSystem fs = FileSystem.get(conf);
        CompressionCodec codec = (CompressionCodec) ReflectionUtils
                .newInstance(codecClass, conf);
        FSDataInputStream inputStream = fs.open(new Path(args[1]));
        // 把压缩文件进行解压，然后输出到控制台
        InputStream in = codec.createInputStream(inputStream);
        IOUtils.copyBytes(in, System.out, conf);
        IOUtils.closeStream(in);
	}
}
