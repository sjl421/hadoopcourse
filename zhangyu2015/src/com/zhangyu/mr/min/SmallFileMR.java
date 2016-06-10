package com.zhangyu.mr.min;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.zhangyu.mr.allin.WholeFileInputFormat;
public class SmallFileMR {
	public static class MyMapper extends Mapper<NullWritable, Text, Text, Text>{
		@Override
		protected void map(NullWritable key, Text value,
				Mapper<NullWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			System.out.println("value: \n"+value);
			FileSplit fsp = (FileSplit) context.getInputSplit() ;
			String fileName = fsp.getPath().getName()+"\n" ;
			context.write(new Text(fileName), value);
		}
	}
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		if (args.length != 4) {
			System.err.println("Usage: inFilePath1 inFilePath2 inFilePath3 outPath");
			System.exit(1);
		}
		Configuration conf = new Configuration();
		Job job = new Job(conf, "SmallFileMR");
		job.setJarByClass(SmallFileMR.class);
		/**
		 *  hdfs://CDH1:8020/zhangyu/data/wordcount
			hdfs://CDH1:8020/zhangyu/data/wordcounts.txt
			hdfs://CDH1:8020/zhangyu/data/city.txt
			hdfs://CDH1:8020/zhangyu/outdata/fileminout
		 */
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileInputFormat.addInputPath(job, new Path(args[2]));
		job.setInputFormatClass(WholeFileInputFormat.class);
		
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		System.exit(job.waitForCompletion(true) ? 0:1);
	}
}
