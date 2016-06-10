package com.zhangyu.mr.writable;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class City {
	
	public static class TokenizerMapper extends Mapper<Object,Text,Text,TextPair>{

		@Override
		protected void map(Object key, Text value,Context context)
				throws IOException, InterruptedException {
			StringTokenizer line = new StringTokenizer(value.toString()) ; 
			System.out.println("每行数据： "+value.toString());
			Text local = new Text(line.nextToken()) ;
			System.out.println("省份： "+local);
			TextPair tp = new TextPair(line.nextToken(),line.nextToken()) ;
			System.out.println("省份对应的前两个城市： "+tp);
			context.write(local, tp);
		}
	}
	
	public static class IntSumRedcer extends Reducer<Text,TextPair,Text,Text>{

		@Override
		protected void reduce(Text k2, Iterable<TextPair> v2s,Context context)
				throws IOException, InterruptedException {
			String s =": " ;
			for (TextPair textPair : v2s) {
				s+=textPair.toString() ;
			}
			context.write(k2, new Text(s));
		}
	}

	public static void main(String[] args) throws Exception {
		
		if (args.length != 2) {
			System.err.println("Usage: inFilePath outPath");
			System.exit(1);
		}
		Configuration conf = new Configuration();
		Job job = new Job(conf, "city");
		job.setJarByClass(City.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setMapperClass(TokenizerMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TextPair.class);
		
		job.setReducerClass(IntSumRedcer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(TextOutputFormat.class);
		System.exit(job.waitForCompletion(true) ? 0:1);
		
	}

}
