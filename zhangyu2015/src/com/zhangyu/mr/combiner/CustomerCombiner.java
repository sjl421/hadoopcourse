package com.zhangyu.mr.combiner;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
public class CustomerCombiner extends Configured implements Tool{
	public CustomerCombiner() {
	}
	public static class smapper extends MapReduceBase 
	implements Mapper<LongWritable, Text, Text, IntWritable>{
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			StringTokenizer itr = new StringTokenizer(value.toString(),"-") ;
			String first = null ;
			String second = null ;
			first = itr.nextToken() ;
			second = itr.nextToken() ;
			System.out.println("first= "+first+" "+"second= "+second);
			output.collect(new Text(first), new IntWritable(1)) ;
		}
	}
	public static class sreduce extends MapReduceBase 
	implements Reducer<Text, IntWritable, Text, IntWritable>{
		@Override
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			IntWritable result = new IntWritable() ;
			int sum = 0 ;
			while(values.hasNext()){
				sum += values.next().get() ;
			}
			result.set(sum);
			System.out.println(key+" reduce数量："+sum);
			output.collect(key, result) ;
		}
	}
	//设置分区
	public static class NewPartitioner extends HashPartitioner<Text, IntWritable>{
		@Override
		public int getPartition(Text key, IntWritable value, int numReduceTasks) {
			String term = key.toString().split(":")[0] ;
			System.out.println(term);
			return super.getPartition(new Text(term), value, numReduceTasks);
		}
	}
	//设置combine
	public static class NewCombiner extends MapReduceBase 
	implements Reducer<Text, IntWritable, Text, IntWritable>{
		@Override
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			IntWritable result = new IntWritable() ;
			int sum = 0 ;
			while(values.hasNext()){
				sum += values.next().get() ;
			}
			result.set(sum);
			System.out.println(key+" combiner数量："+sum);
			output.collect(key, result) ;
		}
	}
	public CustomerCombiner(Configuration conf){
		super(conf) ;
	}
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: inFilePath  outPath");
			System.exit(1);
		}
		Configuration conf = new Configuration() ;
		JobConf job = new JobConf(conf,CustomerCombiner.class) ;
		job.setMapperClass(smapper.class) ;
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(sreduce.class) ;
		
		job.setPartitionerClass(NewPartitioner.class) ;
		
		job.setCombinerClass(NewCombiner.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0])) ;
		FileOutputFormat.setOutputPath(job, new Path(args[1])) ;
		JobClient.runJob(job) ;
		return 0;
	}
	public static void main(String[] args) throws Exception {
		int exit = ToolRunner.run(new CustomerCombiner(), args) ;
		System.exit(exit) ;
	}
}
