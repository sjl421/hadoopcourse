package com.zhangyu.mr.partition;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
public class Customerpart extends Configured implements Tool{
	public Customerpart() {
	}
	public static class smapper extends MapReduceBase 
	implements Mapper<LongWritable, Text, Text, Text>{
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			StringTokenizer itr = new StringTokenizer(value.toString(),"-") ;
			String first = null ;
			String second = null ;
			first = itr.nextToken() ;
			second = itr.nextToken() ;
			System.out.println("first= "+first+" "+"second= "+second);
			output.collect(new Text(first), new Text(second)) ;
		}
	}
	public static class sreduce extends MapReduceBase 
	implements Reducer<Text, Text, Text, Text>{
		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			while(values.hasNext()){
				output.collect(key, values.next()) ;
			}
			
		}
	}
	//设置分区
	public static class NewPartitioner extends HashPartitioner<Text, Text>{
		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {
			System.out.println(key);
			String term = key.toString().split(":")[1] ;
			System.out.println(term);
			return super.getPartition(new Text(term), value, numReduceTasks);
		}
	}
	public Customerpart(Configuration conf){
		super(conf) ;
	}
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: inFilePath  outPath");
			System.exit(1);
		}
		Configuration conf = new Configuration() ;
		JobConf job = new JobConf(conf,Customerpart.class) ;
		job.setMapperClass(smapper.class) ;
		job.setPartitionerClass(NewPartitioner.class) ;
		job.setReducerClass(sreduce.class) ;
		job.setMapOutputKeyClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0])) ;
		FileOutputFormat.setOutputPath(job, new Path(args[1])) ;
		JobClient.runJob(job) ;
		return 0;
	}
	public static void main(String[] args) throws Exception {
		int exit = ToolRunner.run(new Customerpart(), args) ;
		System.exit(exit) ;
	}
}
