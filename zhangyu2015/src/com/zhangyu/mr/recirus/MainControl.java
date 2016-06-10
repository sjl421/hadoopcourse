package com.zhangyu.mr.recirus;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MainControl {
	
	public static class TokenizeMapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString()) ;
			System.out.println(value.toString());
			Text local = new Text(itr.nextToken()) ;
			System.out.println(local);
			context.write(local, value);
		}
	}
	public static class IntSunReduce extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			for (Text val : values) {
				context.write(key, val);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration() ;
		String[][] pathfile = {
				{"hdfs://CDH1:8020/zhangyu/data/recirus.txt",
					"hdfs://CDH1:8020/zhangyu/outdata/outControl1"},
				{"hdfs://CDH1:8020/zhangyu/outdata/outControl1",
					"hdfs://CDH1:8020/zhangyu/outdata/outControl2"},
				{"hdfs://CDH1:8020/zhangyu/outdata/outControl2",
					"hdfs://CDH1:8020/zhangyu/outdata/outControl3"}
			} ;
		
		//job1
		ControlledJob job1s = new ControlledJob(conf) ;
		Job job1 = new Job(conf,"job1") ;
		job1.setJarByClass(MainControl.class);
		
		job1.setMapperClass(TokenizeMapper.class) ;
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		
		job1.setReducerClass(IntSunReduce.class) ;
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job1, new Path(pathfile[0][0])) ;
		FileOutputFormat.setOutputPath(job1, new Path(pathfile[0][1])) ;
		job1s.setJob(job1);
		
		//job2
		ControlledJob job2s = new ControlledJob(conf) ;
		Job job2 = new Job(conf,"job2") ;
		job2.setJarByClass(MainControl.class);
		
		job2.setMapperClass(TokenizeMapper.class) ;
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		
		job2.setReducerClass(IntSunReduce.class) ;
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job2, new Path(pathfile[1][0])) ;
		FileOutputFormat.setOutputPath(job2, new Path(pathfile[1][1])) ;
		job2s.setJob(job2);
		
		
		//job3
		ControlledJob job3s = new ControlledJob(conf) ;
		Job job3 = new Job(conf,"job3") ;
		job3.setJarByClass(MainControl.class);
		
		job3.setMapperClass(TokenizeMapper.class) ;
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
		
		job3.setReducerClass(IntSunReduce.class) ;
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job3, new Path(pathfile[2][0])) ;
		FileOutputFormat.setOutputPath(job3, new Path(pathfile[2][1])) ;
		job3s.setJob(job3);
		
		//jobControl配置
		job2s.addDependingJob(job1s) ;
		job3s.addDependingJob(job2s) ;
		JobControl jc = new JobControl("zhangyu") ;
		jc.addJob(job1s) ;
		jc.addJob(job2s) ;
		jc.addJob(job3s) ;
		Thread th = new Thread(jc) ;
		th.start();
		while(true){
			if(jc.allFinished()){
				System.out.println(jc.getSuccessfulJobList());
				jc.stop();
				return ;
			}
			if(jc.getFailedJobList().size()>0){
				System.out.println(jc.getFailedJobList());
				jc.stop();
				return;
			}
		}
	}
}
