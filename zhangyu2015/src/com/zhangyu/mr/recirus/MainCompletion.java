package com.zhangyu.mr.recirus;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

public class MainCompletion {

	private static Class<?> jobClass1;
	private static Class<?> jobClass2;
	private static Class<?> jobClass3;

	@SuppressWarnings("all")
	public static void main(String[] args) throws Exception {
		
		String inpath1 = null ;
		String outpath1 = null ;
		String outpath2 = null ;
		String outpath3 = null ;
		
		Configuration conf = new Configuration() ;
		//第一部分：作业配置，注意只做配置不做运行
		//job1的配置
		ControlledJob joba = new ControlledJob(conf) ;
		Job job1 = new Job(conf,"jobName1") ;
		job1.setJarByClass(jobClass1);
		//...
		joba.setJob(job1);
		//job2的配置
		ControlledJob jobb = new ControlledJob(conf) ;
		Job job2 = new Job(conf,"jobName2") ;
		job2.setJarByClass(jobClass2);
		//...
		jobb.setJob(job2);
		//job3的配置
		ControlledJob jobc = new ControlledJob(conf) ;
		Job job3 = new Job(conf,"jobName3") ;
		job3.setJarByClass(jobClass3);
		//...
		jobc.setJob(job3);
		
		//第二部分：依赖关系配置
		jobb.addDependingJob(joba) ;
		jobc.addDependingJob(jobb) ;
		
		//第三部分：JobControl配置
		JobControl jc = new JobControl("abc") ;
		jc.addJob(joba) ;
		jc.addJob(jobb) ;
		jc.addJob(jobc) ;
	}

}
