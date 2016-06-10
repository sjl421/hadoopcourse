package com.zhangyu.mr.recirus;
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
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.ChainReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
public class MainChainMapred1 extends Configured implements Tool{
	public MainChainMapred1() {}
	public MainChainMapred1(Configuration conf) {
		super(conf) ;
	}
	//map1:输出键：学号  输出值：姓名+身高+性别+特长
	public static class map1 extends MapReduceBase 
	implements Mapper<LongWritable, Text, Text, Text>{
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			StringTokenizer itr = new StringTokenizer(value.toString()) ;
			String code = itr.nextToken() ;
			String name = itr.nextToken() ;
			String height = itr.nextToken() ;
			String sex = itr.nextToken() ;
			String chara = itr.nextToken() ;
			System.out.println("学号= "+code+" "+
			"其他信息= "+name+":"+height+":"+sex+":"+chara);
			String s = name+" "+height+" "+sex+" "+chara ;
			output.collect(new Text(code), new Text(s)) ;
		}
	}
	//map2:输出键：学号  输出值：姓名+身高+性别
	public static class map2 extends MapReduceBase 
	implements Mapper<Text, Text, Text, Text>{
		@Override
		public void map(Text key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			StringTokenizer itr = new StringTokenizer(value.toString()) ;
			String name = itr.nextToken() ;
			String height = itr.nextToken() ;
			String sex = itr.nextToken() ;
			System.out.println("学号= "+key+" "+
			"其他信息= "+name+":"+height+":"+sex);
			String s = name+" "+height+" "+sex ;
			output.collect(key, new Text(s)) ;
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
	//map3:输出键：学号  输出值：姓名+身高
	public static class map3 extends MapReduceBase 
	implements Mapper<Text, Text, Text, Text>{
		@Override
		public void map(Text key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			StringTokenizer itr = new StringTokenizer(value.toString()) ;
			String name = itr.nextToken() ;
			String height = itr.nextToken() ;
			System.out.println("学号= "+key+" "+
			"其他信息= "+name+":"+height);
			String s = name+" "+height ;
			output.collect(key, new Text(s)) ;
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration() ;
		JobConf job = new JobConf(conf) ;
		job.setJobName("ChainJob");
		
		String in = "hdfs://CDH1:8020/zhangyu/data/recirus.txt" ;
		String out = "hdfs://CDH1:8020/zhangyu/outdata/outChain" ;
		
		FileInputFormat.addInputPath(job, new Path(in)) ;
		FileOutputFormat.setOutputPath(job, new Path(out)) ;
		
		//map配置
		JobConf map1Conf = new JobConf(false);
		ChainMapper.addMapper(job, map1.class, LongWritable.class, 
				Text.class, Text.class, Text.class, true, map1Conf);
		JobConf map2Conf = new JobConf(false);
		ChainMapper.addMapper(job, map2.class, Text.class, 
				Text.class, Text.class, Text.class, true, map2Conf);
		//reduce配置
		JobConf red1Conf = new JobConf(false);
		ChainReducer.setReducer(job, sreduce.class, Text.class, 
				Text.class, Text.class, Text.class, true, red1Conf);
		
		JobConf map3Conf = new JobConf(false);
		ChainMapper.addMapper(job, map3.class, Text.class, 
				Text.class, Text.class, Text.class, true, map3Conf);
		
		JobClient.runJob(job) ;
		return 0 ;
	}
	public static void main(String[] args) throws Exception {
		int exit = ToolRunner.run(new MainChainMapred1(), args) ;
		System.exit(exit) ;
	}
}
