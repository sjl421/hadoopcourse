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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
public class MainRecirus extends Configured implements Tool{
	public MainRecirus() {}
	
	public static class smapper extends MapReduceBase 
	implements Mapper<LongWritable, Text, Text, Text>{
		setup(){}
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
   StringTokenizer itr   = new StringTokenizer(value.toString()) ;
   String code           = itr.nextToken() ;
   String name           = itr.nextToken() ;
   String height         = itr.nextToken() ;
   String sex            = itr.nextToken() ;
   String chara          = itr.nextToken() ;
   System.out.println("学号= "+code+" "+
   "其他信息                 = "+name+":"+height+":"+sex+":"+chara);
   String s              = name+" "+height+" "+sex+" "+chara ;
			if(check(s)){
				output.collect(new Text(code), new Text(s)) ;
			}
		}
	}
	public static boolean check(String s,String attr_check){
		String value ;
		StringTokenizer itr = new StringTokenizer(s.toString()) ;
		String name = itr.nextToken() ;
		String height = itr.nextToken() ;
		String sex = itr.nextToken() ;
		String chara = itr.nextToken() ;
		if(attr_check.equals("height")){
			//第一次迭代，计算身高
			value = height ;
			float f = Float.valueOf(value) ;
			float cf =  f-(float)1.6 ;
			System.out.println(name+": "+cf);
			if(cf>0){
				return true ;
			}else{
				return false ;
			}
		}else if(attr_check.equals("sex"){
			//第二次迭代，检查性别
			value = sex ;
			int f = Integer.valueOf(value) ;
			if(f>0){
				return true ;
			}else{
				return false ;
			}
		}else if(attr_check.equals("chara"){
			//第二次迭代，检查特长
			value = chara ;
			int f = Integer.valueOf(value) ;
			if(f>0){
				return true ;
			}else{
				return false ;
			}
		}
		return true ;
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
	public MainRecirus(Configuration conf){
		super(conf) ;
	}
	@SuppressWarnings("static-access")
	@Override
	public int run(String[] args) throws Exception {
 		String[] attrs={'height','sex','chara'}
 		
		Configuration conf = new Configuration() ;
		String[][] pathfile = {
				{"hdfs://CDH1:8020/zhangyu/data/recirus.txt",
					"hdfs://CDH1:8020/zhangyu/outdata/outRecirus"},
				{"hdfs://CDH1:8020/zhangyu/outdata/outRecirus",
					"hdfs://CDH1:8020/zhangyu/outdata/outRecirus1"},
				{"hdfs://CDH1:8020/zhangyu/outdata/outRecirus1",
					"hdfs://CDH1:8020/zhangyu/outdata/outRecirus2"}
			} ;
		
		for(int i=0;i<attrs.length;i++){
			JobConf job = new JobConf(conf,MainRecirus.class) ;
			//将此次迭代检查的项目传递给Map任务
			job.getConfiguration().set('app.custom.attr',attrs[i]);
			job.setMapperClass(smapper.class) ;
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setReducerClass(sreduce.class) ;
			FileInputFormat.addInputPath(job, new Path(pathfile[i][0])) ;
			FileOutputFormat.setOutputPath(job, new Path(pathfile[i][1])) ;
			JobClient.runJob(job) ;
			this.count++ ;
		}
		return 0;
	}
	public static void main(String[] args) throws Exception {
		int exit = ToolRunner.run(new MainRecirus(), args) ;
		System.exit(exit) ;
	}
}
