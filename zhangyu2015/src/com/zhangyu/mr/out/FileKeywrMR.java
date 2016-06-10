package com.zhangyu.mr.out;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class FileKeywrMR {
	
	public static class MyMapper extends Mapper<Object,Text,LongWritable,Text>{
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			StringTokenizer st = new StringTokenizer(value.toString()) ;
			System.out.println(st.toString());
			for (;st.hasMoreTokens();) {
				context.write(new LongWritable(), new Text(st.nextToken()));
			}
		}
	}
	public static class MyReducer extends Reducer<LongWritable,Text,Text,Text>{
		@Override
		protected void reduce(LongWritable k2, Iterable<Text> v2s,
				Reducer<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			for (Text text : v2s) {
				context.write(new Text(k2.toString()), new Text(text.toString()));
			}
		}
	}
	public static void main(String[] args) throws Exception {
		String in = "hdfs://CDH1:8020/zhangyu/data/out.txt" ;
		String out = "hdfs://CDH1:8020/zhangyu/outdata/fileKeyout" ;
		Configuration conf = new Configuration();
		Job job = new Job(conf, "FileKeywrMR");
		job.setJarByClass(FileKeywrMR.class);
		FileInputFormat.addInputPath(job, new Path(in));
		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileOutputFormat.setOutputPath(job, new Path(out));
		job.setOutputFormatClass(FileKeydoubleOutputFormat.class);
		System.exit(job.waitForCompletion(true) ? 0:1);
	}
}
