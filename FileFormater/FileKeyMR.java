package com.zhangyu.mr;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class FileKeyMR {
	public static class MyMapper extends Mapper<Object,Text,Text,Text>{
		protected void map(Text key, Text value,Context context)
				throws IOException, InterruptedException {
			StringTokenizer st = new StringTokenizer(value.toString()) ;
			Text word = new Text() ;
			for (;st.hasMoreTokens();) {
				word.set(st.nextToken());
				context.write(word, key);
			}
		}
	}
	public static class MyReducer extends Reducer<Text,Text,Text,Text>{
		@Override
		protected void reduce(Text k2, Iterable<Text> v2s,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Text val = new Text() ;
			String s = ": " ;
			for (Text text : v2s) {
				s+=text.toString() ;
			}
			val.set(s);
			context.write(k2, val);
		}
	}
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: inFilePath outPath");
			System.exit(1);
		}
		Configuration conf = new Configuration();
		Job job = new Job(conf, "FileKeyMR");
		job.setJarByClass(FileKeyMR.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(FileKeydoublelnputFormat.class);
		
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(TextOutputFormat.class);
		System.exit(job.waitForCompletion(true) ? 0:1);
	}
}
