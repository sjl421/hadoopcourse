package com.zhangyu.mr.mult;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
public class MultiOutputFormat extends Configured implements Tool{
    public MultiOutputFormat() {}
    
    public static class smapper extends MapReduceBase 
    implements Mapper<LongWritable, Text, NullWritable, Text>{
        @Override
        public void map(LongWritable key, Text value,
                OutputCollector<NullWritable, Text> output, Reporter reporter)
                throws IOException {
            output.collect(NullWritable.get(), value) ;
        }
    }
    public static class sreduce extends MapReduceBase 
    implements Reducer<NullWritable, Text, NullWritable, Text>{
        @Override
        public void reduce(NullWritable key, Iterator<Text> values,
                OutputCollector<NullWritable, Text> output, Reporter reporter)
                throws IOException {
            while(values.hasNext()){
                output.collect(NullWritable.get(), values.next()) ;
            }
        }
    }
    
    public static class saveByFormat extends MultipleTextOutputFormat<NullWritable, Text>{
        @Override
        protected String generateFileNameForKeyValue(NullWritable key,
                Text value, String name) {
            String[] datalog = value.toString().split(" ",-1) ;
            String date = datalog[0] ;
            System.out.println(date);
            return date ;
        }
    }
    public MultiOutputFormat(Configuration conf){
        super(conf) ;
    }
    
    @Override
    public int run(String[] args) throws Exception {
    	/**
    	 *  hdfs://CDH1:8020/zhangyu/data/city.txt
			hdfs://CDH1:8020/zhangyu/outdata/filemultout
    	 */
		if (args.length != 2) {
			System.err.println("Usage: inFilePath  outPath");
			System.exit(1);
		}
        Configuration conf = new Configuration() ;
        JobConf job = new JobConf(conf,MultiOutputFormat.class) ;
        job.setMapperClass(smapper.class) ;
        job.setReducerClass(sreduce.class) ;
        job.setMapOutputKeyClass(NullWritable.class) ;
        job.setOutputFormat(saveByFormat.class) ;
        FileInputFormat.addInputPath(job, new Path(args[0])) ;
        FileOutputFormat.setOutputPath(job, new Path(args[1])) ;
        JobClient.runJob(job) ;
        return 0;
    }
    public static void main(String[] args) throws Exception {
        int exit = ToolRunner.run(new MultiOutputFormat(), args) ;
        System.exit(exit) ;
    }
}
