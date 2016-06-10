package com.zhangyu.mr.gloab;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class GloabargMain {
    
    public GloabargMain() {}
    //map处理
    public static class  Map extends Mapper<Object, Text, NullWritable, Text>{
        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(),",") ;
            String id = itr.nextToken() ;
            Configuration conf = context.getConfiguration() ;
            //获取指定名称的全局参数
            String[] filter = conf.getStrings("filter") ;
            boolean flag = false ;
            for(int i=0; i<filter.length; i++){
                if(filter[i].equals(id)){
                    flag = true ;
                    break ;
                }
            }
            if(flag){
                context.write(NullWritable.get(), new Text(value.toString())) ;
            }
        }
    }
    public static void main(String[] args) throws Exception {
        
        Configuration conf = new Configuration() ;
        //设置过滤条件全局参数
        String[] filter = {"1","2"} ;
        conf.setStrings("filter", filter) ;
        
        Job job = new Job(conf,"gloable") ;
        job.setJarByClass(GloabargMain.class) ;
        Path in = new Path("hdfs://CDH1:8020/zhangyu/data/out.txt") ;
        Path out = new Path("hdfs://CDH1:8020/zhangyu/outdata/outGloable") ;
        
        FileInputFormat.setInputPaths(job, in) ;
        FileOutputFormat.setOutputPath(job, out) ;
        job.setMapperClass(Map.class) ;
        //设置没有reduce阶段
        job.setNumReduceTasks(0) ;
        job.setOutputFormatClass(TextOutputFormat.class) ;
        System.exit(job.waitForCompletion(true)?0:1) ;
    }
}
