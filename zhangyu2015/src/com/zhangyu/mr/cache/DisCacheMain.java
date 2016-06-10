package com.zhangyu.mr.cache;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.zookeeper.common.IOUtils;
@SuppressWarnings("all")
public class DisCacheMain {
    public DisCacheMain() {
    }
    //map处理
    public static class  MapFilter extends Mapper<Object, Text, NullWritable, Text>{
        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            try {
                URI[] cacheFile = DistributedCache.getCacheFiles(context.getConfiguration()) ;
                if(cacheFile != null && cacheFile.length >0){
                    FileSystem hdfs = FileSystem.get(context.getConfiguration()) ;
                    InputStream in = null ;
                    Path p = new Path(cacheFile[0].toString()) ;
                    FileStatus[] files = hdfs.listStatus(p) ;
                    //取共享文件大小
                    long length = 0 ;
                    for (FileStatus file : files) {
                        length = file.getLen() ;
                    }
                    System.out.println("文件全路径："+p+"\n文件大小："+length);
                    //读取共享文件
                    try {
                        in = hdfs.open(p) ;
                        byte[] bs = new byte[(int)length] ;
                        in.read(bs, 0, (int)length) ;
                        String s = new String(bs,"utf-8") ;
                        System.out.println("共享文件内容："+s);
                    } catch (Exception e) {
                        e.printStackTrace() ;
                    }finally{
                        IOUtils.closeStream(in) ;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace() ;
            }
        }
        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            context.write(NullWritable.get(), new Text(value.toString())) ;
        }
    }
    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration() ;
        
        String path = "hdfs://CDH1:8020/zhangyu/data/sample_data.txt" ;
        DistributedCache.addCacheFile(new URI(path), conf) ;
        
        Job job = new Job(conf) ;
        job.setJarByClass(DisCacheMain.class) ;
        Path in = new Path("hdfs://CDH1:8020/zhangyu/data/sample_data.txt") ;
        Path out = new Path("hdfs://CDH1:8020/zhangyu/outdata/outCache") ;
        FileInputFormat.setInputPaths(job, in) ;
        FileOutputFormat.setOutputPath(job, out) ;
        job.setMapperClass(MapFilter.class) ;
        //设置没有reduce阶段
        job.setNumReduceTasks(0) ;
        job.setOutputFormatClass(TextOutputFormat.class) ;
        System.exit(job.waitForCompletion(true)?0:1) ;
    }
}
