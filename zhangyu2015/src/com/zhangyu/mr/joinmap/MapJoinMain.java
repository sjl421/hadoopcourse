package com.zhangyu.mr.joinmap;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Hashtable;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MapJoinMain {
    public MapJoinMain() {}
    //Map处理过程
    public static class MyMapper extends  Mapper<Object, Text, NullWritable, Text>{

    	private Hashtable<String,String> joinData = new Hashtable<String,String>();
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			FileSystem hdfs = FileSystem.get(context.getConfiguration()) ;
			InputStream in = null ;
			try {
				String inFilePath = "/zhangyu/data/mapjoin/cache/customers.txt" ;
				in = hdfs.open(new Path(inFilePath)) ;
				byte[] bs = new byte[4096] ;
				in.read(bs, 0, 4096) ;
				String s = new String(bs) ;
				System.out.println(s);
				String[] k = s.split("\n",4);
				String[] tokens ;
				for(int i=0; i<4; i++){
					tokens = k[i].split(",",2) ;
					joinData.put(tokens[0], tokens[1]) ;
				}
				System.out.println("111111");
				
			} catch (Exception e) {
				e.printStackTrace();
			}finally{
				IOUtils.closeStream(in);
				System.out.println("22222");
			}
			Path[] cacheFile = DistributedCache.getLocalCacheFiles(context.getConfiguration()) ;
			
			System.out.println("33333");
			System.out.println("cacheFile ===="+cacheFile[0]);
			if(cacheFile != null && cacheFile.length >0){
				String line ;
				String[] tokens ;
				System.out.println("4444");
				BufferedReader joinRead = new BufferedReader(
						new FileReader(cacheFile[0].toString().replace("file", "hdfs"))) ;
				
				System.out.println("5555");
				try {
					while((line = joinRead.readLine())!=null){
						tokens = line.split(",",2) ;
						System.out.println("tokens[0] == "+tokens[0]);
						joinData.put(tokens[0], tokens[1]) ;
					}
				} catch (Exception e) {
					e.printStackTrace();
				}finally{
					joinRead.close() ;
				}
				
			}
		}
    	
    	@Override
		protected void map(Object key, Text value,Context context)
				throws IOException, InterruptedException {
    		StringTokenizer itr = new StringTokenizer(value.toString(),",") ;
    		String keys = itr.nextToken() ;
    		String joinValue = joinData.get(keys) ;
    		if(joinValue!=null){
    			context.write(NullWritable.get(), 
    					new Text(value.toString()+","+joinValue));
    		}
		}
    }
     
    public static void main(String[] args) throws Exception {
    	Configuration conf = new Configuration() ;
        Job job = new Job(conf,"MapJoin") ;
        job.setJarByClass(MapJoinMain.class);
        
        String uri = "hdfs://CDH1:8020/zhangyu/data/mapjoin/cache/customers.txt" ;
        DistributedCache.addCacheFile(new Path(uri).toUri(),
        		job.getConfiguration());
        
        String in = "hdfs://CDH1:8020/zhangyu/data/mapjoin/in" ;
        String out = "hdfs://CDH1:8020/zhangyu/data/mapjoin/outMapjoin" ;
        
        FileInputFormat.addInputPath(job, new Path(in)) ;
        FileOutputFormat.setOutputPath(job, new Path(out)) ;
        
        job.setMapperClass(MyMapper.class) ;
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(TextOutputFormat.class);
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
