package com.zhangyu.mr.joinmapreduce;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.contrib.utils.join.DataJoinMapperBase;
import org.apache.hadoop.contrib.utils.join.DataJoinReducerBase;
import org.apache.hadoop.contrib.utils.join.TaggedMapOutput;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MRJoinMain {
	
	//创建一个用户ID过滤条件数组，只对1号和2号客户的订单进行连接
	public static String[] filterCust = {"1","2"} ;
	
    public MRJoinMain() {}
    
    
    //Map是过滤用户条件
    public static class MapFilter extends MapReduceBase 
    implements Mapper<Object, Text, NullWritable, Text>{
        @Override
        public void map(Object key, Text value,
                OutputCollector<NullWritable, Text> output, Reporter reporter)
                throws IOException {
    		StringTokenizer itr = new StringTokenizer(value.toString(),",") ;
    		String id = itr.nextToken() ;
    		boolean flag = false ;
    		for(int i=0; i<filterCust.length; i++){
    			if(filterCust[i].equals(id)){
    				flag = true ;
    				break ;
    			}
    		}
    		if(flag){
    			output.collect(NullWritable.get(), new Text(value.toString()));
    		}
		}
    }
    
    //Map处理过程
    public static class MyMapper extends DataJoinMapperBase{
		@Override
		protected Text generateGroupKey(TaggedMapOutput key) {
			//确定主键名
			String line = key.getData().toString() ;
			String[] tokens = line.split(",") ;
			String groupKey = tokens[0] ;
			return new Text(groupKey);
		}
		@Override
		protected Text generateInputTag(String value) {
			//确定源标签
			String dataSource = value.split("-")[0] ;
			return new Text(dataSource);
		}
		@Override
		protected TaggedMapOutput generateTaggedMapOutput(Object obj) {
			//在数据记录中加入设定数据源标签
			Text val = (Text)obj ;
			TaggedRecordWritable trw = new TaggedRecordWritable(val) ;
			trw.setTag(this.inputTag) ;
			return trw;
		}
    }
    public static class TaggedRecordWritable extends TaggedMapOutput{
    	private Writable data ;
    	public TaggedRecordWritable() {
    		this.tag = new Text("") ;
    		this.data = new Text("") ;
		}
    	public TaggedRecordWritable(Writable data) {
    		this.tag = new Text("") ;
    		this.data = data ;
		}
		@Override
		public Writable getData() {
			return data;
		}
		public void setData(Writable data) {
			this.data = data;
		}
		@Override
		public void readFields(DataInput in) throws IOException {
			this.tag.readFields(in);
			this.data.readFields(in);
		}
		@Override
		public void write(DataOutput out) throws IOException {
			this.tag.write(out);
			this.data.write(out);
		}
    }
    public static class ReduceFilter extends DataJoinReducerBase{
		@Override
		protected TaggedMapOutput combine(Object[] tags, Object[] values) {
			if(tags.length<2){
				return null ;
			}
			String joinedStr = "" ;
			for(int i=0; i<values.length; i++){
				if(i > 0){
					joinedStr += "," ;
				}
				TaggedRecordWritable tw = (TaggedRecordWritable) values[i] ;
				String line = tw.getData().toString() ;
				String[] tokens = line.split(",",2) ;
				joinedStr += tokens[1] ;
			}
			TaggedRecordWritable trw = new TaggedRecordWritable(new Text(joinedStr));
			trw.setTag((Text)tags[0]);
			return trw;
		}
    }
     
    public static void main(String[] args) throws Exception {
    	Configuration conf = new Configuration() ;
        //过滤客户信息
    	JobConf jobc = new JobConf(conf,MRJoinMain.class) ;
        jobc.setJarByClass(MRJoinMain.class);
        
        String cin = "hdfs://CDH1:8020/zhangyu/data/in/customers.txt" ;
        String cout = "hdfs://CDH1:8020/zhangyu/data/mrjoin/cout" ;
        
        FileInputFormat.setInputPaths(jobc, new Path(cin)) ;
        FileOutputFormat.setOutputPath(jobc, new Path(cout)) ;
        jobc.setMapperClass(MapFilter.class) ;
        jobc.setNumReduceTasks(0);
        JobClient.runJob(jobc) ;
        
        //过滤订单信息
    	JobConf jobo = new JobConf(conf,MRJoinMain.class) ;
        jobc.setJarByClass(MRJoinMain.class);
        
        String oin = "hdfs://CDH1:8020/zhangyu/data/in/orders.txt" ;
        String oout = "hdfs://CDH1:8020/zhangyu/outdata/outmrjoin" ;
        
        FileInputFormat.setInputPaths(jobo, new Path(oin)) ;
        FileOutputFormat.setOutputPath(jobo, new Path(oout)) ;
        jobo.setMapperClass(MapFilter.class) ;
        jobo.setNumReduceTasks(0);
        JobClient.runJob(jobo) ;
        
        //Reduce端连接
    	JobConf jobr = new JobConf(conf,MRJoinMain.class) ;
        jobc.setJarByClass(MRJoinMain.class);
        
        String rout = "hdfs://CDH1:8020/zhangyu/data/mrjoin/oout" ;
        
        FileInputFormat.setInputPaths(jobr, new Path(cout)) ;
        FileOutputFormat.setOutputPath(jobr, new Path(rout)) ;
        jobr.setMapperClass(MyMapper.class) ;
        jobr.setReducerClass(ReduceFilter.class);
        jobr.setOutputKeyClass(Text.class);
        jobr.setOutputValueClass(TaggedRecordWritable.class);
        
        JobClient.runJob(jobr) ;        
        
    }
}
