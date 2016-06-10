package com.zhangyu.mr.joinreduce;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.contrib.utils.join.DataJoinMapperBase;
import org.apache.hadoop.contrib.utils.join.DataJoinReducerBase;
import org.apache.hadoop.contrib.utils.join.TaggedMapOutput;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
public class ReduceJoinMain extends Configured implements Tool{
    public ReduceJoinMain() {}
    public ReduceJoinMain(Configuration conf){
        super(conf) ;
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
    public static class MyReduce extends DataJoinReducerBase{
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
    @Override
    public int run(String[] args) throws Exception {
    	
        Configuration conf = new Configuration() ;
        JobConf job = new JobConf(conf,ReduceJoinMain.class) ;
        job.setJobName("reduce join");
        String in = "hdfs://CDH1:8020/zhangyu/data/in" ;
        String out = "hdfs://CDH1:8020/zhangyu/outdata/outReducejoin" ;
        FileInputFormat.addInputPath(job, new Path(in)) ;
        FileOutputFormat.setOutputPath(job, new Path(out)) ;
        
        job.setMapperClass(MyMapper.class) ;
        job.setReducerClass(MyReduce.class) ;
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TaggedRecordWritable.class);
        
        job.set("mapred.textoutputformat.separator", ",");
        
        JobClient.runJob(job) ;
        return 0;
    }
    public static void main(String[] args) throws Exception {
        int exit = ToolRunner.run(new ReduceJoinMain(), args) ;
        System.exit(exit) ;
    }
}
