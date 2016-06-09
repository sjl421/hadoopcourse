package com.zhangyu.mr;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
public class FileKeydoubleRecordReader extends RecordReader<Text, Text>{
	String fn ;
	LineRecordReader lrr = new LineRecordReader() ;
	public FileKeydoubleRecordReader(){}
	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		lrr.initialize(arg0, arg1);
		this.fn = ((FileSplit)arg0).getPath().getName() ;
	}
	@Override
	public void close() throws IOException {
		lrr.close();
	}
	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		System.out.println("CurrentKey");
		LongWritable lw = lrr.getCurrentKey() ;
		Text key = new Text("("+fn+"@"+lw+")") ;
		System.out.println("key -- "+key);
		return key;
	}
	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return lrr.getCurrentValue();
	}
	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 0;
	}
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return lrr.nextKeyValue();
	}
}
