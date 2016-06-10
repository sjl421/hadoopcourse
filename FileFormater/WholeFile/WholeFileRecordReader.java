package com.zhangyu.mr.allin;
import java.io.IOException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
public class WholeFileRecordReader extends RecordReader<NullWritable, Text>{
	private FileSplit fileSplit ;
	private FSDataInputStream fis ;
	private NullWritable key = null ;
	private Text value = null ;
	private boolean processed = false ;
	@Override
	public void close() throws IOException {
//		fis.close();
	}
	@Override
	public NullWritable getCurrentKey() throws IOException,
			InterruptedException {
		return this.key;
	}
	@Override
	public Text getCurrentValue() throws IOException,
			InterruptedException {
		return this.value;
	}
	@Override
	public void initialize(InputSplit split, TaskAttemptContext tac)
			throws IOException, InterruptedException {
		fileSplit = (FileSplit) split ;
		Configuration job = tac.getConfiguration() ;
		Path file = fileSplit.getPath() ;
		FileSystem fs = file.getFileSystem(job) ;
		fis = fs.open(file) ;
	}
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(key == null){
			key = NullWritable.get() ;
		}
		if(value == null){
			value = new Text() ;
		}
		if(!processed){
			byte[] content = new byte[(int)fileSplit.getLength()] ;
			Path file = fileSplit.getPath() ;
			System.out.println(file.getName());
			try {
				
				IOUtils.readFully(fis, content,0,content.length);
				value.set(new Text(content));
				
			} catch (Exception e) {

				e.printStackTrace();
			}finally{
				IOUtils.closeQuietly(fis);
			}
			processed = true ;
			return true ;
		}
		return false;
	}
	@Override
	public float getProgress() throws IOException, InterruptedException {
		return processed?fileSplit.getLength():0;
	}
}
