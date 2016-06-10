package com.zhangyu.mr.out;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class FileKeydoubleOutputFormat extends FileOutputFormat<Text, Text>{
	public FileKeydoubleOutputFormat() {}

	@Override
	public void checkOutputSpecs(JobContext job)
			throws FileAlreadyExistsException, IOException {
		super.checkOutputSpecs(job);
	}
	@Override
	public Path getDefaultWorkFile(TaskAttemptContext context, String extension)
			throws IOException {
		return super.getDefaultWorkFile(context, extension);
	}
	public synchronized OutputCommitter getOutputCommitter(
			TaskAttemptContext arg0) throws IOException {
		return super.getOutputCommitter(arg0);
	}
	
	@Override
	public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext arg0)
			throws IOException, InterruptedException {
		Configuration job = arg0.getConfiguration() ;
		String replace = getTaskOutputPath(arg0).toString().replace("/_temporary/0/_temporary", "") ;
		
		Path p = new Path(replace) ;
		System.out.println("输出文件名称： "+p.getName());
		System.out.println("全路径  == "+  p);
		FSDataOutputStream fileOut = p.getFileSystem(job).create(p,true) ;
		//使用自定义的OutoutFormat
		FileKeyRecordWriter<Text,Text> writer = new FileKeyRecordWriter<Text, Text>(fileOut) ;
		return writer;
	}
	
	public Path getTaskOutputPath(TaskAttemptContext conf) throws IOException{
		Path workPath = null ;
		OutputCommitter committer = super.getOutputCommitter(conf) ;
		if(committer instanceof FileOutputCommitter){
			workPath = ((FileOutputCommitter) committer).getWorkPath() ;
		}else{
			Path outputPath = super.getOutputPath(conf) ;
			if(outputPath == null){
				throw new IOException("Undefined job output=path") ;
			}
			workPath = outputPath ;
		}
		return workPath ;
	}
}
