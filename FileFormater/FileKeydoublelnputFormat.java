FileKeydoublelnputFormat类的实现程序代码如下：
package com.zhangyu.mr;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
public class FileKeydoublelnputFormat extends FileInputFormat<Text, Text> {
	public FileKeydoublelnputFormat(){}
	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit split,
			TaskAttemptContext tac) throws IOException, InterruptedException {
		FileKeydoubleRecordReader fkrr = new FileKeydoubleRecordReader() ;
		try {
			fkrr.initialize(split, tac);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return fkrr;
	}
	@Override
	protected long computeSplitSize(long blockSize, long minSize, long maxSize) {
		return super.computeSplitSize(blockSize, minSize, maxSize);
	}
	@Override
	public List<InputSplit> getSplits(JobContext arg0) throws IOException {
		return super.getSplits(arg0);
	}
	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		//return super.isSplitable(context, filename);
		return true ;
	}
	@Override
	protected List<FileStatus> listStatus(JobContext arg0) throws IOException {
		return super.listStatus(arg0);
	}
}
