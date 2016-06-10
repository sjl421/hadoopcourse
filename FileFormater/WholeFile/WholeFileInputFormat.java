package com.zhangyu.mr.allin;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
public class WholeFileInputFormat extends FileInputFormat<NullWritable, Text> {
	public WholeFileInputFormat(){super();}
	@Override
	public RecordReader<NullWritable, Text> createRecordReader(InputSplit split,
			TaskAttemptContext tac) throws IOException, InterruptedException {
		return new WholeFileRecordReader();
	}
	@Override
	protected long computeSplitSize(long blockSize, long minSize, long maxSize) {
		return super.computeSplitSize(blockSize, minSize, maxSize);
	}
	@Override
	protected int getBlockIndex(BlockLocation[] arg0, long arg1) {
		return super.getBlockIndex(arg0, arg1);
	}
	@Override
	protected long getFormatMinSplitSize() {
		return super.getFormatMinSplitSize();
	}
	@Override
	public List<InputSplit> getSplits(JobContext arg0) throws IOException {
		return super.getSplits(arg0);
	}
	// 是否可以继续分片
	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}
	@Override
	protected List<FileStatus> listStatus(JobContext arg0) throws IOException {
		return super.listStatus(arg0);
	}
}
