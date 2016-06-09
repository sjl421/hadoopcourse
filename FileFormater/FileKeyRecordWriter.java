package com.zhangyu.mr.out;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
public class FileKeyRecordWriter<K,V> extends RecordWriter<K, V>{
	private static final String utf8 = "UTF-8" ;
	private static final byte[] newLine ;
	static{
		try { 
			newLine = "\n".getBytes(utf8) ;
		} catch (Exception e) {
			throw new IllegalArgumentException("can't find"+utf8+" encoding") ;
		}
	}
	protected DataOutputStream out ;
	private final byte[] keyValueSeparator ;
	//实现构造方法
	public FileKeyRecordWriter(DataOutputStream out,String keyValueSeparator){
		this.out = out ;
		try {
			this.keyValueSeparator = keyValueSeparator.getBytes(utf8) ;
		} catch (Exception e) {
			throw new IllegalArgumentException("can't find"+utf8+" encoding") ;
		}
	}
	public FileKeyRecordWriter(FSDataOutputStream out){
		this(out,"\t") ;
	}
	private void writeObject(Object o) throws IOException{
		System.out.println("o = " + o);
		if(o instanceof Text){
			Text text = (Text) o ;
			out.write(text.getBytes(),0,text.getLength());
		}else{
			out.write(o.toString().getBytes(utf8));
		}
	}
	//将mapreduce的key,value以自定义格式写入输出流中
	public synchronized void write(K key, V value) throws IOException, InterruptedException {
		boolean nullKey = key == null || key instanceof NullWritable ;
		boolean nullValue = value == null || value instanceof NullWritable ;
		if(nullKey && nullValue){
			return ;
		}
		if(!nullKey){
			out.write("zhangyu".getBytes(utf8));
			writeObject(key) ;
		}
		if(!(nullKey || nullValue)){
			out.write("zhangyu".getBytes(utf8));
			writeObject(keyValueSeparator) ;
		}
		if(!nullValue){
			out.write("zhangyu".getBytes(utf8));
			writeObject(value) ;
		}
		System.out.println("newLine == "+newLine);
		out.write(newLine);
	}
	public synchronized void close(TaskAttemptContext arg0) throws IOException,
			InterruptedException {
		out.close();
	}
}
