package com.zhangyu.mr.dbwrite;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
public class DBwriteMain {
    public DBwriteMain() {}
    public static class person implements Writable,DBWritable{
        int id ;
        String name ;
        int age ; 
        String address ; 
        public person(int id, String name, int age, String address) {
			this.id = id;
			this.name = name;
			this.age = age;
			this.address = address;
		}
		@Override
        public void write(PreparedStatement stmt) throws SQLException {
            stmt.setInt(1, this.id) ;
            stmt.setString(2, this.name) ;
            stmt.setInt(3, this.age) ;
            stmt.setString(4, this.address) ;
        }
        @Override
        public void readFields(ResultSet resultSet) throws SQLException {
            this.id = resultSet.getInt(1) ;
            this.name = resultSet.getString(2) ;
            this.age = resultSet.getInt(3) ;
            this.address = resultSet.getString(4) ;
        }
        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(this.id) ;
            Text.writeString(out, this.name) ;
            out.writeInt(this.age) ;
            Text.writeString(out, this.address) ;
        }
        @Override
        public void readFields(DataInput in) throws IOException {
            this.id = in.readInt() ;
            this.name = Text.readString(in) ;
            this.age = in.readInt() ;
            this.address = Text.readString(in) ;
        }
        @Override
        public String toString() {
            return new String(this.id+" "+this.name+" "+this.age+" "+this.address);
        }
    }
    public static class PersonMapper extends Mapper<Object, Text, Text, NullWritable>{
        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
        	String string = value.toString();
        	if(string != null){
        		context.write(new Text(string),NullWritable.get()) ;
        	}
        }
    }
    public static class PersonReducer extends Reducer<Text, NullWritable, person, NullWritable>{
		@Override
		protected void reduce(Text k2, Iterable<NullWritable> v2s,
				Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(k2.toString()) ;
			int id = Integer.valueOf(itr.nextToken()) ;
			String name = itr.nextToken() ;
			int age = Integer.valueOf(itr.nextToken()) ;
			String address = itr.nextToken() ;
			System.out.println(id+" "+name+" "+age+" "+address);
			context.write(new person(id,name,age,address), NullWritable.get());
		}
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration() ;
        
        System.out.println("start...");
        DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", 
                "jdbc:mysql://192.168.2.44:3306/test","root","strongs") ;
        
        Job job = new Job(conf,"DBreadMain") ;
        FileInputFormat.addInputPath(job, new Path("hdfs://CDH1:8020/zhangyu/data/datamysql.txt")) ;
        job.setInputFormatClass(TextInputFormat.class);
        
        job.setMapperClass(PersonMapper.class) ;
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        
        job.setReducerClass(PersonReducer.class);
        job.setOutputKeyClass(person.class) ;
        job.setOutputValueClass(NullWritable.class) ;
        
        job.setOutputFormatClass(DBOutputFormat.class) ;
        
        String[] fields = {"id","name","age","address"} ;
        DBOutputFormat.setOutput(job,"person2",fields) ;
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
