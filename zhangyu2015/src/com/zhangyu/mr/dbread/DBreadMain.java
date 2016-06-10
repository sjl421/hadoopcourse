package com.zhangyu.mr.dbread;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class DBreadMain {
    public DBreadMain() {}
    public static class person implements Writable,DBWritable{
        int id ;
        String name ;
        int age ; 
        String address ; 
        
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
    public static class PersonMapper extends Mapper<LongWritable, person, NullWritable, Text>{
        @Override
        protected void map(LongWritable key, person value, Context context)
                throws IOException, InterruptedException {
            
            context.write(NullWritable.get(), new Text(value.toString())) ;
        }
    }
    public static void main(String[] args) throws Exception {
        
        Configuration conf = new Configuration() ;
        System.out.println("start...");
        DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", 
                "jdbc:mysql://192.168.2.44:3306/test","root","strongs") ;
        Job job = new Job(conf,"DBreadMain") ;
        job.setOutputKeyClass(LongWritable.class) ;
        job.setOutputValueClass(Text.class) ;
        job.setInputFormatClass(DBInputFormat.class) ;
        
        String[] fields = {"id","name","age","address"} ;
        DBInputFormat.setInput(job, person.class, "person", null,"id",fields) ;
        FileOutputFormat.setOutputPath(job, new Path("hdfs://CDH1:8020/zhangyu/outdata/outDBread")) ;
        job.setMapperClass(PersonMapper.class) ;
        job.setNumReduceTasks(0);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        
    }
}
